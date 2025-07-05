import os
from fastapi import FastAPI, Request, HTTPException, Response
from pydantic import BaseModel
from sqlalchemy.orm import Session
import httpx
from contextlib import contextmanager
import asyncio
import boto3
from botocore.client import Config
from urllib.parse import urlparse

from model import Base, engine, get_session, Order, Beat  # aggiungi Beat all'import

# Gestione variabili ambiente: locale (.env) o produzione (Render.com)
if os.path.exists(os.path.join(os.path.dirname(__file__), '.env')):
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

def get_env_var(key, default=None):
    return os.environ.get(key, default)

def get_bot_internal_url():
    url = get_env_var("BOT_INTERNAL_URL")
    if url:
        return url
    return "http://localhost:8080"

BOT_INTERNAL_URL = get_bot_internal_url()
INTERNAL_TOKEN = get_env_var("INTERNAL_TOKEN") if get_env_var("BOT_INTERNAL_URL") else None

PAYPAL_CLIENT_ID = get_env_var("PAYPAL_CLIENT_ID")
PAYPAL_CLIENT_SECRET = get_env_var("PAYPAL_CLIENT_SECRET")
PAYPAL_WEBHOOK_ID = get_env_var("PAYPAL_WEBHOOK_ID")

# --- PAYPAL ENVIRONMENT SWITCH ---
PAYPAL_ENV = os.environ.get("PAYPAL_ENV", "sandbox").lower()  # "sandbox" or "live"
if PAYPAL_ENV == "live":
    PAYPAL_API_BASE_URL = "https://api-m.paypal.com"
else:
    PAYPAL_API_BASE_URL = "https://api-m.sandbox.paypal.com"
PAYPAL_OAUTH_URL = f"{PAYPAL_API_BASE_URL}/v1/oauth2/token"
PAYPAL_WEBHOOK_VERIFY_URL = f"{PAYPAL_API_BASE_URL}/v1/notifications/verify-webhook-signature"

app = FastAPI()

Base.metadata.create_all(bind=engine)

class PayPalWebhookData(BaseModel):
    id: str
    event_type: str
    resource: dict

@contextmanager
def get_db_session():
    session = get_session()
    try:
        yield session
    finally:
        session.close()

async def get_paypal_access_token() -> str:
    url = PAYPAL_OAUTH_URL
    auth = (PAYPAL_CLIENT_ID, PAYPAL_CLIENT_SECRET)
    headers = {"Accept": "application/json", "Accept-Language": "en_US"}
    data = {"grant_type": "client_credentials"}

    async with httpx.AsyncClient() as client:
        resp = await client.post(url, auth=auth, data=data, headers=headers)
        resp.raise_for_status()
        return resp.json().get("access_token")

async def verify_paypal_webhook(body: dict, headers: dict) -> bool:
    url = PAYPAL_WEBHOOK_VERIFY_URL
    access_token = await get_paypal_access_token()
    headers_verify = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    # Normalizza header a lowercase
    headers_lc = {k.lower(): v for k, v in headers.items()}
    data = {
        "auth_algo": headers_lc.get("paypal-auth-algo"),
        "cert_url": headers_lc.get("paypal-cert-url"),
        "transmission_id": headers_lc.get("paypal-transmission-id"),
        "transmission_sig": headers_lc.get("paypal-transmission-sig"),
        "transmission_time": headers_lc.get("paypal-transmission-time"),
        "webhook_id": PAYPAL_WEBHOOK_ID,
        "webhook_event": body
    }
    print(">>> [WEBHOOK] Dati verifica firma:", data)  # DEBUG
    async with httpx.AsyncClient() as client:
        resp = await client.post(url, json=data, headers=headers_verify)
    resp_json = resp.json()
    return resp_json.get("verification_status") == "SUCCESS"

MAX_CONCURRENT_DOWNLOADS = 2  # Limite consigliato per Render free
download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

async def notify_user_via_bot(user_id, beat_title, transaction_id):
    url = f"{BOT_INTERNAL_URL}/internal/send_message"
    payload = {
        "user_id": user_id,
        "beat_title": beat_title,
        "transaction_id": transaction_id
    }
    headers = {}
    if INTERNAL_TOKEN:
        headers["X-Internal-Token"] = INTERNAL_TOKEN
    async with httpx.AsyncClient(verify=True) as client:
        await client.post(url, json=payload, headers=headers)

@app.post("/webhook/paypal")
async def paypal_webhook(request: Request):
    print(">>> [WEBHOOK] Richiesta ricevuta su /webhook/paypal")
    body = await request.json()
    headers = request.headers

    print(">>> [WEBHOOK] Body ricevuto:", body)
    print(">>> [WEBHOOK] Headers ricevuti:", dict(headers))

    try:
        verified = await verify_paypal_webhook(body, headers)
        print(f">>> [WEBHOOK] Verifica firma PayPal: {verified}")
    except Exception as e:
        print(f">>> [WEBHOOK] Errore verifica firma: {e}")
        raise HTTPException(status_code=400, detail="Webhook verification error")

    if not verified:
        print(">>> [WEBHOOK] Verifica firma fallita, richiesta ignorata.")
        raise HTTPException(status_code=400, detail="Webhook verification failed")

    event_type = body.get("event_type")
    resource = body.get("resource")
    print(f">>> [WEBHOOK] Evento ricevuto: {event_type}")

    # GESTIONE EVENTO CHECKOUT.ORDER.APPROVED: NON inviare più il messaggio di attesa qui!
    if event_type == "CHECKOUT.ORDER.APPROVED":
        print(">>> [WEBHOOK] Gestione evento CHECKOUT.ORDER.APPROVED")
        # Solo log/debug, nessun messaggio Telegram qui!
        return {"status": "ok", "message": "Order approved event received (no Telegram message sent)"}

    # ...existing PAYMENT.CAPTURE.COMPLETED logic...
    if event_type == "PAYMENT.CAPTURE.COMPLETED":
        print(">>> [WEBHOOK] Gestione evento PAYMENT.CAPTURE.COMPLETED")
        transaction_id = resource.get("id")
        payer_email = resource.get("payer", {}).get("email_address", "")
        amount = float(resource.get("amount", {}).get("value", 0))
        currency = resource.get("amount", {}).get("currency_code", "")

        purchase_units = resource.get("purchase_units", [])
        telegram_user_id = None
        beat_title = None

        print(">>> [WEBHOOK] purchase_units:", purchase_units)

        # Cerca custom_id in purchase_units[0] oppure direttamente in resource
        custom_id = None
        if purchase_units and isinstance(purchase_units, list) and len(purchase_units) > 0:
            custom_id = purchase_units[0].get("custom_id") or purchase_units[0].get("reference_id")
        if not custom_id:
            custom_id = resource.get("custom_id") or resource.get("reference_id")
        print(">>> [WEBHOOK] custom_id:", custom_id)

        if custom_id and ':' in custom_id:
            telegram_user_id_str, beat_title_str = custom_id.split(':', 1)
            try:
                telegram_user_id = int(telegram_user_id_str)
                beat_title = beat_title_str.replace('_', ' ')
            except ValueError:
                telegram_user_id = None
                beat_title = None

        print(f">>> [WEBHOOK] telegram_user_id: {telegram_user_id}, beat_title: {beat_title}")

        if telegram_user_id is None or beat_title is None:
            print(">>> [WEBHOOK] custom_id non valido o mancante.")
            print(">>> [WEBHOOK] resource completo:", resource)  # Per debug
            raise HTTPException(status_code=400, detail="Invalid custom_id format in webhook resource")

        token = resource.get("custom_token")  # usa se serve

        with get_db_session() as db:
            exists = db.query(Order).filter(Order.transaction_id == transaction_id).first()
            if exists:
                print(">>> [WEBHOOK] Transazione già processata.")
                return {"status": "ok", "message": "Transaction already processed"}

            new_order = Order(
                transaction_id=transaction_id,
                beat_id=None,  # se usi, altrimenti None
                payer_email=payer_email,
                amount=amount,
                currency=currency,
                token=token,
                telegram_user_id=telegram_user_id,
                beat_title=beat_title
            )
            db.add(new_order)
            db.commit()
            db.refresh(new_order)
            print(">>> [WEBHOOK] Nuovo ordine salvato nel database.")

        # Invia PRIMA il messaggio di attesa all'utente SOLO QUI
        await notify_user_via_bot(
            user_id=telegram_user_id,
            beat_title=beat_title,
            transaction_id=transaction_id
        )

        # Rimuovi beat esclusivo dal database se necessario
        removed = remove_exclusive_beat_by_title(beat_title)
        if removed:
            print(f"[WEBHOOK] Beat esclusivo '{beat_title}' rimosso dopo acquisto PayPal.")

        print(">>> [WEBHOOK] resource completo per debug redirect:", resource)

        return {"status": "ok", "message": "Order saved, user notified, and exclusive beat removed if needed"}

    print(f">>> [WEBHOOK] Evento non gestito: {event_type}")
    return {"status": "ignored", "message": f"Unhandled event type {event_type}"}

# --- INIZIO: Funzioni importate da utils.py, ora locali ---

def remove_exclusive_beat_by_title(title: str):
    """Rimuove un beat esclusivo dal database dato il titolo."""
    with get_db_session() as session:
        beat = session.query(Beat).filter_by(title=title, is_exclusive=1).first()
        if beat:
            session.delete(beat)
            session.commit()
            print(f"[INFO] Beat esclusivo '{title}' rimosso dal database dopo l'acquisto.")
            return True
    return False

# --- FINE: Funzioni importate da utils.py, ora locali ---

if __name__ == "__main__":
    import uvicorn
    print(">>> [WEBHOOK] Server avviato. Webhook URL: /webhook/paypal")
    # Avvia Uvicorn in modalità produzione (no reload)
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False)



