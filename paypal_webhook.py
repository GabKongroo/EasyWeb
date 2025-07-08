import os
import time
import json
from fastapi import FastAPI, Request, HTTPException, Response
from pydantic import BaseModel
from sqlalchemy.orm import Session
import httpx
from contextlib import contextmanager
import asyncio
import boto3
from botocore.client import Config
from urllib.parse import urlparse

from model import Base, engine, get_session, Order, Beat, Bundle, BundleBeat  # aggiungi Bundle e BundleBeat all'import

# Importa funzioni di validazione per beat esclusivi
try:
    import sys
    import os
    # Aggiungi il path per importare da bot/
    bot_path = os.path.join(os.path.dirname(__file__), '..', 'bot')
    sys.path.append(bot_path)
    from db_manager import (
        get_user_active_reservation, validate_checkout_token, cleanup_expired_reservations,
        release_bundle_reservations, reserve_bundle_exclusive_beats_with_retry, release_beat_reservation
    )
    print("✅ Funzioni di validazione beat e bundle importate correttamente")
except ImportError as e:
    print(f"⚠️ Impossibile importare funzioni di validazione beat: {e}")
    # Definisci funzioni fallback per evitare errori
    def get_user_active_reservation(user_id):
        return False, "Validazione non disponibile", None
    def validate_checkout_token(user_id, beat_id, token, timestamp):
        return False
    def cleanup_expired_reservations():
        return 0
    def release_bundle_reservations(bundle_id, user_id=None):
        return 0
    def reserve_bundle_exclusive_beats_with_retry(bundle_id, user_id, reservation_minutes=10, max_retries=3):
        return False, "Funzione non disponibile"
    def release_beat_reservation(beat_id, user_id=None):
        return False

# Gestione variabili ambiente: locale (.env) o produzione (Render.com)
if os.path.exists(os.path.join(os.path.dirname(__file__), '.env')):
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

def get_env_var(key, default=None):
    return os.environ.get(key, default)

def get_environment():
    """Determina l'ambiente di esecuzione"""
    env = get_env_var("ENVIRONMENT", "development").lower()
    return "production" if env == "production" else "development"

def get_paypal_config():
    """Ottiene configurazione PayPal basata sull'ambiente"""
    env = get_environment()
    
    if env == "production":
        return {
            "client_id": get_env_var("PROD_PAYPAL_CLIENT_ID"),
            "client_secret": get_env_var("PROD_PAYPAL_CLIENT_SECRET"),
            "webhook_id": get_env_var("PROD_PAYPAL_WEBHOOK_ID"),
            "api_base": "https://api-m.paypal.com",
            "env_name": "LIVE 💰"
        }
    else:
        return {
            "client_id": get_env_var("DEV_PAYPAL_CLIENT_ID"),
            "client_secret": get_env_var("DEV_PAYPAL_CLIENT_SECRET"), 
            "webhook_id": get_env_var("DEV_PAYPAL_WEBHOOK_ID"),
            "api_base": "https://api-m.sandbox.paypal.com",
            "env_name": "SANDBOX 🧪"
        }

def get_bot_config():
    """Ottiene configurazione Bot basata sull'ambiente"""
    env = get_environment()
    
    if env == "production":
        return {
            "internal_url": get_env_var("PROD_BOT_INTERNAL_URL"),
            "internal_token": get_env_var("PROD_INTERNAL_TOKEN")
        }
    else:
        return {
            "internal_url": get_env_var("DEV_BOT_INTERNAL_URL"),
            "internal_token": get_env_var("DEV_INTERNAL_TOKEN")
        }

def get_database_url():
    """Ottiene URL database basato sull'ambiente"""
    env = get_environment()
    
    if env == "production":
        return get_env_var("PROD_DATABASE_URL")
    else:
        return get_env_var("DEV_DATABASE_URL")

# Configurazione dinamica basata su ambiente
CURRENT_ENV = get_environment()
PAYPAL_CONFIG = get_paypal_config()
BOT_CONFIG = get_bot_config()

PAYPAL_CLIENT_ID = PAYPAL_CONFIG["client_id"]
PAYPAL_CLIENT_SECRET = PAYPAL_CONFIG["client_secret"]
PAYPAL_WEBHOOK_ID = PAYPAL_CONFIG["webhook_id"]
PAYPAL_API_BASE_URL = PAYPAL_CONFIG["api_base"]

BOT_INTERNAL_URL = BOT_CONFIG["internal_url"]
INTERNAL_TOKEN = BOT_CONFIG["internal_token"]

PAYPAL_OAUTH_URL = f"{PAYPAL_API_BASE_URL}/v1/oauth2/token"
PAYPAL_WEBHOOK_VERIFY_URL = f"{PAYPAL_API_BASE_URL}/v1/notifications/verify-webhook-signature"

# Log della configurazione ambiente al startup
print(f"🚀 WEBHOOK PAYPAL AVVIATO")
print(f"🌍 Ambiente: {PAYPAL_CONFIG['env_name']}")
print(f"🔗 PayPal API: {PAYPAL_API_BASE_URL}")
print(f"🤖 Bot URL: {BOT_INTERNAL_URL}")
print(f"💾 Database: {get_database_url()}")
print(f"=" * 50)

app = FastAPI()

Base.metadata.create_all(bind=engine)

# Endpoint di test per verificare che il webhook sia raggiungibile
@app.get("/webhook/test")
async def test_webhook():
    """Endpoint di test per verificare la connettività"""
    print(">>> [WEBHOOK] Richiesta di test ricevuta!")
    return {
        "status": "ok",
        "message": "Webhook PayPal è operativo",
        "timestamp": str(time.time()),
        "environment": CURRENT_ENV,
        "webhook_id": PAYPAL_WEBHOOK_ID[:10] + "..." if PAYPAL_WEBHOOK_ID else "MISSING",
        "server_info": {
            "host": "0.0.0.0",
            "port": int(get_env_var("PORT", "8000")),
            "webhook_verification_enabled": get_env_var("SKIP_WEBHOOK_VERIFICATION", "false").lower() != "true"
        }
    }

# Endpoint per monitorare tutte le richieste in arrivo
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Middleware per loggare tutte le richieste HTTP"""
    start_time = time.time()
    
    # Log richiesta in arrivo
    print(f">>> [REQUEST] {request.method} {request.url.path}")
    print(f">>> [REQUEST] Headers: {dict(request.headers)}")
    print(f">>> [REQUEST] Query params: {dict(request.query_params)}")
    
    # Se è una richiesta al webhook, logga il body
    if request.url.path.startswith("/webhook/") and request.method == "POST":
        body = await request.body()
        try:
            body_json = json.loads(body.decode())
            print(f">>> [REQUEST] Body JSON: {body_json}")
        except:
            print(f">>> [REQUEST] Body (raw): {body[:500]}...")  # Primi 500 caratteri
    
    # Processa la richiesta
    response = await call_next(request)
    
    # Log risposta
    process_time = time.time() - start_time
    print(f">>> [RESPONSE] Status: {response.status_code}, Time: {process_time:.3f}s")
    
    return response

@app.get("/")
async def root():
    """Endpoint root per verificare che il servizio sia attivo"""
    return {"message": "PayPal Webhook Service is running"}

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
    """
    Ottiene l'access token PayPal con timeout ottimizzato per ngrok.
    """
    url = PAYPAL_OAUTH_URL
    auth = (PAYPAL_CLIENT_ID, PAYPAL_CLIENT_SECRET)
    headers = {"Accept": "application/json", "Accept-Language": "en_US"}
    data = {"grant_type": "client_credentials"}

    # Timeout ridotto per evitare problemi con ngrok
    async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as client:
        resp = await client.post(url, auth=auth, data=data, headers=headers)
        resp.raise_for_status()
        return resp.json().get("access_token")

async def verify_paypal_webhook(body: dict, headers: dict) -> bool:
    """
    Verifica la firma del webhook PayPal con timeout ottimizzato.
    In sviluppo locale (ngrok), la verifica può essere saltata per evitare ritardi.
    """
    # SVILUPPO: Salta verifica se usando ngrok (per evitare timeout)
    if get_env_var("SKIP_WEBHOOK_VERIFICATION", "false").lower() == "true":
        print(">>> [WEBHOOK] ⚠️ Verifica webhook SALTATA (modalità sviluppo)")
        return True
    
    try:
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
        
        # Timeout ridotto per evitare ritardi con ngrok
        async with httpx.AsyncClient(timeout=httpx.Timeout(5.0)) as client:
            resp = await client.post(url, json=data, headers=headers_verify)
            resp_json = resp.json()
            is_verified = resp_json.get("verification_status") == "SUCCESS"
            print(f">>> [WEBHOOK] Verifica firma completata: {is_verified}")
            return is_verified
            
    except Exception as e:
        print(f">>> [WEBHOOK] ❌ Errore verifica firma: {e}")
        # In caso di errore di verifica, accetta il webhook in sviluppo
        if CURRENT_ENV == "development":
            print(">>> [WEBHOOK] ⚠️ Accetto webhook senza verifica (development mode)")
            return True
        return False

MAX_CONCURRENT_DOWNLOADS = 2  # Limite consigliato per Render free
download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

async def get_custom_id_from_order(order_id: str) -> str:
    """
    Recupera il custom_id dall'ordine PayPal usando l'order_id.
    """
    try:
        print(f">>> [WEBHOOK] Recupero dettagli ordine PayPal: {order_id}")
        
        # Ottieni access token PayPal
        headers = {
            "Accept": "application/json",
            "Accept-Language": "en_US",
        }
        data = "grant_type=client_credentials"
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                PAYPAL_OAUTH_URL,
                headers=headers,
                data=data,
                auth=(PAYPAL_CLIENT_ID, PAYPAL_CLIENT_SECRET)
            )
            
            if response.status_code != 200:
                print(f">>> [WEBHOOK] Errore ottenimento token: {response.status_code}")
                return None
                
            access_token = response.json().get("access_token")
            
            # Recupera dettagli ordine
            order_headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {access_token}"
            }
            
            order_response = await client.get(
                f"{PAYPAL_API_BASE_URL}/v2/checkout/orders/{order_id}",
                headers=order_headers
            )
            
            if order_response.status_code == 200:
                order_data = order_response.json()
                purchase_units = order_data.get("purchase_units", [])
                
                if purchase_units and len(purchase_units) > 0:
                    custom_id = purchase_units[0].get("custom_id") or purchase_units[0].get("reference_id")
                    print(f">>> [WEBHOOK] Custom_id trovato nell'ordine: {custom_id}")
                    return custom_id
                else:
                    print(">>> [WEBHOOK] Nessun purchase_unit trovato nell'ordine")
                    return None
            elif order_response.status_code == 404:
                print(f">>> [WEBHOOK] Ordine non esistente (404) - probabile simulazione PayPal")
                return None
            else:
                print(f">>> [WEBHOOK] Errore recupero ordine: {order_response.status_code}")
                print(f">>> [WEBHOOK] Risposta: {order_response.text}")
                return None
                
    except Exception as e:
        print(f">>> [WEBHOOK] Errore recupero custom_id da ordine: {e}")
        return None

async def parse_payment_data(resource: dict):
    """
    Estrae e parsing dei dati di pagamento dal resource PayPal.
    Gestisce sia beat singoli che bundle, e simulazioni PayPal.
    
    Returns:
        tuple: (telegram_user_id, beat_title, bundle_id, order_type)
    """
    purchase_units = resource.get("purchase_units", [])
    telegram_user_id = None
    beat_title = None
    bundle_id = None
    order_type = "beat"

    print(">>> [WEBHOOK] 📋 purchase_units:", purchase_units)

    # Cerca custom_id in vari luoghi con gestione simulazione/reale
    custom_id = None
    order_id = resource.get("supplementary_data", {}).get("related_ids", {}).get("order_id")
    is_simulation = False
    
    # Primo tentativo: direttamente nel resource (capture)
    custom_id = resource.get("custom_id") or resource.get("reference_id")
    
    # Secondo tentativo: cerca nell'ordine PayPal usando order_id (solo per eventi reali)
    if not custom_id and order_id:
        print(f">>> [WEBHOOK] 🔍 Recupero custom_id dall'ordine: {order_id}")
        custom_id = await get_custom_id_from_order(order_id)
        
        # Se l'ordine non esiste (404), probabilmente è una simulazione
        if not custom_id:
            print(">>> [WEBHOOK] 🧪 Ordine non trovato - probabile simulazione PayPal")
            is_simulation = True
    
    # Terzo tentativo: cerca nei purchase_units (se presenti)
    if not custom_id and purchase_units and isinstance(purchase_units, list) and len(purchase_units) > 0:
        custom_id = purchase_units[0].get("custom_id") or purchase_units[0].get("reference_id")
        
    print(f">>> [WEBHOOK] 🆔 custom_id: {custom_id}, is_simulation: {is_simulation}")

    # Parsing del custom_id per distinguere beat singoli da bundle
    if custom_id and ':' in custom_id:
        parts = custom_id.split(':', 2)  # user_id:type:item_name
        telegram_user_id_str = parts[0]
        
        if len(parts) >= 3:
            # Formato nuovo: user_id:type:item_name
            order_type = parts[1]  # "beat" o "bundle"
            item_name = parts[2]   # nome beat o bundle
        else:
            # Formato vecchio: user_id:item_name (assume beat)
            order_type = "beat"
            item_name = parts[1]
        
        try:
            telegram_user_id = int(telegram_user_id_str)
            beat_title = item_name.replace('_', ' ')
            
            # Se è un bundle, trova l'ID nel database
            if order_type == "bundle":
                with get_db_session() as db:
                    bundle = db.query(Bundle).filter(Bundle.name == beat_title, Bundle.is_active == 1).first()
                    if bundle:
                        bundle_id = bundle.id
                        print(f">>> [WEBHOOK] 📦 Bundle trovato: {bundle.name} (ID: {bundle_id})")
                    else:
                        print(f">>> [WEBHOOK] ❌ Bundle non trovato: {beat_title}")
                        
        except ValueError:
            print(f">>> [WEBHOOK] ❌ Errore parsing telegram_user_id: {telegram_user_id_str}")
            telegram_user_id = None
            beat_title = None
            order_type = "beat"

    # Gestione speciale per simulazioni PayPal
    if is_simulation and (telegram_user_id is None or beat_title is None):
        print(">>> [WEBHOOK] 🧪 SIMULAZIONE PayPal rilevata - genero dati di test")
        # Per le simulazioni, genera dati fittizi ma validi
        telegram_user_id = 12345  # ID fittizio per test
        beat_title = "Test Beat Simulation"
        order_type = "beat"
        bundle_id = None
        print(">>> [WEBHOOK] 🧪 Dati simulazione generati per test")

    print(f">>> [WEBHOOK] 📊 Risultato parsing:")
    print(f">>> [WEBHOOK]   - telegram_user_id: {telegram_user_id}")
    print(f">>> [WEBHOOK]   - order_type: {order_type}")
    print(f">>> [WEBHOOK]   - beat_title: {beat_title}")
    print(f">>> [WEBHOOK]   - bundle_id: {bundle_id}")

    return telegram_user_id, beat_title, bundle_id, order_type

async def send_waiting_message(user_id, beat_title, bundle_id=None, order_type="beat"):
    """
    Invia un messaggio di attesa all'utente quando l'ordine PayPal viene approvato.
    """
    try:
        url = f"{BOT_INTERNAL_URL}/internal/send_waiting_message"
        payload = {
            "user_id": user_id,
            "beat_title": beat_title,
            "bundle_id": bundle_id,
            "order_type": order_type
        }
        headers = {
            "Content-Type": "application/json"
        }
        if INTERNAL_TOKEN:
            headers["X-Internal-Token"] = INTERNAL_TOKEN
        
        print(f">>> [WEBHOOK] Invio messaggio attesa al bot: {url}")
        print(f">>> [WEBHOOK] Payload: {payload}")
        
        # Timeout più breve per messaggio di attesa
        async with httpx.AsyncClient(verify=True, timeout=5.0) as client:
            response = await client.post(url, json=payload, headers=headers)
            
            if response.status_code == 200:
                print(">>> [WEBHOOK] ✅ Messaggio attesa inviato con successo")
            else:
                print(f">>> [WEBHOOK] ⚠️ Errore messaggio attesa: {response.status_code}")
                print(f">>> [WEBHOOK] Risposta: {response.text}")
                
    except Exception as e:
        print(f">>> [WEBHOOK] ❌ Errore invio messaggio attesa: {e}")
        # Non solleva l'eccezione per non bloccare il webhook

async def notify_user_via_bot(user_id, beat_title, bundle_id=None, order_type="beat", transaction_id=None):
    """
    Notifica l'utente tramite il bot Telegram con retry e timeout ottimizzati.
    """
    max_retries = 3
    base_timeout = 15.0  # Timeout più alto per bundle grandi
    
    for attempt in range(max_retries):
        try:
            url = f"{BOT_INTERNAL_URL}/internal/send_message"
            payload = {
                "user_id": user_id,
                "beat_title": beat_title,
                "bundle_id": bundle_id,
                "order_type": order_type,
                "transaction_id": transaction_id
            }
            headers = {
                "Content-Type": "application/json"
            }
            if INTERNAL_TOKEN:
                headers["X-Internal-Token"] = INTERNAL_TOKEN
            
            # Timeout progressivo: primo tentativo più lungo, successivi più corti
            current_timeout = base_timeout if attempt == 0 else 8.0
            
            if attempt > 0:
                print(f">>> [WEBHOOK] 🔄 Tentativo {attempt + 1}/{max_retries} - timeout: {current_timeout}s")
            
            print(f">>> [WEBHOOK] Invio notifica al bot: {url}")
            print(f">>> [WEBHOOK] Payload: {payload}")
            
            async with httpx.AsyncClient(verify=True, timeout=httpx.Timeout(current_timeout)) as client:
                response = await client.post(url, json=payload, headers=headers)
                
                if response.status_code == 200:
                    # Verifica il contenuto della risposta per successo reale
                    try:
                        response_data = response.json()
                        status = response_data.get("status", "unknown")
                        
                        if status == "ok":
                            print(">>> [WEBHOOK] ✅ Beat inviato con successo al bot")
                            return True  # Successo completo
                        elif status == "partial":
                            sent = response_data.get("sent", 0)
                            total = response_data.get("total", 1)
                            print(f">>> [WEBHOOK] ⚠️ Invio parziale: {sent}/{total} beat inviati")
                            return False  # Successo parziale = fallimento per beat esclusivi
                        elif status == "error":
                            message = response_data.get("message", "Unknown error")
                            print(f">>> [WEBHOOK] ❌ Errore bot: {message}")
                            return False  # Fallimento
                        else:
                            print(f">>> [WEBHOOK] ❓ Status sconosciuto: {status}")
                            return False
                    except Exception as parse_error:
                        print(f">>> [WEBHOOK] ⚠️ Errore parsing risposta bot: {parse_error}")
                        print(f">>> [WEBHOOK] Risposta raw: {response.text}")
                        return False
                else:
                    print(f">>> [WEBHOOK] ⚠️ Errore notifica bot: {response.status_code}")
                    print(f">>> [WEBHOOK] Risposta: {response.text}")
                    
                    # Se è un errore client (4xx), non ritentare
                    if 400 <= response.status_code < 500:
                        print(f">>> [WEBHOOK] ❌ Errore client {response.status_code} - non ritentatile")
                        return False
                        
        except asyncio.TimeoutError:
            print(f">>> [WEBHOOK] ⏰ Timeout tentativo {attempt + 1} ({current_timeout}s)")
        except Exception as e:
            print(f">>> [WEBHOOK] ❌ Errore tentativo {attempt + 1}: {e}")
        
        # Pausa progressiva tra i tentativi
        if attempt < max_retries - 1:
            wait_time = 2 ** attempt  # 1s, 2s, 4s
            print(f">>> [WEBHOOK] ⏳ Attesa {wait_time}s prima del prossimo tentativo...")
            await asyncio.sleep(wait_time)
    
    print(f">>> [WEBHOOK] ❌ Fallimento dopo {max_retries} tentativi - notifica bot non inviata")
    return False  # Fallimento definitivo

@app.post("/webhook/paypal")
async def paypal_webhook(request: Request):
    """
    Webhook PayPal ottimizzato per sviluppo locale con ngrok.
    Gestisce rapidamente i webhook per evitare timeout.
    """
    start_time = asyncio.get_event_loop().time()
    print(f">>> [WEBHOOK] ⏰ Richiesta ricevuta alle {start_time}")
    
    try:
        body = await request.json()
        headers = request.headers

        print(">>> [WEBHOOK] 📨 Body ricevuto:", body)
        print(">>> [WEBHOOK] 📋 Headers chiave ricevuti:", {
            k: v for k, v in dict(headers).items() 
            if k.lower().startswith('paypal-') or k.lower() in ['user-agent', 'content-type']
        })

        # Risposta rapida per eventi non critici
        event_type = body.get("event_type")
        webhook_id = body.get("id", "UNKNOWN")
        
        print(f">>> [WEBHOOK] 🎯 Evento ricevuto: {event_type} (ID: {webhook_id})")
        
        if event_type not in ["PAYMENT.CAPTURE.COMPLETED", "CHECKOUT.ORDER.APPROVED"]:
            print(f">>> [WEBHOOK] ⏭️ Evento ignorato rapidamente: {event_type}")
            return {"status": "ok", "message": f"Event {event_type} acknowledged"}

        # Verifica webhook con timeout ottimizzato
        try:
            print(">>> [WEBHOOK] 🔐 Inizio verifica firma...")
            verified = await verify_paypal_webhook(body, headers)
            print(f">>> [WEBHOOK] 🔐 Verifica firma completata: {verified}")
        except Exception as e:
            print(f">>> [WEBHOOK] ❌ Errore verifica firma: {e}")
            # In sviluppo, continua anche se la verifica fallisce
            if CURRENT_ENV == "development":
                print(">>> [WEBHOOK] ⚠️ Continuo senza verifica (development mode)")
                verified = True
            else:
                raise HTTPException(status_code=400, detail="Webhook verification error")

        if not verified:
            print(">>> [WEBHOOK] ❌ Verifica firma fallita, richiesta rifiutata.")
            raise HTTPException(status_code=400, detail="Webhook verification failed")

        resource = body.get("resource")
        print(f">>> [WEBHOOK] 🎯 Elaborazione evento: {event_type}")

        # GESTIONE EVENTO CHECKOUT.ORDER.APPROVED: invia messaggio di attesa
        if event_type == "CHECKOUT.ORDER.APPROVED":
            print(">>> [WEBHOOK] ✅ Ordine approvato - invio messaggio di attesa")
            
            # Parsing custom_id per inviare messaggio di attesa
            telegram_user_id, beat_title, bundle_id, order_type = await parse_payment_data(resource)
            
            if telegram_user_id and beat_title:
                await send_waiting_message(telegram_user_id, beat_title, bundle_id, order_type)
                print(f">>> [WEBHOOK] 📱 Messaggio di attesa inviato a user {telegram_user_id}")
            else:
                print(">>> [WEBHOOK] ⚠️ Impossibile inviare messaggio di attesa - dati mancanti")
            
            return {"status": "ok", "message": "Order approved - waiting message sent"}

        # GESTIONE EVENTO PAYMENT.CAPTURE.COMPLETED: processo completo
        if event_type == "PAYMENT.CAPTURE.COMPLETED":
            print(">>> [WEBHOOK] 💰 Elaborazione pagamento completato...")
            
            # Estrai dati base rapidamente
            transaction_id = resource.get("id")
            webhook_id = body.get("id")  # ID univoco del webhook event
            payer_email = resource.get("payer", {}).get("email_address", "")
            amount = float(resource.get("amount", {}).get("value", 0))
            currency = resource.get("amount", {}).get("currency_code", "")

            print(f">>> [WEBHOOK] 💳 Transazione: {transaction_id}, Importo: {amount} {currency}")
            print(f">>> [WEBHOOK] 🆔 Webhook Event ID: {webhook_id}")

            # IDEMPOTENZA RAFFORZATA: Controlla sia transaction_id che webhook event_id
            with get_db_session() as db:
                # Controllo primario: transaction_id
                exists_txn = db.query(Order).filter(Order.transaction_id == transaction_id).first()
                
                # Controllo secondario: blocca eventi webhook duplicati per 5 minuti
                import sqlite3
                import tempfile
                temp_cache = os.path.join(tempfile.gettempdir(), "paypal_webhook_cache.db")
                
                try:
                    # Semplice cache SQLite per eventi recenti
                    cache_conn = sqlite3.connect(temp_cache)
                    cache_conn.execute('''
                        CREATE TABLE IF NOT EXISTS webhook_events (
                            webhook_id TEXT PRIMARY KEY,
                            timestamp REAL,
                            transaction_id TEXT
                        )
                    ''')
                    
                    # Pulisci eventi vecchi (>5 minuti)
                    cache_conn.execute('DELETE FROM webhook_events WHERE timestamp < ?', (time.time() - 300,))
                    
                    # Controlla se questo webhook_id è già stato processato
                    existing_webhook = cache_conn.execute(
                        'SELECT timestamp FROM webhook_events WHERE webhook_id = ?', 
                        (webhook_id,)
                    ).fetchone()
                    
                    if existing_webhook:
                        print(f">>> [WEBHOOK] 🚫 Webhook event duplicato bloccato: {webhook_id}")
                        print(f">>> [WEBHOOK] 🚫 Processato {time.time() - existing_webhook[0]:.1f}s fa")
                        cache_conn.close()
                        return {"status": "ok", "message": "Duplicate webhook event blocked"}
                    
                    # Registra questo webhook event
                    cache_conn.execute(
                        'INSERT INTO webhook_events (webhook_id, timestamp, transaction_id) VALUES (?, ?, ?)',
                        (webhook_id, time.time(), transaction_id)
                    )
                    cache_conn.commit()
                    cache_conn.close()
                    
                except Exception as cache_error:
                    print(f">>> [WEBHOOK] ⚠️ Errore cache webhook: {cache_error}")
                    # Continua anche se la cache fallisce
                
                if exists_txn:
                    print(f">>> [WEBHOOK] ⚠️ Transazione già processata: {transaction_id}")
                    print(f">>> [WEBHOOK] ⚠️ Ordine esistente: ID={exists_txn.id}, User={exists_txn.telegram_user_id}, Beat='{exists_txn.beat_title}'")
                    
                    # Verifica se l'utente ha già ricevuto i file (controllo con timestamp)
                    try:
                        if hasattr(exists_txn, 'created_at') and exists_txn.created_at:
                            elapsed_since_order = time.time() - exists_txn.created_at.timestamp()
                        else:
                            elapsed_since_order = 999  # Tratta come vecchio se non ha timestamp
                            
                        if elapsed_since_order < 300:  # 5 minuti
                            print(f">>> [WEBHOOK] ✅ Ordine recente ({elapsed_since_order:.1f}s fa) - probabilmente già processato")
                        else:
                            print(f">>> [WEBHOOK] ⚠️ Ordine vecchio ({elapsed_since_order/60:.1f}min fa) - potrebbe essere un retry")
                            
                            # Per ordini vecchi, prova a rinviare i file (safety net)
                            print(">>> [WEBHOOK] 🔄 Tentativo re-invio per ordine vecchio...")
                            await notify_user_via_bot(
                                user_id=exists_txn.telegram_user_id,
                                beat_title=exists_txn.beat_title,
                                bundle_id=exists_txn.bundle_id,
                                order_type=exists_txn.order_type,
                                transaction_id=transaction_id
                            )
                    except Exception as timestamp_error:
                        print(f">>> [WEBHOOK] ⚠️ Errore controllo timestamp: {timestamp_error}")
                    
                    return {"status": "ok", "message": "Transaction already processed (idempotent)"}

            # Parsing custom_id con gestione errori robusta
            telegram_user_id, beat_title, bundle_id, order_type = await parse_payment_data(resource)
            
            if not telegram_user_id or not beat_title:
                print(f">>> [WEBHOOK] ❌ Dati pagamento non validi: user_id={telegram_user_id}, beat='{beat_title}'")
                # Salva comunque il record per debug
                with get_db_session() as db:
                    new_order = Order(
                        transaction_id=transaction_id,
                        beat_id=None,
                        bundle_id=bundle_id,
                        order_type="unknown",
                        payer_email=payer_email,
                        amount=amount,
                        currency=currency,
                        token=resource.get("custom_token"),
                        telegram_user_id=telegram_user_id or 0,
                        beat_title=beat_title or "PARSING_FAILED"
                    )
                    db.add(new_order)
                    db.commit()
                raise HTTPException(status_code=400, detail="Invalid payment data - saved for debug")

            # CONTROLLO RACE CONDITION: Verifica che i beat esclusivi siano ancora disponibili
            # Questo controllo deve essere fatto PRIMA di salvare l'ordine per prevenire vendite duplicate
            with get_db_session() as db:
                if order_type == "bundle" and bundle_id:
                    # Per i bundle, verifica se il bundle esiste ancora e contiene beat
                    bundle = db.query(Bundle).filter(Bundle.id == bundle_id).first()
                    if not bundle:
                        print(f">>> [WEBHOOK] ❌ Bundle {bundle_id} non trovato nel database!")
                        raise HTTPException(status_code=404, detail="Bundle not found")
                    
                    # Verifica che il bundle contenga ancora dei beat (esclusivi o meno)
                    beats_in_bundle = db.query(Beat).join(BundleBeat).filter(
                        BundleBeat.bundle_id == bundle_id
                    ).all()
                    
                    if not beats_in_bundle:
                        print(f">>> [WEBHOOK] ❌ Bundle {bundle_id} è vuoto - nessun beat disponibile!")
                        raise HTTPException(status_code=409, detail="Bundle is empty - no beats available")
                    
                    # Se il bundle contiene beat esclusivi, verifica che siano ancora disponibili
                    exclusive_beats_in_bundle = [beat for beat in beats_in_bundle if beat.is_exclusive == 1]
                    
                    if exclusive_beats_in_bundle:
                        print(f">>> [WEBHOOK] 🔒 Bundle {bundle_id} contiene {len(exclusive_beats_in_bundle)} beat esclusivi")
                        print(f">>> [WEBHOOK] 📦 Beat totali nel bundle: {len(beats_in_bundle)}")
                        
                        # Per bundle con beat esclusivi, questo è normale - il pagamento dovrebbe procedere
                        # La rimozione dei beat esclusivi avverrà DOPO l'invio dei file
                        print(f">>> [WEBHOOK] ✅ Bundle {bundle_id} valido per l'acquisto")
                    else:
                        # Bundle contiene solo beat non esclusivi - sempre OK
                        print(f">>> [WEBHOOK] 📦 Bundle {bundle_id} contiene solo beat non esclusivi - sempre disponibile")
                        
                elif order_type == "beat":
                    # Controlla se il beat singolo è esclusivo
                    exclusive_beat = db.query(Beat).filter(
                        Beat.title == beat_title,
                        Beat.is_exclusive == 1
                    ).first()
                    
                    if exclusive_beat:
                        print(f">>> [WEBHOOK] 🔒 Beat esclusivo: {beat_title} (ID: {exclusive_beat.id})")
                        
                        # VALIDAZIONE PRENOTAZIONE: Verifica che l'utente abbia effettivamente prenotato questo beat
                        print(f">>> [WEBHOOK] 🔍 Verifica prenotazione per utente {telegram_user_id}...")
                        try:
                            cleanup_expired_reservations()  # Pulisci prenotazioni scadute
                            has_reservation, reservation_info, reserved_beat_id = get_user_active_reservation(telegram_user_id)
                            
                            if not has_reservation or reserved_beat_id != exclusive_beat.id:
                                print(f">>> [WEBHOOK] ❌ PRENOTAZIONE NON VALIDA!")
                                print(f">>> [WEBHOOK] ❌ Utente {telegram_user_id} non ha prenotazione attiva per beat {exclusive_beat.id}")
                                print(f">>> [WEBHOOK] ❌ Has reservation: {has_reservation}, Reserved beat: {reserved_beat_id}")
                                print(f">>> [WEBHOOK] ❌ Link di pagamento salvato utilizzato impropriamente!")
                                
                                # Log di sicurezza per debugging
                                with open("/tmp/paypal_security_violations.log", "a") as f:
                                    import datetime
                                    f.write(f"{datetime.datetime.now()}: INVALID_RESERVATION - User {telegram_user_id} tried to pay for beat {exclusive_beat.id} without valid reservation. Transaction: {transaction_id}\n")
                                
                                raise HTTPException(status_code=409, detail="Invalid payment - beat reservation required")
                            else:
                                print(f">>> [WEBHOOK] ✅ Prenotazione valida confermata per utente {telegram_user_id}")
                                print(f">>> [WEBHOOK] ✅ {reservation_info}")
                        except Exception as validation_error:
                            print(f">>> [WEBHOOK] ⚠️ Errore validazione prenotazione: {validation_error}")
                            # In caso di errore nel sistema di validazione, permetti il pagamento
                            # ma logga l'evento per verifica manuale
                            print(f">>> [WEBHOOK] ⚠️ Continuando con pagamento per errore sistema di validazione")
                        
                        print(f">>> [WEBHOOK] ⚡ Verifica race condition superata - beat ancora disponibile")
                    else:
                        # Beat standard/scontato (non esclusivo)
                        print(f">>> [WEBHOOK] 📦 Beat standard: {beat_title} (nessuna limitazione di prenotazione)")
                        
                        # Per beat non esclusivi, verifica solo che esista nel database
                        standard_beat = db.query(Beat).filter(
                            Beat.title == beat_title,
                            Beat.is_exclusive == 0  # Solo beat non esclusivi
                        ).first()
                        
                        if not standard_beat:
                            print(f">>> [WEBHOOK] ❌ Beat standard '{beat_title}' non trovato nel database")
                            raise HTTPException(status_code=404, detail="Beat not found")
                        else:
                            print(f">>> [WEBHOOK] ✅ Beat standard '{beat_title}' disponibile (ID: {standard_beat.id})")

            # Salva ordine nel database PRIMA di processare
            with get_db_session() as db:
                new_order = Order(
                    transaction_id=transaction_id,
                    beat_id=None,
                    bundle_id=bundle_id,
                    order_type=order_type,
                    payer_email=payer_email,
                    amount=amount,
                    currency=currency,
                    token=resource.get("custom_token"),
                    telegram_user_id=telegram_user_id,
                    beat_title=beat_title
                )
                db.add(new_order)
                db.commit()
                print(">>> [WEBHOOK] 💾 Ordine salvato nel database")

            # Notifica utente (processo asincrono con timeout aumentato)
            print(">>> [WEBHOOK] 📱 Invio notifica utente...")
            notification_success = False
            try:
                notification_success = await notify_user_via_bot(
                    user_id=telegram_user_id,
                    beat_title=beat_title,
                    bundle_id=bundle_id,
                    order_type=order_type,
                    transaction_id=transaction_id
                )
                if notification_success:
                    print(">>> [WEBHOOK] ✅ Beat inviato con successo - notifica completata")
                else:
                    print(">>> [WEBHOOK] ❌ Invio beat fallito - notifica non riuscita")
            except Exception as notify_error:
                print(f">>> [WEBHOOK] ❌ Errore notifica utente: {notify_error}")
                print(">>> [WEBHOOK] ⚠️ Beat non rimosso a causa dell'errore di notifica")
                # NON continuare con cleanup se notifica fallisce
                notification_success = False

            # Rilascia prenotazione per beat esclusivi acquistati SOLO se notifica è riuscita
            if notification_success:
                try:
                    if order_type == "beat":
                        # Trova il beat per rilasciare la prenotazione
                        with get_db_session() as db:
                            beat = db.query(Beat).filter(
                                Beat.title == beat_title,
                                Beat.is_exclusive == 1
                            ).first()
                            
                            if beat:
                                # Rilascia la prenotazione usando la funzione già importata
                                try:
                                    success = release_beat_reservation(beat.id, telegram_user_id)
                                    if success:
                                        print(f">>> [WEBHOOK] 🔓 Prenotazione rilasciata per beat {beat.id} - utente {telegram_user_id}")
                                    else:
                                        print(f">>> [WEBHOOK] ⚠️ Impossibile rilasciare prenotazione beat {beat.id} - potrebbe essere già rilasciata")
                                except Exception as release_error:
                                    print(f">>> [WEBHOOK] ❌ Errore rilascio prenotazione: {release_error}")
                                    
                    elif order_type == "bundle" and bundle_id:
                        # Per i bundle, rilascia tutte le prenotazioni dei beat esclusivi nel bundle
                        try:
                            released_count = release_bundle_reservations(bundle_id, telegram_user_id)
                            if released_count > 0:
                                print(f">>> [WEBHOOK] 🔓 {released_count} prenotazioni bundle rilasciate per bundle {bundle_id} - utente {telegram_user_id}")
                            else:
                                print(f">>> [WEBHOOK] ⚠️ Nessuna prenotazione bundle da rilasciare per bundle {bundle_id}")
                        except Exception as release_error:
                            print(f">>> [WEBHOOK] ❌ Errore rilascio prenotazioni bundle: {release_error}")
                            
                except Exception as reservation_error:
                    print(f">>> [WEBHOOK] ❌ Errore generale rilascio prenotazioni: {reservation_error}")
            else:
                print(">>> [WEBHOOK] ⚠️ Rilascio prenotazioni saltato - notifica fallita")

            # Cleanup beat esclusivi SOLO se notifica è riuscita
            if notification_success:
                try:
                    if order_type == "bundle" and bundle_id:
                        cleanup_result = remove_exclusive_beats_from_bundle(bundle_id)
                        
                        if cleanup_result["bundle_deleted"]:
                            print(f">>> [WEBHOOK] 🗑️ Bundle eliminato: {cleanup_result['message']}")
                        elif cleanup_result["bundle_updated"]:
                            print(f">>> [WEBHOOK] 📝 Bundle aggiornato: {cleanup_result['message']}")
                            print(f">>> [WEBHOOK] 💰 Prezzo: {cleanup_result['old_price']}€ → {cleanup_result['new_price']}€")
                        elif cleanup_result["removed_beats"] > 0:
                            print(f">>> [WEBHOOK] 🗑️ {cleanup_result['removed_beats']} beat esclusivi rimossi dal bundle")
                    else:
                        removed = remove_exclusive_beat_by_title(beat_title)
                        if removed:
                            print(f">>> [WEBHOOK] 🗑️ Beat esclusivo '{beat_title}' rimosso")
                except Exception as cleanup_error:
                    print(f">>> [WEBHOOK] ❌ Errore cleanup beat esclusivi: {cleanup_error}")
                    # Non è critico, continua
            else:
                print(">>> [WEBHOOK] ⚠️ Cleanup beat esclusivi saltato - notifica fallita")

            elapsed = asyncio.get_event_loop().time() - start_time
            print(f">>> [WEBHOOK] ✅ Webhook completato in {elapsed:.2f}s")
            return {"status": "ok", "message": "Payment processed successfully", "elapsed_time": f"{elapsed:.2f}s"}

        print(f">>> [WEBHOOK] ❓ Evento non gestito: {event_type}")
        return {"status": "ignored", "message": f"Unhandled event type {event_type}"}
        
    except HTTPException:
        raise
    except Exception as e:
        print(f">>> [WEBHOOK] 💥 Errore critico: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal webhook error")

# --- INIZIO: Funzioni importate da utils.py, ora locali ---

def remove_exclusive_beat_by_title(title: str):
    """Rimuove un beat esclusivo dal database dato il titolo."""
    with get_db_session() as session:
        beat = session.query(Beat).filter_by(title=title, is_exclusive=1).first()
        if beat:
            print(f"[INFO] Rimuovendo beat esclusivo '{title}' (ID: {beat.id})")
            
            # PASSO 1: Rimuovi PRIMA tutte le relazioni bundle_beats per questo beat
            bundle_beat_relations = session.query(BundleBeat).filter(BundleBeat.beat_id == beat.id).all()
            for relation in bundle_beat_relations:
                print(f"[INFO] Rimuovendo relazione bundle_beats: bundle_id={relation.bundle_id}, beat_id={relation.beat_id}")
                session.delete(relation)
            
            # PASSO 2: Rimuovi il beat solo DOPO aver rimosso le relazioni
            session.delete(beat)
            session.commit()
            print(f"[INFO] Beat esclusivo '{title}' e le sue relazioni rimossi dal database dopo l'acquisto.")
            return True
    return False

def remove_exclusive_beats_from_bundle(bundle_id: int):
    """
    Rimuove tutti i beat esclusivi contenuti in un bundle dal database.
    Se il bundle rimane vuoto (solo beat esclusivi), elimina anche il bundle.
    Se rimangono beat non esclusivi, ricalcola il prezzo del bundle.
    
    Returns:
        dict: Informazioni sui cambiamenti effettuati
    """
    with get_db_session() as session:
        # Trova il bundle
        bundle = session.query(Bundle).filter(Bundle.id == bundle_id).first()
        if not bundle:
            print(f"[WARNING] Bundle ID {bundle_id} non trovato")
            return {"removed_beats": 0, "bundle_deleted": False, "bundle_updated": False}
        
        # Trova tutti i beat nel bundle (esclusivi e non)
        all_beats_in_bundle = session.query(Beat).join(BundleBeat).filter(
            BundleBeat.bundle_id == bundle_id
        ).all()
        
        # Separa beat esclusivi da quelli normali
        exclusive_beats = [beat for beat in all_beats_in_bundle if beat.is_exclusive]
        non_exclusive_beats = [beat for beat in all_beats_in_bundle if not beat.is_exclusive]
        
        print(f"[INFO] Bundle '{bundle.name}' (ID: {bundle_id}):")
        print(f"[INFO]   - Beat totali: {len(all_beats_in_bundle)}")
        print(f"[INFO]   - Beat esclusivi: {len(exclusive_beats)}")
        print(f"[INFO]   - Beat non esclusivi: {len(non_exclusive_beats)}")
        
        removed_count = 0
        
        # Rimuovi tutti i beat esclusivi
        for beat in exclusive_beats:
            print(f"[INFO] Rimuovendo beat esclusivo '{beat.title}' (ID: {beat.id}) dal bundle ID: {bundle_id}")
            
            # PASSO 1: Rimuovi PRIMA tutte le relazioni bundle_beats per questo beat
            bundle_beat_relations = session.query(BundleBeat).filter(BundleBeat.beat_id == beat.id).all()
            for relation in bundle_beat_relations:
                print(f"[INFO] Rimuovendo relazione bundle_beats: bundle_id={relation.bundle_id}, beat_id={relation.beat_id}")
                session.delete(relation)
            
            # PASSO 2: Rimuovi il beat solo DOPO aver rimosso le relazioni
            session.delete(beat)
            removed_count += 1
            print(f"[INFO] Beat esclusivo '{beat.title}' e le sue relazioni rimossi dal database.")
        
        # Decidi cosa fare con il bundle
        if len(non_exclusive_beats) == 0:
            # Bundle vuoto (conteneva solo beat esclusivi) -> ELIMINA IL BUNDLE
            print(f"[INFO] Bundle '{bundle.name}' conteneva solo beat esclusivi -> ELIMINAZIONE BUNDLE")
            session.delete(bundle)
            session.commit()
            
            return {
                "removed_beats": removed_count,
                "bundle_deleted": True,
                "bundle_updated": False,
                "message": f"Bundle '{bundle.name}' eliminato (conteneva solo beat esclusivi)"
            }
        else:
            # Bundle contiene ancora beat non esclusivi -> RICALCOLA PREZZO
            print(f"[INFO] Bundle '{bundle.name}' contiene ancora {len(non_exclusive_beats)} beat non esclusivi -> RICALCOLO PREZZO")
            
            # Calcola nuovo prezzo individuale (somma dei beat rimasti)
            new_individual_price = sum(beat.price for beat in non_exclusive_beats)
            
            # Mantieni la stessa percentuale di sconto o applica uno minimo
            original_discount_percent = bundle.discount_percent
            min_discount = 10  # Sconto minimo del 10%
            
            # Usa il discount originale se >= 10%, altrimenti 10%
            new_discount_percent = max(original_discount_percent, min_discount)
            new_bundle_price = new_individual_price * (1 - new_discount_percent / 100)
            
            # Aggiorna il bundle
            old_bundle_price = bundle.bundle_price
            old_individual_price = bundle.individual_price
            
            bundle.individual_price = new_individual_price
            bundle.bundle_price = new_bundle_price
            bundle.discount_percent = new_discount_percent
            
            print(f"[INFO] Aggiornamento prezzi bundle '{bundle.name}':")
            print(f"[INFO]   - Prezzo individuale: {old_individual_price}€ → {new_individual_price}€")
            print(f"[INFO]   - Prezzo bundle: {old_bundle_price}€ → {new_bundle_price}€")
            print(f"[INFO]   - Sconto: {new_discount_percent}%")
            
            session.commit()
            
            return {
                "removed_beats": removed_count,
                "bundle_deleted": False,
                "bundle_updated": True,
                "old_price": old_bundle_price,
                "new_price": new_bundle_price,
                "remaining_beats": len(non_exclusive_beats),
                "message": f"Bundle '{bundle.name}' aggiornato: {removed_count} beat esclusivi rimossi, prezzo ricalcolato"
            }
        
        return {"removed_beats": 0, "bundle_deleted": False, "bundle_updated": False}

# --- FINE: Funzioni importate da utils.py, ora locali ---

if __name__ == "__main__":
    import uvicorn
    print(">>> [WEBHOOK] 🚀 Server PayPal Webhook avviato")
    print(">>> [WEBHOOK] 📡 Endpoint: /webhook/paypal")
    print(">>> [WEBHOOK] 🏥 Health check: /webhook/test")
    print(">>> [WEBHOOK] 🌍 Environment:", CURRENT_ENV.upper())
    print(">>> [WEBHOOK] 🆔 Webhook ID:", PAYPAL_WEBHOOK_ID[:10] + "..." if PAYPAL_WEBHOOK_ID else "❌ MISSING")
    
    # Configurazione ottimizzata per ngrok
    is_development = get_env_var("ENV", "development").lower() == "development"
    skip_verification = get_env_var("SKIP_WEBHOOK_VERIFICATION", "false").lower() == "true"
    
    if is_development:
        print(">>> [WEBHOOK] 🔧 Modalità sviluppo attiva")
        if skip_verification:
            print(">>> [WEBHOOK] ⚠️  Verifica webhook DISABILITATA per sviluppo")
        print(">>> [WEBHOOK] 💡 Usa 'python start_webhook.py' per setup guidato")
    
    # Configurazione server ottimizzata per ngrok
    config = {
        "host": "0.0.0.0",
        "port": int(get_env_var("PORT", "8000")),
        "reload": False,  # Disabilitato per evitare problemi con ngrok
        "access_log": True,
        "log_level": "info" if is_development else "warning",
        # Timeout ottimizzati per ngrok
        "timeout_keep_alive": 75,  # Maggiore del timeout ngrok (60s)
        "timeout_graceful_shutdown": 10
    }
    
    print(">>> [WEBHOOK] ⚙️  Configurazione server:", config)
    print(">>> [WEBHOOK] ✅ Server pronto - in ascolto...")
    
    # Avvia server con configurazione ottimizzata
    uvicorn.run(app, **config)


