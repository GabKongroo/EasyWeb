import os

# Carica sempre il file .env dalla cartella corrente se esiste
env_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(env_path):
    from dotenv import load_dotenv
    load_dotenv(env_path)

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL non impostato! Verifica le variabili di ambiente o il file .env.")

from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()

class Beat(Base):
    __tablename__ = "beats"
    id = Column(Integer, primary_key=True)
    genre = Column(String(50), nullable=False)
    mood = Column(String(50), nullable=False)
    folder = Column(String(50), nullable=False)
    title = Column(String(100), nullable=False)
    preview_key = Column(String(255), nullable=False)
    file_key = Column(String(255), nullable=False)
    image_key = Column(String(255), nullable=False)
    price = Column(Float, nullable=False, default=19.99)
    original_price = Column(Float, nullable=True)
    is_exclusive = Column(Integer, nullable=False, default=0)
    is_discounted = Column(Integer, nullable=False, default=0)
    discount_percent = Column(Integer, nullable=False, default=0)
    orders = relationship("Order", back_populates="beat")

class Order(Base):
    __tablename__ = "orders"
    id = Column(Integer, primary_key=True)
    transaction_id = Column(String(255), unique=True, nullable=False)
    telegram_user_id = Column(Integer, nullable=False)
    beat_title = Column(String(255), nullable=False)
    payer_email = Column(String(255), nullable=False)
    amount = Column(Float, nullable=False)
    currency = Column(String(10), nullable=False)
    token = Column(String(255), nullable=True)
    beat_id = Column(Integer, ForeignKey("beats.id"), nullable=True)
    beat = relationship("Beat", back_populates="orders")

def get_session():
    return SessionLocal()
def get_session():
    return SessionLocal()
