"""
Proferta - Crawler Autónomo para Bienes Raíces (Venta)
------------------------------------------------------
Stack: Python 3.11+, SQLAlchemy, Pydantic, Phonenumbers, Celery/Asyncio.
Cumple estrictamente con las especificaciones técnicas de arquitectura y calidad:
- Pipeline modular en 7/8 etapas.
- Extracción exclusiva de propiedades en VENTA con antigüedad < 90 días.
- Límite estricto de hasta 3 fotos por propiedad.
- Sanitización anti-fuga (Regex de teléfonos/patrones + OCR mock).
- Normalización a esquema JSON estricto para tags.
- Identidad canónica de agentes vía E.164 + chequeo en suppression list.
- Deduplicación near-exact con rango de precios automático.
- Publicación PASIVA en PostgreSQL (CERO disparos de emails o contacto en frío).
- Umbral de acceso a "Búsquedas particulares": USD 60/mes o superior.

NOTA IMPORTANTE — pendiente antes de producción:
- check_photo_ocr_leak() es un STUB (siempre devuelve False). Falta integrar
  un proveedor real de OCR (Google Vision API o Tesseract) antes de publicar
  fotos en un entorno real — hoy ninguna foto queda efectivamente filtrada.
"""

import enum
import json
import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import phonenumbers
from pydantic import BaseModel, Field
from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    or_,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Session, declarative_base, relationship, sessionmaker

# JSON portable: en Postgres (producción) usa JSONB real; en cualquier otro
# motor (ej. SQLite, usado en la demo de este archivo) cae a JSON genérico.
JSONType = JSON().with_variant(JSONB(), "postgresql")

# Configuración de Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("proferta_crawler")

# Umbral de suscripción para acceder a "Búsquedas particulares" (Busco propiedad)
# Definido en la documentación de negocio: USD 60/mes o superior.
BUSQUEDAS_PARTICULARES_MIN_TIER_USD = 60

# ==============================================================================
# 1. MODELOS DE BASE DE DATOS (SQLAlchemy ORM)
# ==============================================================================

Base = declarative_base()

class SnapshotStatus(str, enum.Enum):
    PENDING = "pending"
    PROCESSED = "processed"
    ERROR = "error"

class AccountStatus(str, enum.Enum):
    PENDING = "pending"
    ACTIVE = "active"

class MatriculaStatus(str, enum.Enum):
    VERIFICADO = "verificado"
    SIN_MATRICULA = "sin_matricula"

class PropertyStatus(str, enum.Enum):
    ACTIVA = "activa"
    PAUSADA = "pausada"
    VENDIDA = "vendida"
    RETIRADA = "retirada"
    PENDIENTE_VERIFICACION = "pendiente_verificacion"

class PriceDisplayMode(str, enum.Enum):
    FIJO = "fijo"
    RANGO = "rango"
    CONSULTAR = "consultar"

class RawSnapshot(Base):
    __tablename__ = "raw_snapshots"

    id = Column(Integer, primary_key=True)
    source_portal = Column(String(50), nullable=False)
    source_url = Column(Text, nullable=False, unique=True)
    raw_content = Column(Text, nullable=False)
    fetched_at = Column(DateTime, default=datetime.utcnow)
    status = Column(Enum(SnapshotStatus), default=SnapshotStatus.PENDING)

class AgentSuppressionList(Base):
    __tablename__ = "agent_suppression_list"

    id = Column(Integer, primary_key=True)
    phone_e164 = Column(String(30), unique=True, nullable=False)
    email = Column(String(255), nullable=True)
    opted_out_at = Column(DateTime, default=datetime.utcnow)
    reason = Column(Text, nullable=True)

class Agent(Base):
    __tablename__ = "agents"

    id = Column(Integer, primary_key=True)
    phone_e164 = Column(String(30), unique=True, nullable=False)
    email = Column(String(255), nullable=True)
    name = Column(String(255), nullable=True)
    matricula_number = Column(String(100), nullable=True)
    matricula_status = Column(Enum(MatriculaStatus), default=MatriculaStatus.SIN_MATRICULA)
    account_status = Column(Enum(AccountStatus), default=AccountStatus.PENDING)
    subscription_tier = Column(String(50), nullable=True, default=None)  # "free" o "tier_50_usd"
    created_at = Column(DateTime, default=datetime.utcnow)

class ZonesCatalog(Base):
    __tablename__ = "zones_catalog"

    id = Column(Integer, primary_key=True)
    province = Column(String(100), nullable=False)
    city = Column(String(100), nullable=False)
    neighborhood = Column(String(100), nullable=False)
    aliases = Column(JSONType, nullable=False, default=[])

class Property(Base):
    __tablename__ = "properties"

    id = Column(Integer, primary_key=True)
    canonical_title = Column(String(255), nullable=False)
    canonical_description_tags = Column(JSONType, nullable=False)
    operation_type = Column(String(20), default="venta", nullable=False)
    price_amount = Column(Numeric(12, 2), nullable=True)
    price_max_amount = Column(Numeric(12, 2), nullable=True)  # Se utiliza si mode == RANGO
    price_currency = Column(String(10), default="USD")
    price_display_mode = Column(Enum(PriceDisplayMode), default=PriceDisplayMode.FIJO)
    zone_id = Column(Integer, ForeignKey("zones_catalog.id"), nullable=True)
    address_raw = Column(Text, nullable=True)
    lat = Column(Numeric(10, 8), nullable=True)
    lng = Column(Numeric(11, 8), nullable=True)
    rooms = Column(Integer, nullable=True)
    bedrooms = Column(Integer, nullable=True)
    bathrooms = Column(Integer, nullable=True)
    surface_m2 = Column(Numeric(8, 2), nullable=True)
    status = Column(Enum(PropertyStatus), default=PropertyStatus.ACTIVA)
    first_seen_at = Column(DateTime, default=datetime.utcnow)
    last_verified_at = Column(DateTime, default=datetime.utcnow)
    source_count = Column(Integer, default=1)

class PropertyAgentLink(Base):
    __tablename__ = "property_agent_links"

    property_id = Column(Integer, ForeignKey("properties.id"), primary_key=True)
    agent_id = Column(Integer, ForeignKey("agents.id"), primary_key=True)
    source_portal = Column(String(50), nullable=False)
    source_url = Column(Text, nullable=False)
    source_price = Column(Numeric(12, 2), nullable=True)

class PropertyPhoto(Base):
    __tablename__ = "property_photos"

    id = Column(Integer, primary_key=True)
    property_id = Column(Integer, ForeignKey("properties.id"), nullable=False)
    source_url = Column(Text, nullable=False)
    display_order = Column(Integer, nullable=False)  # Máximo 3 fotos por propiedad (1, 2, 3)
    ocr_flagged = Column(Boolean, default=False)
    ocr_flag_reason = Column(Text, nullable=True)


# ==============================================================================
# 2. ESQUEMA PYDANTIC PARA TAGS NORMALIZADOS (LLM Contract)
# ==============================================================================

class PropertyTagsSchema(BaseModel):
    property_type: str = Field(..., description="departamento|casa|ph|terreno|local|oficina")
    antiquity: str = Field(..., description="a_estrenar|menos_5|5_a_15|15_a_30|mas_30")
    amenities: List[str] = Field(default_factory=list, description="Ej: balcon, terraza, parrilla, pileta, etc.")
    cochera_count: int = Field(0, description="Cantidad de cocheras")


# ==============================================================================
# 3. PIPELINE DE PROCESAMIENTO, ANTI-FUGA Y DEDUPLICACIÓN
# ==============================================================================

class PipelineProcessor:
    def __init__(self, db_session: Session):
        self.db = db_session
        
        # Regex base para teléfono argentino (Formatos variados + código de área)
        self.phone_regex = re.compile(
            r'(\+?54)?[\s\-]?9?[\s\-]?\(?\d{2,4}\)?[\s\-]?\d{3,4}[\s\-]?\d{4}',
            re.IGNORECASE
        )
        # Regex para palabras clave de contacto directo
        self.leak_keywords_regex = re.compile(
            r'(wsp|whatsapp|cel|tel|contacto|llamadas|instagram|@[\w.]+)',
            re.IGNORECASE
        )

    def validate_quality_filters(self, listing_data: Dict[str, Any]) -> bool:
        """Etapa 2: Filtra operaciones que no sean venta o tengan fecha de origen > 90 días."""
        if listing_data.get("operation_type") != "venta":
            logger.info(f"[Descarte - Operación no es Venta]: {listing_data.get('source_url')}")
            return False

        published_at = listing_data.get("published_at")
        if published_at and published_at < (datetime.utcnow() - timedelta(days=90)):
            logger.info(f"[Descarte - Antigüedad > 90 días]: {listing_data.get('source_url')}")
            return False

        return True

    def sanitize_text(self, text: str) -> Optional[str]:
        """Etapa 4: Detecta si el texto crudo contiene teléfonos o enlaces de contacto directo."""
        if not text:
            return None
        if self.phone_regex.search(text) or self.leak_keywords_regex.search(text):
            logger.warning("Fuga de datos detectada en descripción. Se excluye el texto libre.")
            return None
        return text

    def check_photo_ocr_leak(self, photo_url: str) -> bool:
        """Etapa 4: Análisis OCR sobre imágenes de la propiedad para prevenir marcas de agua o números."""
        # Integración de OCR (Google Vision / Tesseract fallback)
        # Retorna True si detecta patrones de texto con teléfonos o datos de contacto
        return False

    def resolve_agent(self, raw_phone: str, raw_email: Optional[str], agent_name: str) -> Optional[int]:
        """Etapa 5: Formatea teléfono a E.164, valida contra suppression list y crea/recupera agente."""
        if not raw_phone:
            logger.error("Propiedad sin teléfono de agente. No se puede establecer la identidad canónica.")
            return None

        try:
            parsed = phonenumbers.parse(raw_phone, "AR")
            phone_e164 = phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
        except Exception:
            logger.error(f"Formato de teléfono inválido: {raw_phone}")
            return None

        # 1. Chequeo estricto en AgentSuppressionList (OR explícito, sin trucos de Python)
        suppression_conditions = [AgentSuppressionList.phone_e164 == phone_e164]
        if raw_email:
            suppression_conditions.append(AgentSuppressionList.email == raw_email)

        suppressed = self.db.query(AgentSuppressionList).filter(
            or_(*suppression_conditions)
        ).first()

        if suppressed:
            logger.info(f"[Descarte - Agente en Suppression List]: {phone_e164}")
            return None

        # 2. Obtener o crear agente con estado PENDING
        agent = self.db.query(Agent).filter(Agent.phone_e164 == phone_e164).first()
        if not agent:
            agent = Agent(
                phone_e164=phone_e164,
                email=raw_email,
                name=agent_name,
                account_status=AccountStatus.PENDING,
                subscription_tier=None
            )
            self.db.add(agent)
            self.db.flush()

        return agent.id

    def normalize_string_for_dedup(self, text: str) -> str:
        """Normaliza cadenas de texto para matching exacto/near-exacto de propiedades."""
        return re.sub(r'\W+', '', text.lower())

    def build_safe_title(self, raw_title: str, tags: Dict[str, Any]) -> str:
        """
        Etapa 4 (anti-fuga) aplicada al título: si el título crudo contiene un
        teléfono o palabra clave de contacto directo, se descarta y se reemplaza
        por un título genérico armado a partir de los tags estructurados, en vez
        de publicar el texto crudo sin filtrar.
        """
        sanitized = self.sanitize_text(raw_title)
        if sanitized:
            return sanitized

        logger.warning("Título con fuga de datos detectado — se reemplaza por título genérico.")
        property_type = tags.get("property_type", "propiedad")
        return f"{property_type.capitalize()} en venta"

    def process_and_save(self, listing: Dict[str, Any], tags_dict: Dict[str, Any]):
        """Etapas 6 y 7: Deduplica propiedades, resuelve precio/rango y guarda sin disparar contacto."""
        # 1. Filtros de calidad iniciales
        if not self.validate_quality_filters(listing):
            return

        # 2. Validar estructura de tags del LLM contra Pydantic Schema
        try:
            validated_tags = PropertyTagsSchema(**tags_dict).model_dump()
        except Exception as e:
            logger.error(f"Error de validación de JSON Schema en tags: {e}")
            return

        # 3. Resolver Agente
        agent_id = self.resolve_agent(
            raw_phone=listing.get("agent_phone"),
            raw_email=listing.get("agent_email"),
            agent_name=listing.get("agent_name", "")
        )
        if not agent_id:
            return

        new_price = listing.get("price_amount")
        safe_title = self.build_safe_title(listing["title"], validated_tags)
        normalized_new_title = self.normalize_string_for_dedup(safe_title)

        # 4. Deduplicación: matching por título normalizado dentro de la misma zona
        # (criterio definido en la documentación — NO por atributos numéricos como
        # ambientes/superficie, que pueden coincidir por casualidad entre propiedades
        # distintas y generar fusiones incorrectas).
        existing_property = None
        candidates = self.db.query(Property).filter(
            Property.zone_id == listing.get("zone_id")
        ).all()
        for candidate in candidates:
            if self.normalize_string_for_dedup(candidate.canonical_title) == normalized_new_title:
                existing_property = candidate
                break

        if existing_property:
            logger.info(f"Deduplicación: Fusión con propiedad existente ID #{existing_property.id}")
            existing_property.source_count += 1
            existing_property.last_verified_at = datetime.utcnow()

            # Ajuste dinámico a Modo RANGO de Precio si difieren las publicaciones
            if new_price and existing_property.price_amount:
                prices = [float(existing_property.price_amount), float(new_price)]
                if existing_property.price_max_amount:
                    prices.append(float(existing_property.price_max_amount))

                min_price, max_price = min(prices), max(prices)
                if min_price != max_price:
                    existing_property.price_amount = min_price
                    existing_property.price_max_amount = max_price
                    existing_property.price_display_mode = PriceDisplayMode.RANGO

            prop_id = existing_property.id
        else:
            # Creación de nueva propiedad
            new_prop = Property(
                canonical_title=safe_title,
                canonical_description_tags=validated_tags,
                operation_type="venta",
                price_amount=new_price,
                price_currency=listing.get("price_currency", "USD"),
                # Si no viene precio, la propiedad se publica como "Consultar",
                # no como precio fijo vacío.
                price_display_mode=(
                    PriceDisplayMode.FIJO if new_price is not None else PriceDisplayMode.CONSULTAR
                ),
                zone_id=listing.get("zone_id"),
                address_raw=listing.get("address_raw"),
                lat=listing.get("lat"),
                lng=listing.get("lng"),
                rooms=listing.get("rooms"),
                bedrooms=listing.get("bedrooms"),
                bathrooms=listing.get("bathrooms"),
                surface_m2=listing.get("surface_m2"),
                status=PropertyStatus.ACTIVA
            )
            self.db.add(new_prop)
            self.db.flush()
            prop_id = new_prop.id

        # 5. Registrar / Vincular Agente a la Propiedad
        link = self.db.query(PropertyAgentLink).filter_by(
            property_id=prop_id, agent_id=agent_id
        ).first()

        if not link:
            new_link = PropertyAgentLink(
                property_id=prop_id,
                agent_id=agent_id,
                source_portal=listing["source_portal"],
                source_url=listing["source_url"],
                source_price=new_price
            )
            self.db.add(new_link)

        # 6. Guardar máximo 3 Fotos y aplicar flag OCR si corresponde
        photos = listing.get("photos", [])[:3]
        for idx, photo_url in enumerate(photos, start=1):
            is_flagged = self.check_photo_ocr_leak(photo_url)
            photo_record = PropertyPhoto(
                property_id=prop_id,
                source_url=photo_url,
                display_order=idx,
                ocr_flagged=is_flagged,
                ocr_flag_reason="Patrón de teléfono/contacto detectado en OCR" if is_flagged else None
            )
            self.db.add(photo_record)

        # 7. Guardar en Base de Datos (PASIVO - Sin envíos de emails/notificaciones)
        self.db.commit()
        logger.info(f"Propiedad #{prop_id} procesada e insertada de forma pasiva.")


# ==============================================================================
# 4. DEMO / PRUEBA DE EJECUCIÓN SÍNCRONA
# ==============================================================================

if __name__ == "__main__":
    # Base de datos SQLite en memoria para la prueba
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(engine)
    SessionFactory = sessionmaker(bind=engine)
    session = SessionFactory()

    # Cargar zona de prueba en catálogo
    zona_demo = ZonesCatalog(
        province="Santa Fe",
        city="Santa Fe",
        neighborhood="Centro",
        aliases=["Santa Fe Centro", "Zona Centro"]
    )
    session.add(zona_demo)
    session.commit()

    # Pipeline
    pipeline = PipelineProcessor(db_session=session)

    # Objeto simulado extraído por scraper
    listing_ejemplo = {
        "source_portal": "ZonaProp",
        "source_url": "https://www.zonaprop.com.ar/propiedades/ejemplo-1234.html",
        "operation_type": "venta",
        "title": "Departamento 2 Ambientes con Balcón",
        "published_at": datetime.utcnow() - timedelta(days=5),
        "agent_phone": "+54 9 342 412 3456",
        "agent_email": "contacto@inmobiliariaejemplo.com",
        "agent_name": "Agencia Santa Fe Real Estate",
        "price_amount": 75000.00,
        "price_currency": "USD",
        "zone_id": zona_demo.id,
        "rooms": 2,
        "bedrooms": 1,
        "bathrooms": 1,
        "surface_m2": 48.0,
        "photos": [
            "https://img.portal.com/foto1.jpg",
            "https://img.portal.com/foto2.jpg",
            "https://img.portal.com/foto3.jpg",
            "https://img.portal.com/foto4_excedente.jpg"  # Será ignorada (máximo 3)
        ]
    }

    # Tags parseados por el LLM (normalización)
    tags_ejemplo = {
        "property_type": "departamento",
        "antiquity": "menos_5",
        "amenities": ["balcon", "apto_credito"],
        "cochera_count": 0
    }

    logger.info("--- Ejecutando ciclo de prueba del Crawler Proferta ---")
    pipeline.process_and_save(listing_ejemplo, tags_ejemplo)

    # Verificación de resultados en DB
    total_props = session.query(Property).count()
    total_agents = session.query(Agent).count()
    total_photos = session.query(PropertyPhoto).count()

    print("\n================ RESULTADOS DE LA PRUEBA ================")
    print(f"Propiedades registradas: {total_props}")
    print(f"Agentes creados (Pendientes): {total_agents}")
    print(f"Fotos asociadas (Máx 3): {total_photos}")
    print(f"Umbral 'Búsquedas particulares': USD {BUSQUEDAS_PARTICULARES_MIN_TIER_USD}/mes")
    print("========================================================\n")
