"""
proferta_scraper.py

Módulo de scraping real (Etapas 1-2 del pipeline: Descubrimiento + Extracción).
Complementa proferta_crawler.py (que cubre las Etapas 3-8: normalización,
anti-fuga, identidad de agente, deduplicación, publicación).

Zona piloto: Caballito, CABA.
Fuentes: ZonaProp y Argenprop.

IMPORTANTE — leer antes de correr contra los sitios reales:
Los selectores CSS de este archivo (marcados con el comentario "AJUSTAR SI CAMBIA
EL SITIO") son una estimación basada en patrones típicos de portales inmobiliarios,
no fueron verificados contra el HTML en vivo de ZonaProp/Argenprop al momento de
escribir este código. Los portales cambian de estructura con frecuencia. Antes de
la primera corrida real:
  1. Descargar manualmente 2-3 fichas de ejemplo de cada portal.
  2. Correr `test_parsers_con_html_local()` al final de este archivo contra esas
     fichas guardadas localmente.
  3. Ajustar los selectores marcados hasta que el test extraiga los campos
     correctamente.
Este diseño aísla todos los selectores dentro de cada clase *Parser, así que
ajustarlos no requiere tocar el resto del pipeline.
"""

import logging
import re
import time
import urllib.robotparser
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger("proferta.scraper")
logging.basicConfig(level=logging.INFO)

# --------------------------------------------------------------------------
# Configuración de la corrida piloto (ver doc 14 del paquete de documentación)
# --------------------------------------------------------------------------
ZONA_PILOTO = "Caballito, CABA"
MAX_ANTIGUEDAD_DIAS = 90
DELAY_ENTRE_REQUESTS_SEGUNDOS = 4.0
USER_AGENT = "ProfertaBot/0.1 (+https://proferta.example/bot-info)"
TIMEOUT_SEGUNDOS = 15
MAX_PAGINAS_POR_FUENTE = 50  # límite de seguridad para no correr indefinidamente


@dataclass
class RawListingData:
    """
    Forma de salida esperada por PipelineProcessor.process_and_save()
    en proferta_crawler.py. Mantener estos nombres de campo sincronizados
    con ese archivo si se modifica alguno de los dos.
    """
    source_portal: str
    source_url: str
    title: str
    description_raw: str
    operation_type: str  # debe ser "venta" — cualquier otro valor se descarta antes de llegar acá
    price_amount: Optional[float]
    price_currency: Optional[str]
    address_raw: str
    zone_raw: str  # texto crudo del barrio/zona tal como aparece en la fuente
    rooms: Optional[int]
    bedrooms: Optional[int]
    bathrooms: Optional[int]
    surface_m2: Optional[float]
    published_at: Optional[datetime]
    photo_urls: List[str] = field(default_factory=list)
    agent_phone_raw: Optional[str] = None
    agent_email_raw: Optional[str] = None
    agent_name_raw: Optional[str] = None


class RobotsChecker:
    """Cachea y consulta robots.txt por dominio antes de scrapear una URL."""

    def __init__(self) -> None:
        self._parsers: Dict[str, urllib.robotparser.RobotFileParser] = {}

    def puede_scrapear(self, url: str, user_agent: str = USER_AGENT) -> bool:
        parsed = urlparse(url)
        domain = f"{parsed.scheme}://{parsed.netloc}"
        if domain not in self._parsers:
            rp = urllib.robotparser.RobotFileParser()
            rp.set_url(urljoin(domain, "/robots.txt"))
            try:
                rp.read()
            except Exception as exc:
                logger.warning("No se pudo leer robots.txt de %s (%s) — se asume permitido con precaución.", domain, exc)
                self._parsers[domain] = None
                return True
            self._parsers[domain] = rp
        parser = self._parsers[domain]
        if parser is None:
            return True
        return parser.can_fetch(user_agent, url)


class RateLimiter:
    """Espera un delay fijo entre requests salientes (sin proxies en la prueba piloto)."""

    def __init__(self, delay_segundos: float = DELAY_ENTRE_REQUESTS_SEGUNDOS) -> None:
        self.delay = delay_segundos
        self._last_request_ts: Optional[float] = None

    def esperar_turno(self) -> None:
        if self._last_request_ts is not None:
            elapsed = time.monotonic() - self._last_request_ts
            faltante = self.delay - elapsed
            if faltante > 0:
                time.sleep(faltante)
        self._last_request_ts = time.monotonic()


class HttpClient:
    """Wrapper fino sobre httpx con rate limiting y manejo de errores uniforme."""

    def __init__(self, rate_limiter: RateLimiter, robots: RobotsChecker) -> None:
        self.rate_limiter = rate_limiter
        self.robots = robots
        self.client = httpx.Client(
            headers={"User-Agent": USER_AGENT},
            timeout=TIMEOUT_SEGUNDOS,
            follow_redirects=True,
        )

    def get(self, url: str) -> Optional[str]:
        if not self.robots.puede_scrapear(url):
            logger.warning("robots.txt bloquea %s — se omite.", url)
            return None
        self.rate_limiter.esperar_turno()
        try:
            resp = self.client.get(url)
            resp.raise_for_status()
            return resp.text
        except httpx.HTTPStatusError as exc:
            logger.error("HTTP %s al pedir %s", exc.response.status_code, url)
        except httpx.RequestError as exc:
            logger.error("Error de red pidiendo %s: %s", url, exc)
        return None

    def close(self) -> None:
        self.client.close()


# --------------------------------------------------------------------------
# Interfaz común de parser por fuente
# --------------------------------------------------------------------------
class SourceParser(ABC):
    """Cada fuente (ZonaProp, Argenprop, futuras) implementa esta interfaz.
    Agregar una fuente nueva = una clase nueva, sin tocar el resto del pipeline."""

    source_name: str

    def __init__(self, http_client: HttpClient) -> None:
        self.http = http_client

    @abstractmethod
    def discover_urls(self, zona: str, max_paginas: int = MAX_PAGINAS_POR_FUENTE) -> List[str]:
        """Devuelve una lista de URLs de fichas individuales de propiedades en venta."""
        raise NotImplementedError

    @abstractmethod
    def extract_listing(self, html: str, url: str) -> Optional[RawListingData]:
        """Parsea el HTML de una ficha y devuelve los datos crudos, o None si falla."""
        raise NotImplementedError

    # -- utilidades compartidas entre parsers --------------------------------
    @staticmethod
    def _texto_limpio(el) -> str:
        return el.get_text(strip=True) if el else ""

    @staticmethod
    def _parsear_precio(texto_precio: str) -> tuple[Optional[float], Optional[str]]:
        """'USD 130.000' -> (130000.0, 'USD'). Devuelve (None, None) si no hay precio
        (caso 'Consultar precio')."""
        if not texto_precio:
            return None, None
        texto_precio = texto_precio.strip()
        if re.search(r"consultar", texto_precio, re.IGNORECASE):
            return None, None
        moneda = "USD" if "USD" in texto_precio.upper() or "U$S" in texto_precio.upper() else "ARS"
        numero = re.sub(r"[^\d]", "", texto_precio)
        if not numero:
            return None, None
        return float(numero), moneda

    @staticmethod
    def _es_venta(texto_operacion: str) -> bool:
        """Filtro de calidad: solo aceptar operaciones explícitamente de venta.
        Ante ambigüedad, devuelve False (se descarta la ficha) — más seguro
        perder un caso válido que publicar un alquiler mal clasificado."""
        if not texto_operacion:
            return False
        texto = texto_operacion.strip().lower()
        return texto == "venta"

    @staticmethod
    def _dentro_de_antiguedad(fecha_publicacion: Optional[datetime], max_dias: int = MAX_ANTIGUEDAD_DIAS) -> bool:
        if fecha_publicacion is None:
            # Sin fecha detectable: se acepta con precaución en el MVP, mejor
            # revisar manualmente que descartar todo lo que no tenga fecha visible.
            logger.info("Ficha sin fecha de publicación detectable — se acepta con precaución.")
            return True
        return (datetime.now() - fecha_publicacion) <= timedelta(days=max_dias)


class ZonaPropParser(SourceParser):
    source_name = "zonaprop"
    BASE_URL = "https://www.zonaprop.com.ar"

    def discover_urls(self, zona: str, max_paginas: int = MAX_PAGINAS_POR_FUENTE) -> List[str]:
        urls: List[str] = []
        slug_zona = zona.split(",")[0].strip().lower().replace(" ", "-")
        for pagina in range(1, max_paginas + 1):
            listado_url = f"{self.BASE_URL}/departamentos-venta-{slug_zona}-pagina-{pagina}.html"
            html = self.http.get(listado_url)
            if not html:
                break
            soup = BeautifulSoup(html, "html.parser")
            # AJUSTAR SI CAMBIA EL SITIO: selector de tarjetas de resultado
            links = soup.select("a[data-qa='POSTING_CARD_LINK']") or soup.select("div.posting-card a")
            if not links:
                logger.info("Sin más resultados en página %s de ZonaProp — fin de paginación.", pagina)
                break
            for link in links:
                href = link.get("href")
                if href:
                    urls.append(urljoin(self.BASE_URL, href))
        return list(dict.fromkeys(urls))  # dedupe preservando orden

    def extract_listing(self, html: str, url: str) -> Optional[RawListingData]:
        soup = BeautifulSoup(html, "html.parser")
        try:
            # AJUSTAR SI CAMBIA EL SITIO: cada selector de esta sección
            titulo = self._texto_limpio(soup.select_one("h1"))
            descripcion = self._texto_limpio(
                soup.select_one("[data-qa='POSTING_DESCRIPTION']") or soup.select_one("#description")
            )
            texto_operacion = self._texto_limpio(soup.select_one("[data-qa='OPERATION_TYPE']"))
            if not self._es_venta(texto_operacion):
                logger.info("Descartada (no es venta): %s", url)
                return None

            precio_texto = self._texto_limpio(soup.select_one("[data-qa='POSTING_CARD_PRICE'], .price"))
            precio_monto, precio_moneda = self._parsear_precio(precio_texto)

            direccion = self._texto_limpio(soup.select_one("[data-qa='POSTING_LOCATION']"))
            zona_texto = direccion.split("-")[-1].strip() if direccion else ZONA_PILOTO

            ambientes = self._extraer_entero(soup, "AMBIENTES")
            dormitorios = self._extraer_entero(soup, "DORMITORIOS")
            banos = self._extraer_entero(soup, "BAÑOS")
            superficie = self._extraer_superficie(soup)

            fotos = [img.get("src") or img.get("data-src") for img in soup.select("img.gallery-image, [data-qa='PHOTO'] img")]
            fotos = [f for f in fotos if f][:10]  # tope generoso acá; el truncado final a 3 lo hace el pipeline

            telefono, email, nombre_agente = self._extraer_contacto(soup)

            fecha_pub = self._extraer_fecha(soup)
            if not self._dentro_de_antiguedad(fecha_pub):
                logger.info("Descartada (más de %s días): %s", MAX_ANTIGUEDAD_DIAS, url)
                return None

            if not titulo or precio_monto is None and precio_texto and "consultar" not in precio_texto.lower():
                # precio_monto None sin ser "Consultar" explícito sugiere un fallo de parseo, no un dato real
                logger.warning("Parseo posiblemente incompleto en %s — revisar selectores.", url)

            return RawListingData(
                source_portal=self.source_name,
                source_url=url,
                title=titulo,
                description_raw=descripcion,
                operation_type="venta",
                price_amount=precio_monto,
                price_currency=precio_moneda,
                address_raw=direccion,
                zone_raw=zona_texto,
                rooms=ambientes,
                bedrooms=dormitorios,
                bathrooms=banos,
                surface_m2=superficie,
                published_at=fecha_pub,
                photo_urls=fotos,
                agent_phone_raw=telefono,
                agent_email_raw=email,
                agent_name_raw=nombre_agente,
            )
        except Exception as exc:
            logger.error("Fallo parseando %s: %s", url, exc)
            return None

    # -- helpers específicos de ZonaProp (AJUSTAR SI CAMBIA EL SITIO) --------
    def _extraer_entero(self, soup: BeautifulSoup, etiqueta: str) -> Optional[int]:
        el = soup.find(string=re.compile(etiqueta, re.IGNORECASE))
        if not el:
            return None
        match = re.search(r"\d+", str(el))
        return int(match.group()) if match else None

    def _extraer_superficie(self, soup: BeautifulSoup) -> Optional[float]:
        el = soup.select_one("[data-qa='POSTING_FEATURES_SURFACE']")
        if not el:
            return None
        match = re.search(r"[\d.,]+", self._texto_limpio(el))
        return float(match.group().replace(".", "").replace(",", ".")) if match else None

    def _extraer_contacto(self, soup: BeautifulSoup) -> tuple[Optional[str], Optional[str], Optional[str]]:
        telefono_el = soup.select_one("[data-qa='CONTACT_PHONE'], a[href^='tel:']")
        email_el = soup.select_one("a[href^='mailto:']")
        nombre_el = soup.select_one("[data-qa='PUBLISHER_NAME']")
        telefono = None
        if telefono_el:
            href = telefono_el.get("href", "")
            telefono = href.replace("tel:", "") if href.startswith("tel:") else self._texto_limpio(telefono_el)
        email = email_el.get("href", "").replace("mailto:", "") if email_el else None
        nombre = self._texto_limpio(nombre_el) if nombre_el else None
        return telefono, email, nombre

    def _extraer_fecha(self, soup: BeautifulSoup) -> Optional[datetime]:
        el = soup.select_one("[data-qa='POSTING_PUBLISH_DATE']")
        if not el:
            return None
        # Los portales suelen mostrar fechas relativas ("Publicado hace 5 días")
        texto = self._texto_limpio(el)
        match = re.search(r"(\d+)\s*d[ií]a", texto, re.IGNORECASE)
        if match:
            return datetime.now() - timedelta(days=int(match.group(1)))
        return None


class ArgenpropParser(SourceParser):
    source_name = "argenprop"
    BASE_URL = "https://www.argenprop.com"

    def discover_urls(self, zona: str, max_paginas: int = MAX_PAGINAS_POR_FUENTE) -> List[str]:
        urls: List[str] = []
        slug_zona = zona.split(",")[0].strip().lower().replace(" ", "-")
        for pagina in range(1, max_paginas + 1):
            listado_url = f"{self.BASE_URL}/departamentos/venta/{slug_zona}/pagina-{pagina}"
            html = self.http.get(listado_url)
            if not html:
                break
            soup = BeautifulSoup(html, "html.parser")
            # AJUSTAR SI CAMBIA EL SITIO
            links = soup.select("a.card__wrapper-link") or soup.select("div.listing__item a")
            if not links:
                logger.info("Sin más resultados en página %s de Argenprop — fin de paginación.", pagina)
                break
            for link in links:
                href = link.get("href")
                if href:
                    urls.append(urljoin(self.BASE_URL, href))
        return list(dict.fromkeys(urls))

    def extract_listing(self, html: str, url: str) -> Optional[RawListingData]:
        soup = BeautifulSoup(html, "html.parser")
        try:
            # AJUSTAR SI CAMBIA EL SITIO: misma lógica que ZonaProp, selectores propios
            titulo = self._texto_limpio(soup.select_one("h1.titlebar__title, h1"))
            descripcion = self._texto_limpio(soup.select_one(".description__content, #description"))
            texto_operacion = self._texto_limpio(soup.select_one(".titlebar__operation, [data-tipo-operacion]"))
            if not self._es_venta(texto_operacion):
                logger.info("Descartada (no es venta): %s", url)
                return None

            precio_texto = self._texto_limpio(soup.select_one(".titlebar__price, .price"))
            precio_monto, precio_moneda = self._parsear_precio(precio_texto)

            direccion = self._texto_limpio(soup.select_one(".titlebar__address, .location"))
            zona_texto = direccion.split(",")[-1].strip() if direccion else ZONA_PILOTO

            ambientes = self._extraer_de_features(soup, "ambiente")
            dormitorios = self._extraer_de_features(soup, "dormitorio")
            banos = self._extraer_de_features(soup, "baño")
            superficie = self._extraer_superficie(soup)

            fotos = [img.get("src") or img.get("data-src") for img in soup.select(".gallery img, .property-gallery img")]
            fotos = [f for f in fotos if f][:10]

            telefono, email, nombre_agente = self._extraer_contacto(soup)
            fecha_pub = self._extraer_fecha(soup)
            if not self._dentro_de_antiguedad(fecha_pub):
                logger.info("Descartada (más de %s días): %s", MAX_ANTIGUEDAD_DIAS, url)
                return None

            return RawListingData(
                source_portal=self.source_name,
                source_url=url,
                title=titulo,
                description_raw=descripcion,
                operation_type="venta",
                price_amount=precio_monto,
                price_currency=precio_moneda,
                address_raw=direccion,
                zone_raw=zona_texto,
                rooms=ambientes,
                bedrooms=dormitorios,
                bathrooms=banos,
                surface_m2=superficie,
                published_at=fecha_pub,
                photo_urls=fotos,
                agent_phone_raw=telefono,
                agent_email_raw=email,
                agent_name_raw=nombre_agente,
            )
        except Exception as exc:
            logger.error("Fallo parseando %s: %s", url, exc)
            return None

    def _extraer_de_features(self, soup: BeautifulSoup, palabra_clave: str) -> Optional[int]:
        for li in soup.select(".property-features li, .features__item"):
            texto = self._texto_limpio(li)
            if palabra_clave.lower() in texto.lower():
                match = re.search(r"\d+", texto)
                return int(match.group()) if match else None
        return None

    def _extraer_superficie(self, soup: BeautifulSoup) -> Optional[float]:
        for li in soup.select(".property-features li, .features__item"):
            texto = self._texto_limpio(li)
            if "m²" in texto or "m2" in texto.lower():
                match = re.search(r"[\d.,]+", texto)
                if match:
                    return float(match.group().replace(".", "").replace(",", "."))
        return None

    def _extraer_contacto(self, soup: BeautifulSoup) -> tuple[Optional[str], Optional[str], Optional[str]]:
        telefono_el = soup.select_one("a[href^='tel:']")
        email_el = soup.select_one("a[href^='mailto:']")
        nombre_el = soup.select_one(".publisher-info__name, .agent-name")
        telefono = telefono_el.get("href", "").replace("tel:", "") if telefono_el else None
        email = email_el.get("href", "").replace("mailto:", "") if email_el else None
        nombre = self._texto_limpio(nombre_el) if nombre_el else None
        return telefono, email, nombre

    def _extraer_fecha(self, soup: BeautifulSoup) -> Optional[datetime]:
        el = soup.select_one(".titlebar__date, .publish-date")
        if not el:
            return None
        texto = self._texto_limpio(el)
        match = re.search(r"(\d+)\s*d[ií]a", texto, re.IGNORECASE)
        if match:
            return datetime.now() - timedelta(days=int(match.group(1)))
        return None


# --------------------------------------------------------------------------
# Orquestador: corre Etapas 1-2 sobre todas las fuentes configuradas
# --------------------------------------------------------------------------
class ScraperOrchestrator:
    def __init__(self, zona: str = ZONA_PILOTO) -> None:
        self.zona = zona
        self.robots = RobotsChecker()
        self.rate_limiter = RateLimiter()
        self.http = HttpClient(self.rate_limiter, self.robots)
        self.parsers: List[SourceParser] = [
            ZonaPropParser(self.http),
            ArgenpropParser(self.http),
        ]

    def correr(self) -> List[RawListingData]:
        """Devuelve la lista de RawListingData lista para pasar a
        PipelineProcessor.process_and_save() (Etapas 3-8, en proferta_crawler.py)."""
        resultados: List[RawListingData] = []
        for parser in self.parsers:
            logger.info("=== Descubriendo URLs en %s para zona '%s' ===", parser.source_name, self.zona)
            try:
                urls = parser.discover_urls(self.zona)
            except Exception as exc:
                logger.error("Fallo en discover_urls de %s: %s", parser.source_name, exc)
                continue
            logger.info("%s URLs encontradas en %s", len(urls), parser.source_name)

            for url in urls:
                html = self.http.get(url)
                if not html:
                    continue
                listing = parser.extract_listing(html, url)
                if listing:
                    resultados.append(listing)
        self.http.close()
        logger.info("Total de fichas válidas extraídas: %s", len(resultados))
        return resultados

    def to_pipeline_dict(self, listing: RawListingData) -> Dict[str, Any]:
        """Convierte RawListingData al formato de diccionario que espera
        PipelineProcessor.process_and_save() en proferta_crawler.py."""
        return {
            "source_portal": listing.source_portal,
            "source_url": listing.source_url,
            "title": listing.title,
            "description_raw": listing.description_raw,
            "operation_type": listing.operation_type,
            "price_amount": listing.price_amount,
            "price_currency": listing.price_currency,
            "zone_id": None,  # resolver contra zones_catalog antes/durante Etapa 3 (doc 13)
            "zone_raw": listing.zone_raw,
            "address_raw": listing.address_raw,
            "rooms": listing.rooms,
            "bedrooms": listing.bedrooms,
            "bathrooms": listing.bathrooms,
            "surface_m2": listing.surface_m2,
            "published_at": listing.published_at,
            "photos": listing.photo_urls[:3],  # truncado final a 3 (doble seguro, el pipeline también lo hace)
            "agent_phone": listing.agent_phone_raw,
            "agent_email": listing.agent_email_raw,
            "agent_name": listing.agent_name_raw,
        }


# --------------------------------------------------------------------------
# Test con HTML local — usar esto para validar/ajustar selectores sin
# pegarle a los portales reales. Ver instrucciones al inicio del archivo.
# --------------------------------------------------------------------------
def test_parsers_con_html_local() -> None:
    """
    Ejemplo de cómo validar un parser contra una ficha guardada localmente,
    sin hacer requests reales. Reemplazar el HTML de ejemplo por una ficha
    real descargada manualmente antes de la primera corrida en serio.
    """
    html_ejemplo = """
    <html>
      <h1>Departamento 2 ambientes en Caballito</h1>
      <div data-qa="POSTING_DESCRIPTION">Luminoso, a metros del subte.</div>
      <div data-qa="OPERATION_TYPE">Venta</div>
      <div data-qa="POSTING_CARD_PRICE">USD 130.000</div>
      <div data-qa="POSTING_LOCATION">Av. Rivadavia 5000 - Caballito</div>
      <div data-qa="POSTING_PUBLISH_DATE">Publicado hace 10 días</div>
      <a href="tel:+541155551234">Contacto</a>
    </html>
    """
    fake_http = HttpClient(RateLimiter(delay_segundos=0), RobotsChecker())
    parser = ZonaPropParser(fake_http)
    resultado = parser.extract_listing(html_ejemplo, "https://www.zonaprop.com.ar/ejemplo.html")
    fake_http.close()

    assert resultado is not None, "El parser no debería devolver None con este HTML de ejemplo"
    assert resultado.operation_type == "venta"
    assert resultado.price_amount == 130000.0
    assert resultado.price_currency == "USD"
    print("OK — test_parsers_con_html_local pasó correctamente.")
    print(resultado)


if __name__ == "__main__":
    logger.info("Corriendo test local de parsers (sin requests reales)...")
    test_parsers_con_html_local()

    logger.info(
        "Para correr el scraper real contra ZonaProp/Argenprop, descomentar "
        "las líneas siguientes DESPUÉS de validar/ajustar los selectores "
        "contra HTML real (ver instrucciones al inicio del archivo)."
    )
    # orchestrator = ScraperOrchestrator(zona=ZONA_PILOTO)
    # listings = orchestrator.correr()
    # for listing in listings:
    #     pipeline_dict = orchestrator.to_pipeline_dict(listing)
    #     # pipeline.process_and_save(pipeline_dict, tags_dict)  # ver proferta_crawler.py
