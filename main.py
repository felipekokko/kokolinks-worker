"""
Felipe Brasil — KokoLinks Media
Worker FastAPI: 4 flujos de link-building + API para CRM web

Flujos:
  WF1 /wf1/trigger  — Primer email al medio (W=yes|new → W=Sent)
  WF3 /wf3/trigger  — Enviar artículo al cliente (AB=Checking by Client)
  WF4 /wf4/trigger  — Email al medio para publicar (AC=Approved → AC=Enviado)
  WF2 /wf2/trigger  — Actualizar live link en hoja cliente (AF=URL + AJ=Approved)

API CRM:
  GET /api/prices?domain=xxx  — comparar precios entre proveedores
  GET /api/medios              — listado paginado de dominios Bazoom
  GET /status                  — resumen de campañas activas
  GET /health
"""

import os
import logging
import smtplib
import json
import re
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException, Header, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "info").upper())
logger = logging.getLogger("kokolinks")

# ─── Config ──────────────────────────────────────────────────────────────────

NOCODB_URL   = os.getenv("NOCODB_URL",   "http://nocodb-xyd190woraek9xbik7c8heq4.86.48.2.187.sslip.io")
NOCODB_TOKEN = os.getenv("NOCODB_TOKEN", "hhftPqJQkyoE-8oBu5UYVgYAVHmOHFduQ7qSeR9h")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

# IDs de tablas NocoDB (Prices Comparison)
TABLES = {
    "bazoom":          os.getenv("NOCODB_TABLE_BAZOOM",         "mbdpznnm0mxolaj"),
    "leolytics":       os.getenv("NOCODB_TABLE_LEOLYTICS",      "mwx5usficvhvqz3"),
    "whitepress":      os.getenv("NOCODB_TABLE_WHITEPRESS",     "me6qowoq4qaetdn"),
    "backlinksglobal": os.getenv("NOCODB_TABLE_BACKLINKS",      "m01r3pqfw2hrrfa"),
    "meup":            os.getenv("NOCODB_TABLE_MEUP",           "m3kqbqqloknqpdn"),
    "price_lists":     os.getenv("NOCODB_TABLE_PRICE_LISTS",    "mafhrxq41mgl7ly"),
    "linkplans":       os.getenv("NOCODB_TABLE_LINKPLANS",      "mxsx2sfxzeq3hbq"),  # ProjectA
}

SMTP_HOST    = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT    = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER    = os.getenv("SMTP_USER", "kokolinkstools@gmail.com")
SMTP_PASS    = os.getenv("SMTP_PASS", "")
GMAIL_SENDER = os.getenv("GMAIL_SENDER", SMTP_USER)

NOCODB_HEADERS = {
    "xc-token":     NOCODB_TOKEN,
    "Content-Type": "application/json",
}

# ─── App ─────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="KokoLinks Worker",
    description="Worker de automatizaciones de link-building para Felipe Brasil",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Auth ─────────────────────────────────────────────────────────────────────

def verify_webhook(x_webhook_secret: Optional[str] = Header(None)):
    if WEBHOOK_SECRET and x_webhook_secret != WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return True

# ─── Modelos ──────────────────────────────────────────────────────────────────

class WF1Request(BaseModel):
    linkplan_table_id: str  # ID de la tabla Airtable que representa el linkplan (hoja interna)
    dry_run: bool = False   # True = solo listar filas que se procesarían, sin enviar

class WF3Request(BaseModel):
    linkplan_table_id: str
    dry_run: bool = False

class WF4Request(BaseModel):
    linkplan_table_id: str
    dry_run: bool = False

class WF2Request(BaseModel):
    linkplan_table_id: str
    client_sheet_table_id: str  # tabla Airtable que representa la hoja cliente
    dry_run: bool = False

# ─── SMTP helper ──────────────────────────────────────────────────────────────

def send_email(to: str, subject: str, body_html: str) -> dict:
    """Envía un email vía SMTP y devuelve {'message_id': str, 'gmail_url': str}."""
    if not SMTP_PASS:
        raise HTTPException(
            status_code=503,
            detail="SMTP no configurado. Agregá SMTP_USER y SMTP_PASS al .env"
        )
    msg = MIMEMultipart("alternative")
    msg["From"] = GMAIL_SENDER
    msg["To"] = to
    msg["Subject"] = subject
    msg.attach(MIMEText(body_html, "html"))

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.ehlo()
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(GMAIL_SENDER, [to], msg.as_string())

    import uuid
    msg_id = str(uuid.uuid4()).replace("-", "")
    return {
        "message_id": msg_id,
        "thread_id":  "",
        "gmail_url":  f"https://mail.google.com/mail/u/0/#sent",
    }

# ─── NocoDB helpers ───────────────────────────────────────────────────────────

def _noco_url(table_id: str) -> str:
    return f"{NOCODB_URL}/api/v2/tables/{table_id}/records"


async def nocodb_list(table_id: str, where: str = "", fields: list[str] = None) -> list[dict]:
    """Lista todos los registros de una tabla NocoDB con paginación automática."""
    params: dict = {"limit": 100, "offset": 0}
    if where:
        params["where"] = where
    if fields:
        params["fields"] = ",".join(fields)

    records = []
    async with httpx.AsyncClient(timeout=30) as client:
        while True:
            resp = await client.get(_noco_url(table_id), headers=NOCODB_HEADERS, params=params)
            if resp.status_code != 200:
                logger.error(f"NocoDB list error {resp.status_code}: {resp.text}")
                raise HTTPException(status_code=502, detail=f"NocoDB error: {resp.text}")
            data = resp.json()
            batch = data.get("list", [])
            records.extend(batch)
            page_info = data.get("pageInfo", {})
            if page_info.get("isLastPage", True) or not batch:
                break
            params["offset"] += 100

    # Normalizar al formato {id, fields} que usa el resto del código
    return [{"id": str(r.get("Id", r.get("id", ""))), "fields": r} for r in records]


async def nocodb_update(table_id: str, record_id: str, fields: dict) -> dict:
    """Actualiza un registro en NocoDB (PATCH)."""
    url = f"{NOCODB_URL}/api/v2/tables/{table_id}/records"
    payload = {"Id": int(record_id) if record_id.isdigit() else record_id}
    payload.update(fields)
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.patch(url, headers=NOCODB_HEADERS, json=payload)
        if resp.status_code not in (200, 201):
            logger.error(f"NocoDB update error {resp.status_code}: {resp.text}")
            raise HTTPException(status_code=502, detail=f"NocoDB update error: {resp.text}")
        return {"id": str(record_id), "fields": resp.json()}


async def nocodb_get(table_id: str, record_id: str) -> dict:
    """Obtiene un registro por ID."""
    url = f"{NOCODB_URL}/api/v2/tables/{table_id}/records/{record_id}"
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(url, headers=NOCODB_HEADERS)
        if resp.status_code == 404:
            raise HTTPException(status_code=404, detail="Registro no encontrado")
        if resp.status_code != 200:
            raise HTTPException(status_code=502, detail=f"NocoDB error: {resp.text}")
        r = resp.json()
        return {"id": str(r.get("Id", record_id)), "fields": r}


async def nocodb_create(table_id: str, fields: dict) -> dict:
    """Crea un registro en NocoDB."""
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(_noco_url(table_id), headers=NOCODB_HEADERS, json=fields)
        if resp.status_code not in (200, 201):
            raise HTTPException(status_code=502, detail=f"NocoDB error: {resp.text}")
        r = resp.json()
        return {"id": str(r.get("Id", "")), "fields": r}


async def nocodb_list_page(table_id: str, where: str = "", fields: list[str] = None,
                           limit: int = 100, offset: int = 0) -> dict:
    """Una sola página de registros (para /api/medios)."""
    params: dict = {"limit": limit, "offset": offset}
    if where:
        params["where"] = where
    if fields:
        params["fields"] = ",".join(fields)
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(_noco_url(table_id), headers=NOCODB_HEADERS, params=params)
        if resp.status_code != 200:
            raise HTTPException(status_code=502, detail=f"NocoDB error: {resp.text}")
        return resp.json()


# Alias para compatibilidad con el código de WFs
airtable_list   = nocodb_list
airtable_update = nocodb_update

# ─── Templates de email ───────────────────────────────────────────────────────

def template_wf1_deal(row: dict) -> tuple[str, str]:
    """WF1 rama 'Ya tenemos deal' (W=yes). Devuelve (subject, body_html)."""
    domain = row.get("Domain", row.get("Website", ""))
    subject = f"Re: Colaboración editorial — {domain}"
    body = f"""
    <p>Hola,</p>
    <p>Quería confirmar la colaboración para publicar un artículo en <strong>{domain}</strong>.</p>
    <p>Nos ponemos en contacto para coordinar los próximos pasos.</p>
    <p>¿Podés confirmar los detalles del acuerdo?</p>
    <br>
    <p>Saludos,<br>Felipe<br>KokoLinks Media</p>
    """
    return subject, body


def template_wf1_new(row: dict) -> tuple[str, str]:
    """WF1 rama 'No tenemos deal' (W=new). Primera propuesta."""
    domain = row.get("Domain", row.get("Website", ""))
    subject = f"Propuesta de colaboración — {domain}"
    body = f"""
    <p>Hola,</p>
    <p>Mi nombre es Felipe, soy de KokoLinks Media. Nos especializamos en contenido editorial de calidad.</p>
    <p>Nos gustaría publicar un artículo en <strong>{domain}</strong>. ¿Estarían interesados en una colaboración?</p>
    <p>Podemos adaptarnos a sus guidelines editoriales y temáticas.</p>
    <br>
    <p>¿Tienen disponibilidad para una llamada rápida esta semana?</p>
    <br>
    <p>Saludos,<br>Felipe<br>KokoLinks Media</p>
    """
    return subject, body


def template_wf3(row: dict, article_url: str = "") -> tuple[str, str]:
    """WF3 — Enviar artículo al cliente para revisión."""
    domain = row.get("Domain", row.get("Website", ""))
    subject = f"Artículo listo para revisión — {domain}"
    body = f"""
    <p>Hola,</p>
    <p>El artículo para <strong>{domain}</strong> ya está listo para tu revisión.</p>
    {"<p>Podés verlo aquí: <a href='" + article_url + "'>" + article_url + "</a></p>" if article_url else ""}
    <p>Por favor avisanos si tenés comentarios o si está aprobado para proceder con la publicación.</p>
    <br>
    <p>Saludos,<br>KokoLinks Media</p>
    """
    return subject, body


def template_wf4(row: dict) -> tuple[str, str]:
    """WF4 — Email al medio para publicar el artículo aprobado."""
    domain = row.get("Domain", row.get("Website", ""))
    subject = f"Artículo aprobado — listo para publicar en {domain}"
    body = f"""
    <p>Hola,</p>
    <p>El artículo ha sido revisado y aprobado por nuestro cliente.</p>
    <p>¿Pueden proceder con la publicación en <strong>{domain}</strong>?</p>
    <p>Quedamos atentos para confirmar la URL live una vez publicado.</p>
    <br>
    <p>Saludos,<br>Felipe<br>KokoLinks Media</p>
    """
    return subject, body

# ─── WF1 ──────────────────────────────────────────────────────────────────────

@app.post("/wf1/trigger", dependencies=[Depends(verify_webhook)])
async def wf1_trigger(req: WF1Request):
    """
    WF1 — Primer email al medio.
    Condición: columna W = 'yes' o W = 'new' (y AE != 'Live' para evitar reprocesar).
    Actualiza W='Sent', Y=messageId, Z=gmail_url.
    """
    formula = "OR(FIND('yes',LOWER({W})),FIND('new',LOWER({W})))"
    rows = await airtable_list(req.linkplan_table_id, formula)

    processed = []
    errors = []

    for rec in rows:
        fields = rec.get("fields", {})
        rec_id = rec["id"]

        # Evitar reprocesar si ya está Live
        if fields.get("AE", "").lower() == "live":
            continue

        email_to = fields.get("Q", "").strip()
        if not email_to:
            errors.append({"id": rec_id, "reason": "Sin email en columna Q"})
            continue

        w_value = fields.get("W", "").lower()
        domain = fields.get("Domain", fields.get("Website", rec_id))

        if req.dry_run:
            processed.append({"id": rec_id, "domain": domain, "to": email_to, "branch": "deal" if "yes" in w_value else "new", "dry_run": True})
            continue

        try:
            if "yes" in w_value:
                subject, body = template_wf1_deal(fields)
                branch = "deal"
            else:
                subject, body = template_wf1_new(fields)
                branch = "new"

            result = send_email(email_to, subject, body)

            await airtable_update(req.linkplan_table_id, rec_id, {
                "W": "Sent",
                "Y": result["message_id"],
                "Z": result["gmail_url"],
            })

            processed.append({
                "id": rec_id,
                "domain": domain,
                "to": email_to,
                "branch": branch,
                "message_id": result["message_id"],
                "gmail_url": result["gmail_url"],
            })
            logger.info(f"WF1 OK: {domain} → {email_to} (branch={branch})")

        except Exception as e:
            logger.error(f"WF1 error en {rec_id}: {e}")
            errors.append({"id": rec_id, "domain": domain, "error": str(e)})

    return {
        "wf": "WF1",
        "processed": len(processed),
        "errors": len(errors),
        "detail": processed,
        "error_detail": errors,
    }

# ─── WF3 ──────────────────────────────────────────────────────────────────────

@app.post("/wf3/trigger", dependencies=[Depends(verify_webhook)])
async def wf3_trigger(req: WF3Request):
    """
    WF3 — Enviar artículo al cliente para revisión.
    Condición: AB = 'Checking by Client' (y AD != 'Article sent').
    Lee Z (URL de la conversación Gmail del medio) para incluirla como referencia.
    """
    formula = "AND({AB}='Checking by Client',{AD}!='Article sent')"
    rows = await airtable_list(req.linkplan_table_id, formula)

    processed = []
    errors = []

    for rec in rows:
        fields = rec.get("fields", {})
        rec_id = rec["id"]

        # WF3 envía al cliente, no al medio.
        # El email del cliente debería estar en otra columna; usamos Q como fallback.
        # TODO: confirmar con Felipe qué columna tiene el email del cliente.
        client_email = fields.get("Client Email", fields.get("Q", "")).strip()
        if not client_email:
            errors.append({"id": rec_id, "reason": "Sin email de cliente"})
            continue

        domain = fields.get("Domain", fields.get("Website", rec_id))
        article_url = fields.get("Z", "")  # URL de la convo Gmail o del artículo en Drive

        if req.dry_run:
            processed.append({"id": rec_id, "domain": domain, "to": client_email, "dry_run": True})
            continue

        try:
            subject, body = template_wf3(fields, article_url)
            result = send_email(client_email, subject, body)

            await airtable_update(req.linkplan_table_id, rec_id, {
                "AD": "Article sent",
            })

            processed.append({
                "id": rec_id,
                "domain": domain,
                "to": client_email,
                "message_id": result["message_id"],
            })
            logger.info(f"WF3 OK: {domain} → {client_email}")

        except Exception as e:
            logger.error(f"WF3 error en {rec_id}: {e}")
            errors.append({"id": rec_id, "domain": domain, "error": str(e)})

    return {
        "wf": "WF3",
        "processed": len(processed),
        "errors": len(errors),
        "detail": processed,
        "error_detail": errors,
    }

# ─── WF4 ──────────────────────────────────────────────────────────────────────

@app.post("/wf4/trigger", dependencies=[Depends(verify_webhook)])
async def wf4_trigger(req: WF4Request):
    """
    WF4 — Email al medio para publicar el artículo aprobado.
    Condición: AC = 'Approved' (y AE != 'Live').
    Actualiza AC='Enviado'.
    """
    formula = "AND({AC}='Approved',{AE}!='Live')"
    rows = await airtable_list(req.linkplan_table_id, formula)

    processed = []
    errors = []

    for rec in rows:
        fields = rec.get("fields", {})
        rec_id = rec["id"]

        email_to = fields.get("Q", "").strip()
        if not email_to:
            errors.append({"id": rec_id, "reason": "Sin email en columna Q"})
            continue

        domain = fields.get("Domain", fields.get("Website", rec_id))

        if req.dry_run:
            processed.append({"id": rec_id, "domain": domain, "to": email_to, "dry_run": True})
            continue

        try:
            subject, body = template_wf4(fields)
            result = send_email(email_to, subject, body)

            await airtable_update(req.linkplan_table_id, rec_id, {
                "AC": "Enviado",
            })

            processed.append({
                "id": rec_id,
                "domain": domain,
                "to": email_to,
                "message_id": result["message_id"],
            })
            logger.info(f"WF4 OK: {domain} → {email_to}")

        except Exception as e:
            logger.error(f"WF4 error en {rec_id}: {e}")
            errors.append({"id": rec_id, "domain": domain, "error": str(e)})

    return {
        "wf": "WF4",
        "processed": len(processed),
        "errors": len(errors),
        "detail": processed,
        "error_detail": errors,
    }

# ─── WF2 ──────────────────────────────────────────────────────────────────────

@app.post("/wf2/trigger", dependencies=[Depends(verify_webhook)])
async def wf2_trigger(req: WF2Request):
    """
    WF2 — Actualizar live link en la hoja cliente.
    Condición: AF tiene URL + AJ = 'Approved' (y AE != 'Live').
    Busca la fila correspondiente en la tabla cliente y escribe el live link.
    Actualiza AE='Live' en la tabla interna.
    """
    formula = "AND({AF}!='',{AJ}='Approved',{AE}!='Live')"
    rows = await airtable_list(req.linkplan_table_id, formula)

    processed = []
    errors = []

    for rec in rows:
        fields = rec.get("fields", {})
        rec_id = rec["id"]
        live_link = fields.get("AF", "").strip()
        domain = fields.get("Domain", fields.get("Website", rec_id))

        if not live_link:
            continue

        if req.dry_run:
            processed.append({"id": rec_id, "domain": domain, "live_link": live_link, "dry_run": True})
            continue

        try:
            # Buscar la fila en la hoja cliente por dominio
            client_rows = await airtable_list(
                req.client_sheet_table_id,
                f"{{Domain}}='{domain}'",
            )

            if client_rows:
                client_rec_id = client_rows[0]["id"]
                await airtable_update(req.client_sheet_table_id, client_rec_id, {
                    "AF": live_link,
                    "AE": "Live",
                })
            else:
                logger.warning(f"WF2: no se encontró fila en hoja cliente para dominio {domain}")

            # Marcar en la tabla interna
            await airtable_update(req.linkplan_table_id, rec_id, {
                "AE": "Live",
            })

            processed.append({
                "id": rec_id,
                "domain": domain,
                "live_link": live_link,
                "client_row_updated": bool(client_rows),
            })
            logger.info(f"WF2 OK: {domain} → live link: {live_link}")

        except Exception as e:
            logger.error(f"WF2 error en {rec_id}: {e}")
            errors.append({"id": rec_id, "domain": domain, "error": str(e)})

    return {
        "wf": "WF2",
        "processed": len(processed),
        "errors": len(errors),
        "detail": processed,
        "error_detail": errors,
    }

# ─── API CRM: Comparador de precios ───────────────────────────────────────────

PROVIDER_TABLES = {
    "Bazoom":           TABLES["bazoom"],
    "Leolytics":        TABLES["leolytics"],
    "WhitePress":       TABLES["whitepress"],
    "BacklinksGlobal":  TABLES["backlinksglobal"],
    "MeUp":             TABLES["meup"],
}

# Posibles nombres de campo "dominio" en Airtable (varía por proveedor)
DOMAIN_FIELD_CANDIDATES = ["Website", "Domain", "URL", "Site", "domain", "website"]
# Posibles nombres de campo "precio"
PRICE_FIELD_CANDIDATES  = ["Pub. (enlace)", "Price", "Price (USD)", "Precio Casino",
                           "Precio Oferta", "offer_price", "Precio", "Cost", "price"]
# Posibles nombres de campo "DR" (Domain Rating)
DR_FIELD_CANDIDATES     = ["DR", "Domain Rating", "DA", "da", "Authority"]


def find_field(fields: dict, candidates: list[str]) -> Optional[str]:
    for c in candidates:
        if c in fields:
            return fields[c]
    return None


def normalize_domain(raw: str) -> str:
    """Quita http/https, www y trailing slash para comparación."""
    d = raw.lower().strip()
    d = re.sub(r"^https?://", "", d)
    d = re.sub(r"^www\.", "", d)
    d = d.rstrip("/")
    return d


@app.get("/api/prices")
async def api_prices(domain: str = Query(..., description="Dominio a buscar, ej: example.com")):
    """
    Busca el dominio en los 5 proveedores y devuelve los precios comparados.
    El CRM llama este endpoint directamente desde el browser.
    """
    target = normalize_domain(domain)
    results = {}

    for provider, table_id in PROVIDER_TABLES.items():
        try:
            where = f"(Website,like,%{target}%)"
            data = await nocodb_list_page(table_id, where=where, limit=5)
            records = data.get("list", [])
            if not records:
                results[provider] = {"found": False}
                continue
            f = records[0]
            results[provider] = {
                "found": True,
                "domain": find_field(f, DOMAIN_FIELD_CANDIDATES),
                "price":  find_field(f, PRICE_FIELD_CANDIDATES),
                "dr":     find_field(f, DR_FIELD_CANDIDATES),
                "record_id": str(f.get("Id", "")),
                "raw_fields": f,
            }
        except Exception as e:
            results[provider] = {"error": str(e)}

    return {"domain": domain, "providers": results}


@app.get("/api/medios")
async def api_medios(
    page: int = Query(1, ge=1, description="Página (cada 100 registros)"),
    country: Optional[str] = Query(None, description="Filtrar por país"),
    price_min: Optional[float] = Query(None, description="Precio mínimo"),
    price_max: Optional[float] = Query(None, description="Precio máximo"),
    offset: Optional[str] = Query(None, description="Offset Airtable para paginación"),
):
    """
    Lista paginada de la tabla Bazoom (8090+ registros).
    Devuelve 100 registros por página junto con el offset para la siguiente.
    """
    table_id = TABLES["bazoom"]

    where_parts = []
    if country:
        where_parts.append(f"(Country,like,%{country}%)")
    if price_min is not None:
        where_parts.append(f"(Price,gte,{price_min})")
    if price_max is not None:
        where_parts.append(f"(Price,lte,{price_max})")
    where = "~and".join(where_parts) if where_parts else ""

    noco_offset = int(offset) if offset and offset.isdigit() else (page - 1) * 100
    data = await nocodb_list_page(table_id, where=where, limit=100, offset=noco_offset)

    records = data.get("list", [])
    page_info = data.get("pageInfo", {})
    medios = []
    for f in records:
        medios.append({
            "id":      str(f.get("Id", "")),
            "domain":  find_field(f, DOMAIN_FIELD_CANDIDATES) or "",
            "price":   find_field(f, PRICE_FIELD_CANDIDATES),
            "dr":      find_field(f, DR_FIELD_CANDIDATES),
            "country": f.get("Country", f.get("country", "")),
            "raw":     f,
        })

    next_offset = str(noco_offset + 100) if not page_info.get("isLastPage", True) else None
    return {
        "page": page,
        "count": len(medios),
        "next_offset": next_offset,
        "records": medios,
    }

# ─── Modelos API Campañas ─────────────────────────────────────────────────────

class CampaignCreate(BaseModel):
    id: Optional[str] = None
    date: Optional[str] = None
    target_page: Optional[str] = None
    domain: Optional[str] = None
    contact_email: Optional[str] = None
    enviar: Optional[str] = None
    status_bot: Optional[str] = None
    message_id: Optional[str] = None
    conversation_url: Optional[str] = None
    content_status: Optional[str] = None
    client_approval: Optional[str] = None
    live_link: Optional[str] = None
    comments: Optional[str] = None
    dr: Optional[int] = None
    price: Optional[str] = None
    language: Optional[str] = None
    category: Optional[str] = None
    country: Optional[str] = None
    anchor_type: Optional[str] = None
    anchor: Optional[str] = None
    history: Optional[str] = None
    content_topic: Optional[str] = None
    traffic: Optional[str] = None
    linkbuilder: Optional[str] = None
    content_url: Optional[str] = None
    link_final_status: Optional[str] = None
    invoice_status: Optional[str] = None
    article_sent_date: Optional[str] = None


class CampaignUpdate(BaseModel):
    date: Optional[str] = None
    target_page: Optional[str] = None
    domain: Optional[str] = None
    contact_email: Optional[str] = None
    enviar: Optional[str] = None
    status_bot: Optional[str] = None
    message_id: Optional[str] = None
    conversation_url: Optional[str] = None
    content_status: Optional[str] = None
    client_approval: Optional[str] = None
    live_link: Optional[str] = None
    comments: Optional[str] = None
    dr: Optional[int] = None
    price: Optional[str] = None
    language: Optional[str] = None
    category: Optional[str] = None
    country: Optional[str] = None
    link_final_status: Optional[str] = None
    invoice_status: Optional[str] = None
    article_sent_date: Optional[str] = None


def _fields_to_campaign(rec: dict) -> dict:
    """Normaliza un registro NocoDB al schema del CRM."""
    f = rec.get("fields", {})
    return {
        "airtable_id": rec["id"],  # conservamos el nombre para no romper el CRM
        "id": f.get("ID", ""),
        "date": f.get("Date", ""),
        "target_page": f.get("Target Page", ""),
        "domain": f.get("Domain", ""),
        "contact_email": f.get("Contact Email", ""),
        "enviar": f.get("Enviar", ""),
        "status_bot": f.get("Status BOT", ""),
        "message_id": f.get("Message ID", ""),
        "conversation_url": f.get("Conversation URL", ""),
        "content_status": f.get("Content Status", ""),
        "client_approval": f.get("Client Approval", ""),
        "live_link": f.get("Live Link", ""),
        "comments": f.get("Comments", ""),
        "dr": f.get("DR"),
        "price": f.get("Price", ""),
        "language": f.get("Language", ""),
        "category": f.get("Category", ""),
        "country": f.get("Country", ""),
        "anchor_type": f.get("Anchor Type", ""),
        "anchor": f.get("Anchor", ""),
        "history": f.get("History", ""),
        "content_topic": f.get("Content Topic", ""),
        "traffic": f.get("Traffic", ""),
        "linkbuilder": f.get("Linkbuilder", ""),
        "content_url": f.get("Content URL", ""),
        "link_final_status": f.get("Link Final Status", ""),
        "invoice_status": f.get("Invoice Status", ""),
        "article_sent_date": f.get("Article Sent Date", ""),
    }


def _campaign_to_fields(data: dict) -> dict:
    """Convierte campos del modelo al formato Airtable (omite None)."""
    mapping = {
        "id": "ID",
        "date": "Date",
        "target_page": "Target Page",
        "domain": "Domain",
        "contact_email": "Contact Email",
        "enviar": "Enviar",
        "status_bot": "Status BOT",
        "message_id": "Message ID",
        "conversation_url": "Conversation URL",
        "content_status": "Content Status",
        "client_approval": "Client Approval",
        "live_link": "Live Link",
        "comments": "Comments",
        "dr": "DR",
        "price": "Price",
        "language": "Language",
        "category": "Category",
        "country": "Country",
        "anchor_type": "Anchor Type",
        "anchor": "Anchor",
        "history": "History",
        "content_topic": "Content Topic",
        "traffic": "Traffic",
        "linkbuilder": "Linkbuilder",
        "content_url": "Content URL",
        "link_final_status": "Link Final Status",
        "invoice_status": "Invoice Status",
        "article_sent_date": "Article Sent Date",
    }
    fields = {}
    for model_key, airtable_key in mapping.items():
        val = data.get(model_key)
        if val is not None:
            fields[airtable_key] = val
    return fields


# ─── API Campañas ─────────────────────────────────────────────────────────────

@app.get("/api/campaigns")
async def api_campaigns_list(
    status: Optional[str] = Query(None, description="Filtrar por Content Status (ej: Checking by Client, Approved, Live)"),
    country: Optional[str] = Query(None, description="Filtrar por Country"),
    enviar: Optional[str] = Query(None, description="Filtrar por Enviar (yes, new, Sent)"),
):
    """
    Lista todas las publicaciones de la tabla Linkplans.
    Soporta filtros opcionales por status, country y enviar.
    """
    table_id = TABLES["linkplans"]
    if not table_id:
        raise HTTPException(status_code=503, detail="NOCODB_TABLE_LINKPLANS no configurado")

    where_parts = []
    if status and status.lower() != "all":
        where_parts.append(f"(Content Status,eq,{status})")
    if country and country.lower() != "all":
        where_parts.append(f"(Country,like,%{country}%)")
    if enviar:
        where_parts.append(f"(Enviar,like,%{enviar}%)")
    where = "~and".join(where_parts)

    records = await nocodb_list(table_id, where=where)
    return {
        "count": len(records),
        "records": [_fields_to_campaign(r) for r in records],
    }


@app.get("/api/campaigns/{record_id}")
async def api_campaign_detail(record_id: str):
    """Detalle de una publicación por su ID NocoDB."""
    table_id = TABLES["linkplans"]
    if not table_id:
        raise HTTPException(status_code=503, detail="NOCODB_TABLE_LINKPLANS no configurado")
    rec = await nocodb_get(table_id, record_id)
    return _fields_to_campaign(rec)


@app.post("/api/campaigns", status_code=201)
async def api_campaign_create(data: CampaignCreate):
    """Crea una nueva publicación en la tabla Linkplans."""
    table_id = TABLES["linkplans"]
    if not table_id:
        raise HTTPException(status_code=503, detail="NOCODB_TABLE_LINKPLANS no configurado")
    fields = _campaign_to_fields(data.model_dump(exclude_none=True))
    if not fields:
        raise HTTPException(status_code=422, detail="No se recibieron campos para crear")
    rec = await nocodb_create(table_id, fields)
    return _fields_to_campaign(rec)


@app.patch("/api/campaigns/{record_id}")
async def api_campaign_update(record_id: str, data: CampaignUpdate):
    """Actualiza campos de una publicación (live link, comments, status, etc.)."""
    table_id = TABLES["linkplans"]
    if not table_id:
        raise HTTPException(status_code=503, detail="NOCODB_TABLE_LINKPLANS no configurado")
    fields = _campaign_to_fields(data.model_dump(exclude_none=True))
    if not fields:
        raise HTTPException(status_code=422, detail="No se recibieron campos para actualizar")
    rec = await nocodb_update(table_id, record_id, fields)
    return _fields_to_campaign(rec)


# ─── Status ───────────────────────────────────────────────────────────────────

@app.get("/status")
async def status():
    """
    Resumen de campañas activas.
    Requiere NOCODB_TABLE_LINKPLANS configurado.
    """
    table_id = TABLES.get("linkplans", "")
    if not table_id:
        return {
            "warning": "NOCODB_TABLE_LINKPLANS no configurado.",
            "counts": {},
        }

    counts = {}
    queries = {
        "pending_wf1":  "(Enviar,in,yes,new)",
        "checking":     "(Content Status,eq,Checking by Client)",
        "approved_wf4": "(Content Status,eq,Approved)",
        "live":         "(Live Link,isnot,)",
        "total":        "",
    }

    for key, where in queries.items():
        try:
            rows = await nocodb_list(table_id, where=where)
            counts[key] = len(rows)
        except Exception as e:
            counts[key] = f"error: {e}"

    return {"table": table_id, "counts": counts}


@app.get("/api/pricelists")
async def api_pricelists(
    search: Optional[str] = Query(None, description="Buscar por dominio"),
    country: Optional[str] = Query(None, description="Filtrar por país"),
    limit: int = Query(100, le=500),
    offset: int = Query(0, ge=0),
):
    """
    Devuelve la tabla Price Lists con columnas clave por proveedor.
    Columnas: Website, Country, DR, Traffic, Precio Kokko, Bazoom, Leolytics, WhitePress, BacklinksGlobal, MeUp
    """
    table_id = TABLES["price_lists"]

    where_parts = []
    if search:
        where_parts.append(f"(Website,like,%{search}%)")
    if country:
        where_parts.append(f"(Country,like,%{country}%)")
    where = "~and".join(where_parts)

    data = await nocodb_list_page(table_id, where=where, limit=limit, offset=offset)
    rows = data.get("list", [])
    page_info = data.get("pageInfo", {})

    records = []
    for r in rows:
        records.append({
            "id":           str(r.get("Id", "")),
            "website":      r.get("Website") or r.get("col_0") or "",
            "bbdd":         r.get("¿BBDD?") or r.get("col_1") or "",
            "country":      r.get("Country") or r.get("col_2") or "",
            "dr":           r.get("DR") or r.get("col_3") or "",
            "traffic":      r.get("Traffic") or r.get("col_4") or "",
            "precio_kokko": r.get("PRECIOS KOKKO") or r.get("col_5") or "",
            "p_casino":     r.get("P. CASINO") or r.get("col_6") or "",
            "bazoom":       r.get("PRECIO") or r.get("col_8") or "",
            "leolytics":    r.get("PRECIO_1") or r.get("col_11") or "",
            "whitepress":   r.get("PRECIO_2") or r.get("col_14") or "",
            "backlinks":    r.get("PRECIO_3") or r.get("col_17") or "",
            "meup":         r.get("PRECIO_4") or r.get("col_18") or "",
        })

    return {
        "total": page_info.get("totalRows", len(records)),
        "offset": offset,
        "is_last_page": page_info.get("isLastPage", True),
        "records": records,
    }


@app.get("/health")
async def health():
    return {"status": "ok", "service": "kokolinks-worker"}
