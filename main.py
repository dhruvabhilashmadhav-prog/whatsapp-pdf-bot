import os
import sys
import json
import time
import shutil
import asyncio
import logging
from datetime import datetime
from typing import List, Optional

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Response, HTTPException, Depends, Query, BackgroundTasks, UploadFile, File, Form
from fastapi.responses import HTMLResponse, PlainTextResponse, StreamingResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from database import engine, Base, get_db, SessionLocal
from models import Message, Image, PDF, Setting
from pdf_utils import convert_single_image_to_pdf, convert_multiple_images_to_pdf

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("whatsapp_pdf_app")

# Load environment variables
load_dotenv()

WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN", "").strip()
PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID", "").strip()
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "").strip()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")
IMAGES_DIR = os.path.join(STATIC_DIR, "uploads", "images")
PDFS_DIR = os.path.join(STATIC_DIR, "uploads", "pdfs")
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")

os.makedirs(IMAGES_DIR, exist_ok=True)
os.makedirs(PDFS_DIR, exist_ok=True)
os.makedirs(TEMPLATES_DIR, exist_ok=True)

# Create DB tables
Base.metadata.create_all(bind=engine)

# Initialize Auto-Convert setting if not present
with SessionLocal() as db:
    setting = db.query(Setting).filter(Setting.key == "auto_convert").first()
    if not setting:
        db.add(Setting(key="auto_convert", value="false"))
        db.commit()

app = FastAPI(title="WhatsApp Cloud PDF Studio")

# Mount static files
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory SSE client connections
sse_clients: List[asyncio.Queue] = []

async def broadcast_event(event_type: str, data: dict):
    """Pushes SSE message to all connected clients."""
    payload = f"event: {event_type}\ndata: {json.dumps(data)}\n\n"
    dead_clients = []
    for client in sse_clients:
        try:
            await client.put(payload)
        except Exception as e:
            logger.warning(f"Failed to queue event for client: {e}")
            dead_clients.append(client)
    for dead in dead_clients:
        if dead in sse_clients:
            sse_clients.remove(dead)

def get_auto_convert_setting(db: Session) -> bool:
    setting = db.query(Setting).filter(Setting.key == "auto_convert").first()
    return setting.value.lower() == "true" if setting else False

def set_auto_convert_setting(db: Session, enabled: bool):
    setting = db.query(Setting).filter(Setting.key == "auto_convert").first()
    if not setting:
        setting = Setting(key="auto_convert", value=str(enabled).lower())
        db.add(setting)
    else:
        setting.value = str(enabled).lower()
    db.commit()

def send_whatsapp_reply(recipient_number: str, text_message: str):
    """Sends a text reply back to the sender via WhatsApp Cloud API."""
    if not WHATSAPP_TOKEN or not PHONE_NUMBER_ID:
        logger.warning("WHATSAPP_TOKEN or PHONE_NUMBER_ID is missing. Skipping WhatsApp reply.")
        return

    url = f"https://graph.facebook.com/v21.0/{PHONE_NUMBER_ID}/messages"
    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": recipient_number,
        "type": "text",
        "text": {
            "preview_url": False,
            "body": text_message
        }
    }
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        logger.info(f"WhatsApp reply to {recipient_number} status: {response.status_code}, resp: {response.text}")
    except Exception as e:
        logger.error(f"Error sending WhatsApp reply to {recipient_number}: {e}")

def download_whatsapp_media(media_id: str) -> Optional[tuple[bytes, str]]:
    """Downloads binary media from WhatsApp Cloud API using media_id."""
    # Step 1: Query media URL
    url_graph = f"https://graph.facebook.com/v21.0/{media_id}"
    headers = {"Authorization": f"Bearer {WHATSAPP_TOKEN}"}
    try:
        res = requests.get(url_graph, headers=headers, timeout=10)
        if res.status_code != 200:
            logger.error(f"Failed to retrieve media URL for {media_id}: {res.status_code} {res.text}")
            return None
        media_data = res.json()
        download_url = media_data.get("url")
        mime_type = media_data.get("mime_type", "image/jpeg")

        if not download_url:
            logger.error(f"No download URL in media response: {media_data}")
            return None

        # Step 2: Download binary data
        media_res = requests.get(download_url, headers=headers, timeout=15)
        if media_res.status_code == 200:
            return media_res.content, mime_type
        else:
            logger.error(f"Failed to download media binary from {download_url}: {media_res.status_code}")
            return None
    except Exception as e:
        logger.error(f"Exception downloading media {media_id}: {e}")
        return None

# ==================== WEBHOOK ENDPOINTS ====================

@app.api_route("/webhook", methods=["GET", "HEAD"])
def verify_webhook(
    hub_mode: Optional[str] = Query(None, alias="hub.mode"),
    hub_verify_token: Optional[str] = Query(None, alias="hub.verify_token"),
    hub_challenge: Optional[str] = Query(None, alias="hub.challenge"),
):
    """
    Verification endpoint for Meta WhatsApp Cloud API Webhook.
    Returns hub.challenge as plain text when hub.mode == "subscribe" and hub.verify_token == VERIFY_TOKEN.
    """
    logger.info(f"Webhook verification request: mode={hub_mode}, token={hub_verify_token}, challenge={hub_challenge}")
    if hub_mode == "subscribe" and hub_verify_token == VERIFY_TOKEN:
        if hub_challenge:
            # int(hub.challenge) as plain text or string challenge
            try:
                challenge_val = str(int(hub_challenge))
            except ValueError:
                challenge_val = str(hub_challenge)
            logger.info("Webhook verified successfully!")
            return PlainTextResponse(content=challenge_val, status_code=200)
    logger.warning("Webhook verification failed: Invalid verify token or mode")
    raise HTTPException(status_code=403, detail="Verification token mismatch")

async def process_webhook_payload(payload: dict):
    """Processes incoming Meta WhatsApp webhook payload in background."""
    db = SessionLocal()
    try:
        entries = payload.get("entry", [])
        for entry in entries:
            for change in entry.get("changes", []):
                value = change.get("value", {})
                messages = value.get("messages", [])

                for msg in messages:
                    sender = msg.get("from", "Unknown")
                    msg_id = msg.get("id")
                    timestamp = msg.get("timestamp", str(int(time.time())))
                    msg_type = msg.get("type")

                    # Deduplication check
                    existing_msg = db.query(Message).filter(Message.message_id == msg_id).first()
                    existing_img = db.query(Image).filter(Image.media_id == msg_id).first()
                    if existing_msg or existing_img:
                        logger.info(f"Message ID {msg_id} already processed. Skipping.")
                        continue

                    # 1. TEXT MESSAGE
                    if msg_type == "text":
                        body = msg.get("text", {}).get("body", "").strip()
                        new_msg = Message(
                            message_id=msg_id,
                            sender=sender,
                            text=body,
                            timestamp=timestamp,
                            created_at=datetime.utcnow()
                        )
                        db.add(new_msg)
                        db.commit()
                        db.refresh(new_msg)

                        # Broadcast new text message
                        await broadcast_event("new_message", {
                            "id": new_msg.id,
                            "message_id": new_msg.message_id,
                            "sender": new_msg.sender,
                            "text": new_msg.text,
                            "timestamp": new_msg.timestamp,
                            "created_at": new_msg.created_at.strftime("%Y-%m-%d %H:%M:%S")
                        })

                        # Check for batch "make pdf" command
                        if body.lower() == "make pdf":
                            logger.info(f"Received 'make pdf' command from sender {sender}")
                            # Find all unconverted images from this sender
                            unconverted_images = db.query(Image).filter(
                                Image.sender == sender,
                                Image.is_converted == False
                            ).order_by(Image.id.asc()).all()

                            if unconverted_images:
                                image_paths = [img.filepath for img in unconverted_images if os.path.exists(img.filepath)]
                                if image_paths:
                                    pdf_filename = f"batch_{sender}_{int(time.time())}.pdf"
                                    pdf_filepath = os.path.join(PDFS_DIR, pdf_filename)
                                    pdf_info = convert_multiple_images_to_pdf(image_paths, pdf_filepath)

                                    # Create PDF record
                                    source_ids = [str(img.id) for img in unconverted_images]
                                    new_pdf = PDF(
                                        filename=pdf_filename,
                                        filepath=pdf_filepath,
                                        sender=sender,
                                        page_count=pdf_info["page_count"],
                                        file_size=pdf_info["file_size"],
                                        source_images=",".join(source_ids),
                                        created_at=datetime.utcnow()
                                    )
                                    db.add(new_pdf)
                                    db.commit()
                                    db.refresh(new_pdf)

                                    # Mark images as converted
                                    for img in unconverted_images:
                                        img.is_converted = True
                                        img.pdf_id = new_pdf.id
                                    db.commit()

                                    # Send WhatsApp reply confirmation
                                    send_whatsapp_reply(sender, "✅ Your multi-page PDF is ready on the website!")

                                    # Broadcast new PDF event and image updates
                                    await broadcast_event("new_pdf", {
                                        "id": new_pdf.id,
                                        "filename": new_pdf.filename,
                                        "sender": new_pdf.sender,
                                        "page_count": new_pdf.page_count,
                                        "file_size": new_pdf.file_size,
                                        "created_at": new_pdf.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                                        "url": f"/static/uploads/pdfs/{new_pdf.filename}"
                                    })
                                    await broadcast_event("images_updated", {"updated_ids": [img.id for img in unconverted_images]})
                            else:
                                send_whatsapp_reply(sender, "⚠️ No unconverted images found to convert. Please send images first!")

                    # 2. IMAGE MESSAGE
                    elif msg_type == "image":
                        image_info = msg.get("image", {})
                        media_id = image_info.get("id")
                        caption = image_info.get("caption", "")

                        media_result = download_whatsapp_media(media_id)
                        if media_result:
                            media_bytes, mime_type = media_result
                            ext = ".jpg"
                            if "png" in mime_type:
                                ext = ".png"
                            elif "webp" in mime_type:
                                ext = ".webp"

                            img_filename = f"img_{sender}_{media_id}{ext}"
                            img_filepath = os.path.join(IMAGES_DIR, img_filename)

                            with open(img_filepath, "wb") as f:
                                f.write(media_bytes)

                            new_image = Image(
                                media_id=media_id,
                                sender=sender,
                                filename=img_filename,
                                filepath=img_filepath,
                                caption=caption,
                                timestamp=timestamp,
                                is_converted=False,
                                created_at=datetime.utcnow()
                            )
                            db.add(new_image)
                            db.commit()
                            db.refresh(new_image)

                            # Check auto-convert trigger:
                            # 1) Dashboard "Auto-Convert All Images to PDF" toggle is ON
                            # 2) Image caption contains "pdf" (case-insensitive)
                            auto_convert_enabled = get_auto_convert_setting(db)
                            caption_has_pdf = caption and ("pdf" in caption.lower())

                            if auto_convert_enabled or caption_has_pdf:
                                logger.info(f"Auto-converting image {new_image.id} (auto_toggle={auto_convert_enabled}, caption_trigger={caption_has_pdf})")
                                pdf_filename = f"pdf_{new_image.id}_{int(time.time())}.pdf"
                                pdf_filepath = os.path.join(PDFS_DIR, pdf_filename)
                                pdf_info = convert_single_image_to_pdf(img_filepath, pdf_filepath)

                                new_pdf = PDF(
                                    filename=pdf_filename,
                                    filepath=pdf_filepath,
                                    sender=sender,
                                    page_count=pdf_info["page_count"],
                                    file_size=pdf_info["file_size"],
                                    source_images=str(new_image.id),
                                    created_at=datetime.utcnow()
                                )
                                db.add(new_pdf)
                                db.commit()
                                db.refresh(new_pdf)

                                new_image.is_converted = True
                                new_image.pdf_id = new_pdf.id
                                db.commit()

                                # Broadcast image & pdf
                                await broadcast_event("new_image", {
                                    "id": new_image.id,
                                    "media_id": new_image.media_id,
                                    "sender": new_image.sender,
                                    "filename": new_image.filename,
                                    "caption": new_image.caption,
                                    "timestamp": new_image.timestamp,
                                    "is_converted": new_image.is_converted,
                                    "pdf_id": new_image.pdf_id,
                                    "url": f"/static/uploads/images/{new_image.filename}",
                                    "created_at": new_image.created_at.strftime("%Y-%m-%d %H:%M:%S")
                                })

                                await broadcast_event("new_pdf", {
                                    "id": new_pdf.id,
                                    "filename": new_pdf.filename,
                                    "sender": new_pdf.sender,
                                    "page_count": new_pdf.page_count,
                                    "file_size": new_pdf.file_size,
                                    "created_at": new_pdf.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                                    "url": f"/static/uploads/pdfs/{new_pdf.filename}"
                                })
                            else:
                                # Broadcast new unconverted image
                                await broadcast_event("new_image", {
                                    "id": new_image.id,
                                    "media_id": new_image.media_id,
                                    "sender": new_image.sender,
                                    "filename": new_image.filename,
                                    "caption": new_image.caption,
                                    "timestamp": new_image.timestamp,
                                    "is_converted": new_image.is_converted,
                                    "pdf_id": new_image.pdf_id,
                                    "url": f"/static/uploads/images/{new_image.filename}",
                                    "created_at": new_image.created_at.strftime("%Y-%m-%d %H:%M:%S")
                                })
    except Exception as e:
        logger.error(f"Error processing webhook payload: {e}", exc_info=True)
    finally:
        db.close()

@app.post("/webhook")
async def receive_webhook(request: Request, background_tasks: BackgroundTasks):
    """
    Receives incoming webhook events from Meta WhatsApp Cloud API.
    Always returns 200 OK immediately and processes payload in background.
    """
    try:
        payload = await request.json()
        logger.info(f"Incoming WhatsApp webhook payload: {json.dumps(payload)[:300]}...")
        background_tasks.add_task(process_webhook_payload, payload)
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"Error receiving webhook: {e}")
        return {"status": "ok"}

# ==================== LIVE SSE STREAM ====================

@app.get("/api/events")
async def sse_events(request: Request):
    """Server-Sent Events endpoint streaming live events to the dashboard."""
    queue = asyncio.Queue()
    sse_clients.append(queue)

    async def event_generator():
        try:
            # Send initial ping
            yield "event: connected\ndata: {\"status\": \"connected\"}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=20.0)
                    yield data
                except asyncio.TimeoutError:
                    # Heartbeat keep-alive
                    yield "event: ping\ndata: {}\n\n"
        finally:
            if queue in sse_clients:
                sse_clients.remove(queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )

# ==================== API ENDPOINTS ====================

@app.get("/api/data")
def get_all_data(db: Session = Depends(get_db)):
    """Returns initial data for the dashboard feeds."""
    messages = db.query(Message).order_by(Message.id.desc()).all()
    images = db.query(Image).order_by(Image.id.desc()).all()
    pdfs = db.query(PDF).order_by(PDF.id.desc()).all()
    auto_convert = get_auto_convert_setting(db)

    return {
        "auto_convert": auto_convert,
        "messages": [
            {
                "id": m.id,
                "message_id": m.message_id,
                "sender": m.sender,
                "text": m.text,
                "timestamp": m.timestamp,
                "created_at": m.created_at.strftime("%Y-%m-%d %H:%M:%S")
            }
            for m in messages
        ],
        "images": [
            {
                "id": i.id,
                "media_id": i.media_id,
                "sender": i.sender,
                "filename": i.filename,
                "caption": i.caption,
                "timestamp": i.timestamp,
                "is_converted": i.is_converted,
                "pdf_id": i.pdf_id,
                "url": f"/static/uploads/images/{i.filename}",
                "created_at": i.created_at.strftime("%Y-%m-%d %H:%M:%S")
            }
            for i in images
        ],
        "pdfs": [
            {
                "id": p.id,
                "filename": p.filename,
                "sender": p.sender,
                "page_count": p.page_count,
                "file_size": p.file_size,
                "created_at": p.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                "url": f"/static/uploads/pdfs/{p.filename}"
            }
            for p in pdfs
        ]
    }

@app.get("/api/settings/auto-convert")
def get_setting_auto_convert(db: Session = Depends(get_db)):
    return {"auto_convert": get_auto_convert_setting(db)}

@app.post("/api/settings/auto-convert")
async def toggle_setting_auto_convert(request: Request, db: Session = Depends(get_db)):
    data = await request.json()
    enabled = bool(data.get("enabled", False))
    set_auto_convert_setting(db, enabled)
    await broadcast_event("setting_updated", {"auto_convert": enabled})
    return {"status": "success", "auto_convert": enabled}

@app.post("/convert/{image_id}")
async def convert_image_by_id(image_id: int, db: Session = Depends(get_db)):
    """Manual 1-click conversion of any single image into a PDF."""
    image_record = db.query(Image).filter(Image.id == image_id).first()
    if not image_record:
        raise HTTPException(status_code=404, detail="Image not found")

    if not os.path.exists(image_record.filepath):
        raise HTTPException(status_code=404, detail="Image file missing from disk")

    pdf_filename = f"manual_{image_record.id}_{int(time.time())}.pdf"
    pdf_filepath = os.path.join(PDFS_DIR, pdf_filename)

    try:
        pdf_info = convert_single_image_to_pdf(image_record.filepath, pdf_filepath)
    except Exception as e:
        logger.error(f"Error converting image {image_id} to PDF: {e}")
        raise HTTPException(status_code=500, detail=f"Conversion failed: {str(e)}")

    new_pdf = PDF(
        filename=pdf_filename,
        filepath=pdf_filepath,
        sender=image_record.sender,
        page_count=pdf_info["page_count"],
        file_size=pdf_info["file_size"],
        source_images=str(image_record.id),
        created_at=datetime.utcnow()
    )
    db.add(new_pdf)
    db.commit()
    db.refresh(new_pdf)

    image_record.is_converted = True
    image_record.pdf_id = new_pdf.id
    db.commit()

    pdf_payload = {
        "id": new_pdf.id,
        "filename": new_pdf.filename,
        "sender": new_pdf.sender,
        "page_count": new_pdf.page_count,
        "file_size": new_pdf.file_size,
        "created_at": new_pdf.created_at.strftime("%Y-%m-%d %H:%M:%S"),
        "url": f"/static/uploads/pdfs/{new_pdf.filename}"
    }

    await broadcast_event("new_pdf", pdf_payload)
    await broadcast_event("image_updated", {
        "id": image_record.id,
        "is_converted": True,
        "pdf_id": new_pdf.id
    })

    return {"status": "success", "pdf": pdf_payload}

@app.post("/api/convert-batch")
async def convert_batch_manual(request: Request, db: Session = Depends(get_db)):
    """Manual trigger to convert all unconverted images for a specific sender or all senders."""
    data = await request.json()
    sender = data.get("sender")

    query = db.query(Image).filter(Image.is_converted == False)
    if sender:
        query = query.filter(Image.sender == sender)
    unconverted = query.order_by(Image.id.asc()).all()

    if not unconverted:
        return {"status": "no_op", "message": "No unconverted images available."}

    image_paths = [img.filepath for img in unconverted if os.path.exists(img.filepath)]
    if not image_paths:
        return {"status": "error", "message": "Image files missing on disk."}

    sender_label = sender if sender else "merged"
    pdf_filename = f"batch_{sender_label}_{int(time.time())}.pdf"
    pdf_filepath = os.path.join(PDFS_DIR, pdf_filename)
    pdf_info = convert_multiple_images_to_pdf(image_paths, pdf_filepath)

    new_pdf = PDF(
        filename=pdf_filename,
        filepath=pdf_filepath,
        sender=sender_label,
        page_count=pdf_info["page_count"],
        file_size=pdf_info["file_size"],
        source_images=",".join([str(img.id) for img in unconverted]),
        created_at=datetime.utcnow()
    )
    db.add(new_pdf)
    db.commit()
    db.refresh(new_pdf)

    for img in unconverted:
        img.is_converted = True
        img.pdf_id = new_pdf.id
    db.commit()

    pdf_payload = {
        "id": new_pdf.id,
        "filename": new_pdf.filename,
        "sender": new_pdf.sender,
        "page_count": new_pdf.page_count,
        "file_size": new_pdf.file_size,
        "created_at": new_pdf.created_at.strftime("%Y-%m-%d %H:%M:%S"),
        "url": f"/static/uploads/pdfs/{new_pdf.filename}"
    }

    await broadcast_event("new_pdf", pdf_payload)
    await broadcast_event("images_updated", {"updated_ids": [img.id for img in unconverted]})

    return {"status": "success", "pdf": pdf_payload}

@app.post("/api/clear")
async def clear_all_data(db: Session = Depends(get_db)):
    """Deletes all messages, images, PDFs from the database and cleans files from disk."""
    try:
        # Delete from DB
        db.query(Message).delete()
        db.query(Image).delete()
        db.query(PDF).delete()
        db.commit()

        # Clean image files
        for filename in os.listdir(IMAGES_DIR):
            file_path = os.path.join(IMAGES_DIR, filename)
            if os.path.isfile(file_path):
                try:
                    os.unlink(file_path)
                except Exception:
                    pass

        # Clean PDF files
        for filename in os.listdir(PDFS_DIR):
            file_path = os.path.join(PDFS_DIR, filename)
            if os.path.isfile(file_path):
                try:
                    os.unlink(file_path)
                except Exception:
                    pass

        await broadcast_event("clear_all", {"cleared_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")})
        return {"status": "success", "message": "All data and uploads cleared"}
    except Exception as e:
        db.rollback()
        logger.error(f"Error clearing data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==================== TEST SIMULATOR ENDPOINTS ====================

@app.post("/api/simulate/text")
async def simulate_text_message(
    sender: str = Form("15551234567"),
    text: str = Form("Hello from WhatsApp simulation!"),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """Simulates an incoming WhatsApp text message webhook."""
    fake_msg_id = f"sim_txt_{int(time.time() * 1000)}"
    fake_payload = {
        "object": "whatsapp_business_account",
        "entry": [
            {
                "id": "sim_entry",
                "changes": [
                    {
                        "value": {
                            "messaging_product": "whatsapp",
                            "messages": [
                                {
                                    "from": sender,
                                    "id": fake_msg_id,
                                    "timestamp": str(int(time.time())),
                                    "type": "text",
                                    "text": {"body": text}
                                }
                            ]
                        },
                        "field": "messages"
                    }
                ]
            }
        ]
    }
    background_tasks.add_task(process_webhook_payload, fake_payload)
    return {"status": "success", "message_id": fake_msg_id}

@app.post("/api/simulate/image")
async def simulate_image_upload(
    sender: str = Form("15551234567"),
    caption: str = Form(""),
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    """Simulates an incoming WhatsApp image by uploading a local file directly into the workflow."""
    fake_media_id = f"sim_img_{int(time.time() * 1000)}"
    filename_orig = file.filename or "image.jpg"
    ext = os.path.splitext(filename_orig)[1] or ".jpg"
    saved_filename = f"img_{sender}_{fake_media_id}{ext}"
    saved_filepath = os.path.join(IMAGES_DIR, saved_filename)

    with open(saved_filepath, "wb") as f:
        shutil.copyfileobj(file.file, f)

    new_image = Image(
        media_id=fake_media_id,
        sender=sender,
        filename=saved_filename,
        filepath=saved_filepath,
        caption=caption,
        timestamp=str(int(time.time())),
        is_converted=False,
        created_at=datetime.utcnow()
    )
    db.add(new_image)
    db.commit()
    db.refresh(new_image)

    auto_convert_enabled = get_auto_convert_setting(db)
    caption_has_pdf = caption and ("pdf" in caption.lower())

    if auto_convert_enabled or caption_has_pdf:
        pdf_filename = f"pdf_{new_image.id}_{int(time.time())}.pdf"
        pdf_filepath = os.path.join(PDFS_DIR, pdf_filename)
        pdf_info = convert_single_image_to_pdf(saved_filepath, pdf_filepath)

        new_pdf = PDF(
            filename=pdf_filename,
            filepath=pdf_filepath,
            sender=sender,
            page_count=pdf_info["page_count"],
            file_size=pdf_info["file_size"],
            source_images=str(new_image.id),
            created_at=datetime.utcnow()
        )
        db.add(new_pdf)
        db.commit()
        db.refresh(new_pdf)

        new_image.is_converted = True
        new_image.pdf_id = new_pdf.id
        db.commit()

        await broadcast_event("new_image", {
            "id": new_image.id,
            "media_id": new_image.media_id,
            "sender": new_image.sender,
            "filename": new_image.filename,
            "caption": new_image.caption,
            "timestamp": new_image.timestamp,
            "is_converted": new_image.is_converted,
            "pdf_id": new_image.pdf_id,
            "url": f"/static/uploads/images/{new_image.filename}",
            "created_at": new_image.created_at.strftime("%Y-%m-%d %H:%M:%S")
        })

        await broadcast_event("new_pdf", {
            "id": new_pdf.id,
            "filename": new_pdf.filename,
            "sender": new_pdf.sender,
            "page_count": new_pdf.page_count,
            "file_size": new_pdf.file_size,
            "created_at": new_pdf.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            "url": f"/static/uploads/pdfs/{new_pdf.filename}"
        })
    else:
        await broadcast_event("new_image", {
            "id": new_image.id,
            "media_id": new_image.media_id,
            "sender": new_image.sender,
            "filename": new_image.filename,
            "caption": new_image.caption,
            "timestamp": new_image.timestamp,
            "is_converted": new_image.is_converted,
            "pdf_id": new_image.pdf_id,
            "url": f"/static/uploads/images/{new_image.filename}",
            "created_at": new_image.created_at.strftime("%Y-%m-%d %H:%M:%S")
        })

    return {"status": "success", "image_id": new_image.id}

# ==================== FRONTEND ROOT ====================

@app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse)
def index_page():
    """Serves the live interactive dashboard."""
    index_file = os.path.join(TEMPLATES_DIR, "index.html")
    if os.path.exists(index_file):
        with open(index_file, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h1>Dashboard file not found.</h1>")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
