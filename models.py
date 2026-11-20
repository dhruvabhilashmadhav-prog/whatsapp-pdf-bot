from datetime import datetime
from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime
from database import Base

class Message(Base):
    __tablename__ = "messages"

    id = Column(Integer, primary_key=True, index=True)
    message_id = Column(String(255), unique=True, index=True, nullable=True)
    sender = Column(String(100), index=True)
    text = Column(Text, nullable=False)
    timestamp = Column(String(100), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

class Image(Base):
    __tablename__ = "images"

    id = Column(Integer, primary_key=True, index=True)
    media_id = Column(String(255), unique=True, index=True, nullable=True)
    sender = Column(String(100), index=True)
    filename = Column(String(255), nullable=False)
    filepath = Column(String(500), nullable=False)
    caption = Column(Text, nullable=True)
    timestamp = Column(String(100), nullable=True)
    is_converted = Column(Boolean, default=False)
    pdf_id = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

class PDF(Base):
    __tablename__ = "pdfs"

    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String(255), nullable=False)
    filepath = Column(String(500), nullable=False)
    sender = Column(String(100), index=True)
    page_count = Column(Integer, default=1)
    file_size = Column(String(50), nullable=False)
    source_images = Column(Text, nullable=True)  # JSON or comma-separated image IDs
    created_at = Column(DateTime, default=datetime.utcnow)

class Setting(Base):
    __tablename__ = "settings"

    key = Column(String(100), primary_key=True, index=True)
    value = Column(String(255), nullable=False)
