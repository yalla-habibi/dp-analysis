from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import (
    String, Text, Integer, DateTime, ForeignKey, UniqueConstraint, create_engine
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker


class Base(DeclarativeBase):
    pass


class Article(Base):
    __tablename__ = "articles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    url: Mapped[str] = mapped_column(String(1024), unique=True, index=True)

    title: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    category: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    published_at: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    source_hint: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)

    raw_html: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    clean_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    labels: Mapped[list["LLMLabel"]] = relationship(back_populates="article", cascade="all, delete-orphan")


class LLMLabel(Base):
    __tablename__ = "llm_labels"
    __table_args__ = (
        UniqueConstraint("article_id", "model", "task", name="uq_label_article_model_task"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    article_id: Mapped[int] = mapped_column(ForeignKey("articles.id"), index=True)

    model: Mapped[str] = mapped_column(String(128))
    task: Mapped[str] = mapped_column(String(128))

    json: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    article: Mapped["Article"] = relationship(back_populates="labels")


def make_session(db_url: str):
    engine = create_engine(db_url, future=True)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def init_db(db_url: str) -> None:
    engine = create_engine(db_url, future=True)
    Base.metadata.create_all(engine)
