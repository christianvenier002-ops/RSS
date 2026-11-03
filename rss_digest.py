"""
Aggregatore RSS con thread.

Architettura:
  FeedWorker (1 thread per fonte) --> Queue --> Storage (1 thread, unico a scrivere su SQLite)
                                                    |
  DigestScheduler (1 thread) <---- legge dal DB ----+--> digest/AAAA-MM-GG.md

Dipendenze:  pip install feedparser pyautogui
WhatsApp:    il digest viene inviato tramite la sessione WhatsApp Web del browser predefinito.
Avvio:       python rss_digest.py     (Ctrl+C per fermare in modo pulito)
"""
import hashlib
import getpass
import json
import logging
import queue
import signal
import socket
import sqlite3
import threading
import time
import urllib.parse
import webbrowser
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import feedparser

CONFIG_FILE = Path("config.json")
DB_FILE = "news.db"
DIGEST_DIR = Path("digest")

log = logging.getLogger("rss")


@dataclass
class Article:
    uid: str
    source: str
    title: str
    link: str
    summary: str
    published: str


class FeedWorker(threading.Thread):
    """Scarica periodicamente UNA fonte e mette gli articoli in coda."""

    def __init__(self, name, url, interval, out_q, stop):
        super().__init__(name=f"feed-{name}", daemon=True)
        self.source, self.url, self.interval = name, url, interval
        self.out_q, self.stop = out_q, stop

    def run(self):
        etag = modified = None  # evita di riscaricare se il feed non è cambiato
        while not self.stop.is_set():
            try:
                feed = feedparser.parse(self.url, etag=etag, modified=modified)
                etag = getattr(feed, "etag", etag)
                modified = getattr(feed, "modified", modified)
                if feed.get("status") != 304:
                    for e in feed.entries:
                        link = e.get("link", "")
                        uid = hashlib.sha1((e.get("id") or link).encode()).hexdigest()
                        self.out_q.put(Article(
                            uid, self.source, e.get("title", "(senza titolo)"), link,
                            e.get("summary", ""), e.get("published", ""),
                        ))
                    log.info("%s: %d articoli letti", self.source, len(feed.entries))
            except Exception:
                log.exception("Errore sulla fonte %s", self.source)
            self.stop.wait(self.interval)  # come sleep(), ma si interrompe allo stop


class Storage(threading.Thread):
    """Unico thread che scrive nel DB: filtra per parole chiave e salva."""

    def __init__(self, in_q, keywords, stop):
        super().__init__(name="storage", daemon=True)
        self.in_q, self.keywords, self.stop = in_q, [k.lower() for k in keywords], stop

    def run(self):
        db = sqlite3.connect(DB_FILE)  # la connessione nasce DENTRO il thread
        db.execute("""CREATE TABLE IF NOT EXISTS articles (
            uid TEXT PRIMARY KEY, source TEXT, title TEXT, link TEXT,
            summary TEXT, published TEXT, matched TEXT, sent INTEGER DEFAULT 0)""")
        while not self.stop.is_set() or not self.in_q.empty():
            try:
                a = self.in_q.get(timeout=1)
            except queue.Empty:
                continue
            text = f"{a.title} {a.summary}".lower()
            matched = [k for k in self.keywords if k in text]
            if matched or not self.keywords:  # nessun filtro = salva tutto
                db.execute(
                    "INSERT OR IGNORE INTO articles VALUES (?,?,?,?,?,?,?,0)",
                    (a.uid, a.source, a.title, a.link, a.summary[:500],
                     a.published, ", ".join(matched)),
                )
                db.commit()
        db.close()


class WhatsAppSender:
    """Invia testo tramite l'account collegato a WhatsApp Web nel browser predefinito."""

    def __init__(self, phone, wait_time=20):
        phone = "".join(char for char in phone if char.isdigit())
        if not 8 <= len(phone) <= 15:
            raise ValueError("Inserisci un numero valido con prefisso internazionale")
        self.phone, self.wait_time = phone, wait_time

    def send(self, text):
        import pyautogui

        query = urllib.parse.urlencode({"phone": self.phone, "text": text})
        url = f"https://web.whatsapp.com/send?{query}"
        if not webbrowser.open(url, new=2):
            raise RuntimeError("Impossibile aprire WhatsApp Web nel browser predefinito")

        time.sleep(self.wait_time)
        pyautogui.press("enter")


class DigestScheduler(threading.Thread):
    """Ogni giorno all'ora indicata genera il digest con gli articoli nuovi."""

    def __init__(self, hhmm, stop, sender=None, max_items=15):
        super().__init__(name="digest", daemon=True)
        self.hour, self.minute = map(int, hhmm.split(":"))
        self.stop = stop
        self.sender, self.max_items = sender, max_items
        self.last_day = None

    def run(self):
        while not self.stop.wait(30):
            now = datetime.now()
            due = (now.hour, now.minute) >= (self.hour, self.minute)
            if due and self.last_day != now.date():
                try:
                    self.build_digest(now)
                    self.last_day = now.date()
                except Exception:
                    log.exception("Errore nella creazione/invio del digest")
                    self.stop.wait(600)  # riprova tra 10 minuti

    def whatsapp_text(self, rows, now):
        shown = rows[: self.max_items]
        lines = [f"*Digest {now:%d/%m}* - {len(rows)} notizie"]
        current = None
        for _, source, title, link, _ in shown:
            if source != current:
                lines.append(f"\n*{source}*")  # *testo* = grassetto su WhatsApp
                current = source
            lines.append(f"• {title}\n{link}")
        if len(rows) > len(shown):
            lines.append(f"\n...e altre {len(rows) - len(shown)} nel file del digest")
        return "\n".join(lines)

    def build_digest(self, now):
        db = sqlite3.connect(DB_FILE)
        rows = db.execute(
            "SELECT uid, source, title, link, matched FROM articles "
            "WHERE sent = 0 ORDER BY source, published DESC").fetchall()
        DIGEST_DIR.mkdir(exist_ok=True)
        lines = [f"# Digest del {now:%d/%m/%Y}", f"{len(rows)} articoli nuovi\n"]
        current = None
        for _, source, title, link, matched in rows:
            if source != current:
                lines.append(f"\n## {source}")
                current = source
            tag = f" _({matched})_" if matched else ""
            lines.append(f"- [{title}]({link}){tag}")
        (DIGEST_DIR / f"{now:%Y-%m-%d}.md").write_text("\n".join(lines), encoding="utf-8")
        if self.sender and rows:
            self.sender.send(self.whatsapp_text(rows, now))  # se fallisce, non segna "sent"
        db.executemany("UPDATE articles SET sent = 1 WHERE uid = ?", [(r[0],) for r in rows])
        db.commit()
        db.close()
        log.info("Digest creato con %d articoli", len(rows))


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(threadName)s] %(message)s")
    socket.setdefaulttimeout(20)  # evita thread bloccati su server lenti
    cfg = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))

    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())

    q = queue.Queue()
    wa = cfg.get("whatsapp")
    sender = None
    if wa:
        phone = getpass.getpass(
            "Numero destinatario WhatsApp con prefisso internazionale: ")
        sender = WhatsAppSender(phone, wa.get("wait_time", 20))
    threads = [
        Storage(q, cfg["keywords"], stop),
        DigestScheduler(cfg["digest_time"], stop, sender, (wa or {}).get("max_items", 15)),
    ]
    for f in cfg["feeds"]:
        threads.append(FeedWorker(f["name"], f["url"], f.get("interval", 900), q, stop))

    for t in threads:
        t.start()
    log.info("Avviati %d thread. Ctrl+C per uscire.", len(threads))

    while not stop.is_set():
        stop.wait(1)  # il main resta vivo e reattivo ai segnali
    for t in threads:
        t.join(timeout=5)
    log.info("Chiuso.")


if __name__ == "__main__":
    main()
