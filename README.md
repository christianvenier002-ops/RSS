# RSS

## Installazione

```powershell
.\venv\Scripts\python.exe -m pip install feedparser pyautogui
.\venv\Scripts\python.exe rss_digest.py
```

## Invio WhatsApp

Ogni volta che avvii lo script dal venv, inserisci il destinatario quando richiesto,
includendo il prefisso internazionale. L'inserimento non viene mostrato a schermo
e il numero resta solo in memoria per quella esecuzione: non viene salvato in
`config.json` né nei file del progetto. Il digest viene inviato tramite WhatsApp
Web nel browser predefinito: accedi a
[WhatsApp Web](https://web.whatsapp.com) con l'account dal quale vuoi inviare.
Non servono CallMeBot, chiavi API o un account WhatsApp Business.

Il computer deve essere acceso e la sessione WhatsApp Web già collegata quando
scatta `digest_time`. Durante l'invio il browser viene portato in primo piano e
il tasto Invio viene premuto per spedire il messaggio. Se WhatsApp Web impiega
più tempo a caricarsi, aumenta `whatsapp.wait_time` in `config.json` (secondi;
predefinito: `20`).