#!/usr/bin/env python3
"""
Extraction des résultats VISIBLES depuis une liste d'URLs de recherche
Google Maps déjà prêtes — SANS API.

Contrairement à la version précédente, ce script ne construit plus les URLs
lui-même (ville + type d'activité) : il lit une liste d'URLs toutes faites
dans urls.csv (une par ligne, ou avec un en-tête "url"), et scrape chacune.

⚠️ Important :
- Toujours pas l'API officielle Google : automatisation de navigateur.
  Plus d'onglets en parallèle = plus de requêtes/minute vers Google =
  risque de blocage/captcha (page /sorry/) plus élevé.
- Si Google bloque une URL (captcha), elle est marquée "échec" (et non
  "traité") dans urls.csv : elle ne sera PAS retentée automatiquement au
  prochain lancement, pour ne pas boucler indéfiniment contre le même mur.
  Si tu résous le blocage (proxy, pause plus longue, autre réseau...) et
  veux retenter ces lignes, remets leur colonne "statut" à vide à la main.

Installation :
    pip install playwright
    playwright install chromium

Entrée  : urls.csv              (une URL Google Maps par ligne ; colonne
                                  "statut" ajoutée/mise à jour automatiquement)
Sortie  : resultats_visibles.csv (écriture en temps réel, une ligne par établissement)

Lancement :
    python scrape_wellness_rapide.py
"""

import asyncio
import csv
import os
import re
import sys
import random
import subprocess
import unicodedata
import urllib.parse
from datetime import datetime
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# ============================================================
# CONFIGURATION
# ============================================================

URLS_CSV = "urls.csv"
RESULTATS_CSV = "resultats_visibles.csv"

COLONNES_RESULTATS = [
    "Nom de l'établissement", "Note", "Nombre d'avis", "Catégorie",
    "Adresse / résumé", "Ville (déduite de l'URL)", "Activité (déduite de l'URL)",
    "URL de recherche", "URL Google Maps",
]

HEADLESS = True
MAX_ETABLISSEMENTS_PAR_REQUETE = 25

# ============================================================
# Réglages que tu peux ajuster toi-même
# ============================================================

# Nombre d'URLs traitées EN MÊME TEMPS (chacune dans son propre onglet).
# Plus haut = plus rapide mais plus de risque de blocage Google.
CONCURRENCE = 2

# Commit + push automatique de urls.csv et resultats_visibles.csv tous les
# N URLs traitées (0 = désactivé, uniquement le commit final du workflow).
GIT_COMMIT_TOUTES_LES = 20

# Pause longue tous les N URLs traitées, pour souffler un peu et alléger la
# charge envoyée à Google (0 = désactivé).
PAUSE_LONGUE_TOUTES_LES = 50
PAUSE_LONGUE_SECONDES = 120

PAUSE_COURTE = (0.2, 0.4)
PAUSE_ENTRE_URLS = (0.6, 1.2)   # par onglet, entre deux URLs qu'il traite
PAUSE_SCROLL = (0.35, 0.6)

# ============================================================

RATING_REGEX = re.compile(r"^\d[.,]\d$")
REVIEWS_REGEX = re.compile(r"^\(([\d\s.,]+)\)$")
COORD_REGEX = re.compile(r"/@(-?\d+\.\d+),(-?\d+\.\d+),(\d+(?:\.\d+)?)z")

# Préfixes d'activité reconnus dans les URLs, pour renseigner la colonne
# "Activité" du CSV de sortie (uniquement informatif, n'affecte pas le scraping).
ACTIVITES_CONNUES = ["hôtel spa", "hotel spa", "centre thermal", "parc aquatique"]

RESSOURCES_A_BLOQUER = {"image", "media", "font", "stylesheet"}
DOMAINES_A_BLOQUER = (
    "google-analytics.com", "googletagmanager.com", "doubleclick.net",
    "googlesyndication.com", "fonts.gstatic.com", "fonts.googleapis.com",
    "gstatic.com/og", "play.google.com/log",
)


async def bloquer_ressources_inutiles(route):
    req = route.request
    if req.resource_type in RESSOURCES_A_BLOQUER:
        return await route.abort()
    url = req.url.lower()
    if any(d in url for d in DOMAINES_A_BLOQUER):
        return await route.abort()
    await route.continue_()


async def pause(bornes):
    await asyncio.sleep(random.uniform(*bornes))


# ============================================================
# Lecture / écriture urls.csv
# ============================================================

def read_text_any_encoding(path):
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            with open(path, encoding=enc) as f:
                return f.read(), enc
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Impossible de décoder {path} avec les encodages testés.")


def load_urls(path):
    """Accepte soit une simple liste d'URLs (une par ligne, sans en-tête),
    soit un CSV avec en-tête (ex: 'url' ou 'url,statut' généré par un run
    précédent de ce script)."""
    content, enc_used = read_text_any_encoding(path)
    print(f"[i] {path} lu avec l'encodage : {enc_used}")
    lignes = [l for l in content.splitlines() if l.strip()]
    if not lignes:
        return [], ["url", "statut"], "url"

    premiere = lignes[0].strip()
    if premiere.lower().startswith("http"):
        col_url = "url"
        donnees = lignes
    else:
        entete = next(csv.reader([premiere]))
        col_url = (entete[0].strip() if entete and entete[0].strip() else "url")
        donnees = lignes[1:]

    rows = []
    for parts in csv.reader(donnees):
        if not parts or not parts[0].strip():
            continue
        url = parts[0].strip()
        statut = parts[1].strip() if len(parts) > 1 else ""
        rows.append({col_url: url, "statut": statut})

    return rows, [col_url, "statut"], col_url


def save_urls(path, rows, fieldnames, max_tentatives=5):
    """Plusieurs tentatives : sur Windows, le fichier peut être temporairement
    verrouillé (Excel ouvert, antivirus). On réessaie au lieu de planter."""
    import time as _time
    for tentative in range(1, max_tentatives + 1):
        try:
            with open(path, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            return True
        except PermissionError:
            if tentative == 1:
                print(f"     [!] {path} semble ouvert dans un autre programme. "
                      f"Nouvelle tentative dans 3s...")
            _time.sleep(3)
    print(f"     [!!] Impossible d'écrire {path} après {max_tentatives} tentatives. "
          f"Cette URL sera retraitée au prochain lancement.")
    return False


def mark_done_url(rows, fieldnames, col_url, url_value, path, statut_label):
    for r in rows:
        if r.get(col_url, "").strip() == url_value.strip():
            r["statut"] = f"{statut_label} ({datetime.now().strftime('%Y-%m-%d %H:%M')})"
    save_urls(path, rows, fieldnames)


def git_commit_push(traitees):
    """Commit + push urls.csv et resultats_visibles.csv en cours de route.
    Amende (remplace) le dernier commit s'il a déjà été fait par ce même
    processus de scraping, pour ne pas faire grossir l'historique git."""
    try:
        subprocess.run(["git", "add", URLS_CSV, RESULTATS_CSV], check=False)

        diff = subprocess.run(["git", "diff", "--staged", "--quiet"])
        if diff.returncode == 0:
            print(f"     [git] rien de nouveau à committer ({traitees} URLs traitées)")
            return

        last_author = subprocess.run(
            ["git", "log", "-1", "--pretty=%an"],
            capture_output=True, text=True
        ).stdout.strip()

        if last_author == "github-actions[bot]":
            subprocess.run(
                ["git", "commit", "--amend", "-m", "Mise à jour résultats scraping [skip ci]"],
                check=False,
            )
            subprocess.run(["git", "push", "--force"], check=False)
        else:
            subprocess.run(
                ["git", "commit", "-m", "Mise à jour résultats scraping [skip ci]"],
                check=False,
            )
            subprocess.run(["git", "push"], check=False)

        print(f"     [git] commit/push effectué ({traitees} URLs traitées)")
    except Exception as e:
        print(f"     [!] [git] échec commit/push : {e}")


# ============================================================
# Déduction Ville / Activité depuis l'URL (informatif uniquement)
# ============================================================

def normaliser(texte):
    t = unicodedata.normalize("NFKD", texte).encode("ascii", "ignore").decode("ascii")
    return t.lower()


def parser_label_depuis_url(url):
    """Extrait un couple (activité, ville) lisible depuis une URL du type
    .../maps/search/hotel+spa+Ville%2BFrance/ — best effort, uniquement pour
    remplir les colonnes du CSV de sortie. N'affecte jamais le scraping
    lui-même (qui utilise toujours l'URL fournie telle quelle)."""
    try:
        chemin = urllib.parse.urlsplit(url).path
        segment = chemin.split("/maps/search/")[-1].strip("/").split("/")[0]
        label = urllib.parse.unquote(segment.replace("+", " "))
        label = label.replace("+", " ")
        label = " ".join(label.split())
    except Exception:
        return "", url

    label_norm = normaliser(label)
    activite, ville = "", label
    for connue in ACTIVITES_CONNUES:
        if label_norm.startswith(normaliser(connue)):
            activite = connue
            ville = label[len(connue):].strip()
            break

    ville = re.sub(r"\s+France$", "", ville, flags=re.IGNORECASE).strip()
    return activite, (ville or label)


# ============================================================
# Extraction depuis la liste de résultats (sans ouvrir les fiches)
# ============================================================

def clean_text(text):
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def parse_card_text(raw_text):
    lignes = [clean_text(l) for l in raw_text.split("\n") if clean_text(l)]

    nom = lignes[0] if lignes else ""
    note = ""
    avis = ""
    reste = []

    for l in lignes[1:]:
        if not note and RATING_REGEX.match(l):
            note = l.replace(",", ".")
            continue
        m = REVIEWS_REGEX.match(l)
        if not avis and m:
            avis = m.group(1).replace(" ", "").replace("\u202f", "")
            continue
        reste.append(l)

    categorie = ""
    adresse = ""
    if reste:
        premiere_ligne_utile = reste[0]
        parts = [p.strip() for p in premiere_ligne_utile.split("·")]
        if parts:
            categorie = parts[0]
            adresse = " · ".join(parts[1:]) if len(parts) > 1 else ""

    return nom, note, avis, categorie, adresse


async def extraire_resultats_du_panneau(results_panel, ville, activite, url_recherche, write_row):
    """Scrolle le panneau de résultats et écrit chaque établissement trouvé.
    Retourne le nombre d'établissements extraits."""
    previous_count = 0
    stagnant_rounds = 0
    while stagnant_rounds < 2:
        cards = await results_panel.locator("a.hfpxzc").all()
        if len(cards) >= MAX_ETABLISSEMENTS_PAR_REQUETE:
            break
        if len(cards) == previous_count:
            stagnant_rounds += 1
        else:
            stagnant_rounds = 0
        previous_count = len(cards)

        await results_panel.evaluate("(el) => el.scrollBy(0, 800)")
        await pause(PAUSE_SCROLL)

    cards = (await results_panel.locator("a.hfpxzc").all())[:MAX_ETABLISSEMENTS_PAR_REQUETE]
    print(f"     [{ville or url_recherche}] {len(cards)} établissement(s) repérés pour '{activite}'")

    for card_link in cards:
        href = await card_link.get_attribute("href")
        if not href:
            continue
        try:
            container = card_link.locator("xpath=..")
            raw_text = await container.inner_text(timeout=1500)
        except Exception:
            continue

        nom, note, avis, categorie, adresse = parse_card_text(raw_text)
        if not nom:
            continue

        row = [nom, note, avis, categorie, adresse, ville, activite, url_recherche, href]
        await write_row(row)

    return len(cards)


async def tenter_recherche_alternative(page, ville, activite, url_recherche, write_row):
    """Filet de secours quand la recherche normale ne renvoie aucun panneau
    de résultats. On relance une recherche générique (hôtels/campings/
    résidences de tourisme) centrée sur les coordonnées que Google a déjà
    affichées dans l'URL pour cette zone. Retourne True si des résultats ont
    été trouvés et écrits."""
    m = COORD_REGEX.search(page.url)
    if not m:
        print(f"     [alt] pas de coordonnées dans l'URL ({page.url}), recherche alternative impossible")
        return False

    lat, lng, zoom = m.groups()
    url_alt = (
        f"https://www.google.com/maps/search/hotels,+campings,+r%C3%A9sidences+de+tourisme/"
        f"@{lat},{lng},{zoom}z/data=!4m2!2m1!6e3?authuser=0&entry=ttu"
    )
    print(f"     [alt] tentative recherche alternative -> {url_alt}")

    try:
        await page.goto(url_alt, timeout=15000)
    except PWTimeout:
        print(f"     [alt] timeout au chargement de l'URL alternative")
        return False

    await pause(PAUSE_COURTE)

    results_panel = page.locator('div[role="feed"]').first
    try:
        await results_panel.wait_for(timeout=6000)
    except PWTimeout:
        print(f"     [alt] toujours pas de panneau, même en recherche alternative")
        return False

    activite_alt = f"{activite} (recherche alternative)" if activite else "(recherche alternative)"
    nb = await extraire_resultats_du_panneau(results_panel, ville, activite_alt, url_recherche, write_row)
    return nb > 0


async def scrape_url(page, url, ville, activite, write_row):
    label = f"{activite} à {ville}".strip() if activite or ville else url
    print(f"  -> {label}")

    try:
        await page.goto(url, timeout=15000)
    except PWTimeout:
        print(f"     [!] [{label}] timeout au chargement, on passe")
        return False

    await pause(PAUSE_COURTE)

    # Blocage anti-bot Google (page de captcha) : à distinguer clairement
    # d'un vrai 0 résultat, ce n'est pas la même cause ni le même remède.
    if "/sorry/" in page.url:
        print(f"     [!] [{label}] bloqué par Google (page /sorry/, captcha anti-bot)")
        return False

    # Cas particulier : quand un seul établissement matche très fort, Google
    # saute directement sur sa fiche au lieu d'afficher une liste.
    if "/maps/place/" in page.url:
        try:
            nom = clean_text(await page.locator("h1").first.inner_text(timeout=3000))
        except Exception:
            nom = ""
        if nom:
            note, avis = "", ""
            try:
                bloc = await page.locator('div[jsaction*="pane.rating"]').first.inner_text(timeout=2000)
                for l in [clean_text(x) for x in bloc.split("\n") if clean_text(x)]:
                    if not note and RATING_REGEX.match(l):
                        note = l.replace(",", ".")
                    m = REVIEWS_REGEX.match(l)
                    if not avis and m:
                        avis = m.group(1).replace(" ", "").replace("\u202f", "")
            except Exception:
                pass
            row = [nom, note, avis, "", "", ville, activite, url, page.url]
            await write_row(row)
            print(f"     [{label}] 1 établissement repéré (redirection directe)")
            return True
        return False

    results_panel = page.locator('div[role="feed"]').first
    try:
        await results_panel.wait_for(timeout=5000)
    except PWTimeout:
        try:
            await results_panel.wait_for(timeout=6000)
        except PWTimeout:
            trouve = await tenter_recherche_alternative(page, ville, activite, url, write_row)
            if not trouve:
                print(f"     [!] [{label}] pas de panneau de résultats")
            return trouve

    nb = await extraire_resultats_du_panneau(results_panel, ville, activite, url, write_row)
    return nb > 0


async def traiter_url(context, row, col_url, write_row, sem):
    async with sem:
        page = await context.new_page()
        page.set_default_timeout(5000)
        url = row[col_url].strip()
        activite, ville = parser_label_depuis_url(url)
        try:
            succes = await scrape_url(page, url, ville, activite, write_row)
        except Exception as e:
            print(f"  [!!] erreur inattendue sur {url} : {e}")
            succes = False
        finally:
            await page.close()
        await pause(PAUSE_ENTRE_URLS)
        return row, succes


async def main():
    if not os.path.exists(URLS_CSV):
        print(f"[!] Fichier introuvable : {URLS_CSV}")
        sys.exit(1)

    rows, fieldnames, col_url = load_urls(URLS_CSV)
    a_traiter = [r for r in rows
                 if r.get(col_url, "").strip() and not r.get("statut", "")]

    print(f"{len(a_traiter)} URL(s) à traiter sur {len(rows)} au total")
    print(f"Concurrence : {CONCURRENCE} onglet(s) en parallèle")

    file_exists = os.path.exists(RESULTATS_CSV)
    out_f = open(RESULTATS_CSV, "a", newline="", encoding="utf-8-sig")
    csv_writer = csv.writer(out_f)
    if not file_exists:
        csv_writer.writerow(COLONNES_RESULTATS)
        out_f.flush()

    write_lock = asyncio.Lock()
    csv_urls_lock = asyncio.Lock()

    async def write_row(row):
        async with write_lock:
            csv_writer.writerow(row)
            out_f.flush()

    sem = asyncio.Semaphore(CONCURRENCE)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=HEADLESS,
            args=[
                "--disable-gpu",
                "--disable-extensions",
                "--disable-background-networking",
                "--disable-sync",
                "--disable-translate",
                "--disable-default-apps",
                "--mute-audio",
                "--no-first-run",
            ],
        )
        context = await browser.new_context(
            locale="fr-FR", viewport={"width": 1366, "height": 900}
        )
        await context.route("**/*", bloquer_ressources_inutiles)

        consent_page = await context.new_page()
        try:
            await consent_page.goto("https://www.google.com/maps?hl=fr", timeout=15000)
            consent_btn = consent_page.locator("button:has-text('Tout accepter')").first
            if await consent_btn.is_visible(timeout=2000):
                await consent_btn.click()
        except Exception:
            pass
        await consent_page.close()

        taches = [traiter_url(context, row, col_url, write_row, sem) for row in a_traiter]

        traitees = 0
        for coro in asyncio.as_completed(taches):
            row, succes = await coro
            statut_label = "traité" if succes else "échec"
            async with csv_urls_lock:
                mark_done_url(rows, fieldnames, col_url, row[col_url], URLS_CSV, statut_label)
            print(f"  [{'OK' if succes else 'KO'}] {row[col_url]} marquée '{statut_label}'")

            traitees += 1
            print(f"--- Progression : {traitees}/{len(a_traiter)} URLs ---")

            if GIT_COMMIT_TOUTES_LES and traitees % GIT_COMMIT_TOUTES_LES == 0:
                out_f.flush()
                git_commit_push(traitees)

            if PAUSE_LONGUE_TOUTES_LES and traitees % PAUSE_LONGUE_TOUTES_LES == 0:
                print(f"     [pause] {PAUSE_LONGUE_SECONDES}s de pause après {traitees} URLs traitées...")
                await asyncio.sleep(PAUSE_LONGUE_SECONDES)

        await browser.close()

    out_f.close()
    print(f"\nTerminé. Résultats dans {RESULTATS_CSV}")


if __name__ == "__main__":
    asyncio.run(main())