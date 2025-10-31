import os
import requests
import time
from datetime import datetime, timedelta
import sys
import csv
from dotenv import load_dotenv
from tqdm import tqdm
import urllib.parse
import json


def parse_iso(s: str) -> datetime:
    if not s:
        return None
    try:
        if s.endswith('Z'):
            return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
        return datetime.fromisoformat(s)
    except Exception:
        # fallback: try trimming timezone
        try:
            return datetime.strptime(s.split('+')[0], "%Y-%m-%dT%H:%M:%S")
        except Exception:
            return None

load_dotenv()

ARC_ACCESS_TOKEN = os.getenv("ARC_ACCESS_TOKEN")
ORG_ID = os.getenv("ORG_ID")
WEBSITE_NAMES_STR = os.getenv("WEBSITE_NAMES")
YEARS_TO_AUDIT_STR = os.getenv("YEARS_TO_AUDIT")
REPORTS_DIR = os.getenv("REPORTS_DIR", "reports_fotos")

API_BASE_URL = f"https://api.{ORG_ID}.arcpublishing.com"
PAGE_SIZE = 100
# Usaremos el endpoint /content/v4/search/published (mismo que en auditoria_videos)
SEARCH_ENDPOINT = f"{API_BASE_URL}/content/v4/search/published"
OUTPUT_FILENAME = "reporte_uso_de_fotos.csv"
MAX_RESULT_WINDOW = 10000


def dt_to_iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def midpoint_dt(start: datetime, end: datetime) -> datetime:
    return start + (end - start) / 2


def fetch_count_for_query(session, website_name, q):
    params = {
        "website": website_name,
        "q": q,
        "size": 1,
        "track_total_hits": "true",
    }
    resp = session.get(SEARCH_ENDPOINT, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data.get("count", 0)


def collect_stories_by_date_range(session, website_name, start_dt: datetime, end_dt: datetime):
    """
    Recursively collect photo data for stories in [start_dt, end_dt], subdividing when a window
    returns more than MAX_RESULT_WINDOW hits.
    Returns list of photo_data dicts (same shape as parse_story_for_photos returns flattened).
    """
    results = []

    def retrieve_window(s_dt: datetime, e_dt: datetime):
        s_iso = dt_to_iso(s_dt)
        e_iso = dt_to_iso(e_dt)
        print(f"Consultando rango {s_iso} .. {e_iso} para sitio '{website_name}'...")
        q = f"type:story AND publish_date:[{s_iso} TO {e_iso}]"
        try:
            count = fetch_count_for_query(session, website_name, q)
        except requests.exceptions.RequestException as e:
            print(f"  Error al obtener count para rango {s_iso}..{e_iso}: {e}")
            return []

        print(f"  -> count={count} (límite {MAX_RESULT_WINDOW})")
        if count == 0:
            return []
        if count > MAX_RESULT_WINDOW:
            mid = midpoint_dt(s_dt, e_dt)
            left = retrieve_window(s_dt, mid)
            # avoid overlapping by adding 1 second to mid for the right range
            right = retrieve_window(mid + timedelta(seconds=1), e_dt)
            return left + right

        # count within window, fetch paginated
        collected = []
        params = {
            "website": website_name,
            "q": q,
            "size": PAGE_SIZE,
            "_sourceInclude": ",".join(["_id", "publish_date", "promo_items", "content_elements"]),
            "track_total_hits": "true",
            "from": 0,
        }
        offset = 0
        while offset < count:
            try:
                resp = session.get(SEARCH_ENDPOINT, params=params, timeout=60)
                resp.raise_for_status()
                data = resp.json()
            except requests.exceptions.RequestException as e:
                print(f"  Error al recuperar página para rango {s_iso}..{e_iso}: {e}")
                break

            stories = data.get("content_elements", [])
            for story in stories:
                photo_data = parse_story_for_photos(story)
                if photo_data:
                    collected.extend(photo_data)

            offset += len(stories)
            if offset >= count:
                break
            params["from"] = offset
            time.sleep(0.2)

        return collected

    results = retrieve_window(start_dt, end_dt)
    return results

def parse_story_for_photos(story_ans):
    """
    Analiza un objeto de historia en formato ANS y extrae todas las referencias a imágenes.
    Devuelve una lista de diccionarios con los detalles de cada foto encontrada.
    """
    found_photos = []
    story_id = story_ans.get("_id")
    publish_date = story_ans.get("publish_date")

    # 1. Buscar en la imagen principal (promo_items)
    promo_item = story_ans.get("promo_items", {}).get("basic")
    if promo_item and promo_item.get("type") == "image":
        photo_id = promo_item.get("_id")
        if photo_id:
            found_photos.append({
                "photo_id": photo_id,
                "story_id": story_id,
                "publish_date": publish_date,
                "location": "promo_items"
            })

    # 2. Buscar en los elementos de contenido (imágenes y galerías)
    for element in story_ans.get("content_elements", []):
        # Imagen directa
        if element.get("type") == "image":
            photo_id = element.get("_id")
            if photo_id:
                found_photos.append({
                    "photo_id": photo_id,
                    "story_id": story_id,
                    "publish_date": publish_date,
                    "location": "content_elements.image"
                })
        # Galería (que contiene imágenes)
        elif element.get("type") == "gallery":
            gallery_id = element.get("_id")
            for gallery_image in element.get("content_elements", []):
                if gallery_image.get("type") == "image":
                    photo_id = gallery_image.get("_id")
                    if photo_id:
                        found_photos.append({
                            "photo_id": photo_id,
                            "story_id": story_id,
                            "publish_date": publish_date,
                            "location": f"content_elements.gallery({gallery_id})"
                        })
    
    return found_photos


def extract_story_url(story_ans):
    """
    Intenta extraer la URL pública/canonical de una nota ANS.
    Revisa varios campos comunes y devuelve la primera url encontrada o None.
    """
    if not story_ans:
        return None
    # Campos probables
    for key in ("canonical_url", "website_url", "display_url", "url"):
        val = story_ans.get(key)
        if val:
            return val

    # Algunos ANS usan 'websites' con estructura por sitio
    websites = story_ans.get("websites")
    if isinstance(websites, dict):
        for sitek, sitev in websites.items():
            if isinstance(sitev, dict):
                wurl = sitev.get("website_url") or sitev.get("url")
                if wurl:
                    return wurl

    # fallback: intentar componer desde _id si no hay mejor opción (no ideal)
    _id = story_ans.get("_id")
    if _id:
        return None

    return None

def fetch_stories_for_year(session, website_name, year):
    """
    Obtiene todas las historias para un año y sitio específicos, extrayendo los datos de las fotos.
    Utiliza el endpoint /scan para manejar grandes volúmenes de datos de forma eficiente.
    """
    print(f"\n--- Iniciando auditoría para el sitio: '{website_name}' en el año {year} ---")
    
    all_photo_data = []

    # Construir la consulta de OpenSearch para obtener las notas de un año específico
    # Construir consulta DSL para notas del año
    gte = f"{year}-01-01T00:00:00Z"
    lte = f"{year}-12-31T23:59:59Z"
    query_dsl = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"type": "story"}},
                    {"range": {"publish_date": {"gte": gte, "lte": lte}}}
                ]
            }
        },
        "_source": ["_id", "publish_date", "promo_items", "content_elements"]
    }

    try:
        # Para compatibilidad con el endpoint 'search/published' usamos GET con paginación
        q = f"type:story AND publish_date:[{gte} TO {lte}]"
        params = {
            "website": website_name,
            "q": q,
            "size": PAGE_SIZE,
            "_sourceInclude": ",".join(["_id", "publish_date", "promo_items", "content_elements"]),
            "track_total_hits": "true",
            "from": 0
        }

        # llamada inicial para obtener count y primeros elementos
        response = session.get(SEARCH_ENDPOINT, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()
        total = data.get("count", 0)
        stories = data.get("content_elements", [])

        if total == 0 or not stories:
            print(f"No se encontraron notas para '{website_name}' en {year}.")
            return []

        # Si el total excede el límite del result window, usar particionado por fecha
        if total > MAX_RESULT_WINDOW:
            print(f"El año {year} para '{website_name}' tiene {total} notas (> {MAX_RESULT_WINDOW}). Usando particionado por fecha dentro del año.")
            start_dt = parse_iso(gte)
            end_dt = parse_iso(lte)
            if not start_dt or not end_dt:
                print(f"No se pudieron parsear las fechas del año {year}.")
                return []
            all_photo_data = collect_stories_by_date_range(session, website_name, start_dt, end_dt)
            print(f"Auditoría para '{website_name}' en {year} completada (particionado). Se encontraron {len(all_photo_data)} referencias a fotos.")
            return all_photo_data

        pbar = tqdm(total=total, desc=f"Procesando notas de {year} para '{website_name}'")

        offset = 0
        while offset < total:
            # procesar lote
            for story in stories:
                photo_data = parse_story_for_photos(story)
                if photo_data:
                    all_photo_data.extend(photo_data)

            pbar.update(len(stories))

            offset += len(stories)
            if offset >= total:
                break

            params["from"] = offset
            response = session.get(SEARCH_ENDPOINT, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
            stories = data.get("content_elements", [])
            time.sleep(0.3)

        pbar.close()
        print(f"Auditoría para '{website_name}' en {year} completada. Se encontraron {len(all_photo_data)} referencias a fotos.")

    except requests.exceptions.RequestException as e:
        print(f"\nLa auditoría falló para el sitio '{website_name}' en el año {year}: {e}")
        if 'pbar' in locals() and pbar:
            pbar.close()
        return []

    return all_photo_data


def collect_story_ids_by_date_range(session, website_name, start_dt: datetime, end_dt: datetime):
    """
    Similar to collect_stories_by_date_range but collects story ids and publish_date tuples.
    """
    results = []

    def retrieve_window(s_dt: datetime, e_dt: datetime):
        s_iso = dt_to_iso(s_dt)
        e_iso = dt_to_iso(e_dt)
        print(f"Consultando rango {s_iso} .. {e_iso} para sitio '{website_name}' (stories)...")
        q = f"type:story AND publish_date:[{s_iso} TO {e_iso}]"
        try:
            count = fetch_count_for_query(session, website_name, q)
        except requests.exceptions.RequestException as e:
            print(f"  Error al obtener count para rango {s_iso}..{e_iso}: {e}")
            return []

        if count == 0:
            return []
        if count > MAX_RESULT_WINDOW:
            mid = midpoint_dt(s_dt, e_dt)
            left = retrieve_window(s_dt, mid)
            right = retrieve_window(mid + timedelta(seconds=1), e_dt)
            return left + right

        collected = []
        params = {
            "website": website_name,
            "q": q,
            "size": PAGE_SIZE,
            "_sourceInclude": ",".join(["_id", "publish_date", "canonical_url", "website_url", "display_url", "url", "websites"]),
            "track_total_hits": "true",
            "from": 0,
        }
        offset = 0
        while offset < count:
            try:
                resp = session.get(SEARCH_ENDPOINT, params=params, timeout=60)
                resp.raise_for_status()
                data = resp.json()
            except requests.exceptions.RequestException as e:
                print(f"  Error al recuperar página para rango {s_iso}..{e_iso}: {e}")
                break

            stories = data.get("content_elements", [])
            for story in stories:
                sid = story.get("_id")
                pub = story.get("publish_date")
                url = extract_story_url(story)
                if sid:
                    collected.append((sid, pub, url))

            offset += len(stories)
            if offset >= count:
                break
            params["from"] = offset
            time.sleep(0.2)

        return collected

    results = retrieve_window(start_dt, end_dt)
    return results


def fetch_story_ids_for_year(session, website_name, year):
    """
    Retrieve all story IDs (and publish_date) for a given site and year. Uses date partitioning
    when yearly totals exceed MAX_RESULT_WINDOW to avoid from+size limits.
    Returns list of tuples (story_id, publish_date).
    """
    print(f"\n--- Obteniendo IDs de notas para el sitio: '{website_name}' en el año {year} ---")
    gte = f"{year}-01-01T00:00:00Z"
    lte = f"{year}-12-31T23:59:59Z"
    q = f"type:story AND publish_date:[{gte} TO {lte}]"

    params = {
        "website": website_name,
        "q": q,
        "size": PAGE_SIZE,
        "_sourceInclude": ",".join(["_id", "publish_date", "canonical_url", "website_url", "display_url", "url", "websites"]),
        "track_total_hits": "true",
        "from": 0
    }

    try:
        response = session.get(SEARCH_ENDPOINT, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()
        total = data.get("count", 0)
        stories = data.get("content_elements", [])

        if total == 0 or not stories:
            print(f"No se encontraron notas para '{website_name}' en {year}.")
            return []

        if total > MAX_RESULT_WINDOW:
            print(f"El año {year} para '{website_name}' tiene {total} notas (> {MAX_RESULT_WINDOW}). Usando particionado por fecha dentro del año.")
            start_dt = parse_iso(gte)
            end_dt = parse_iso(lte)
            if not start_dt or not end_dt:
                print(f"No se pudieron parsear las fechas del año {year}.")
                return []
            return collect_story_ids_by_date_range(session, website_name, start_dt, end_dt)

        # regular pagination
        results = []
        offset = 0
        pbar = tqdm(total=total, desc=f"Recuperando IDs de notas {year} para '{website_name}'")
        while offset < total:
            for story in stories:
                sid = story.get("_id")
                pub = story.get("publish_date")
                url = extract_story_url(story)
                if sid:
                    results.append((sid, pub, url))

            pbar.update(len(stories))
            offset += len(stories)
            if offset >= total:
                break
            params["from"] = offset
            response = session.get(SEARCH_ENDPOINT, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
            stories = data.get("content_elements", [])
            time.sleep(0.2)

        pbar.close()
        return results

    except requests.exceptions.RequestException as e:
        print(f"\nLa recuperación de IDs falló para el sitio '{website_name}' en el año {year}: {e}")
        return []


def fetch_all_images_for_site(session, website_name):
    """
    Recupera todos los assets de tipo image para un sitio (usa /scan para grandes volúmenes).
    Devuelve lista de dicts con al menos 'photo_id' y opcionalmente 'url'.
    """
    print(f"Obteniendo listados de imágenes para sitio '{website_name}'...")
    images = []

    # Usar search/published con paginación (GET)
    q = "type:image"
    params = {
        "website": website_name,
        "q": q,
        "size": PAGE_SIZE,
        "_sourceInclude": ",".join(["_id", "display_url", "url"]),
        "track_total_hits": "true",
        "from": 0
    }

    try:
        resp = session.get(SEARCH_ENDPOINT, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        total = data.get("count", 0)
        elems = data.get("content_elements", [])

        offset = 0
        while offset < total:
            for e in elems:
                pid = e.get("_id")
                url = e.get("display_url") or e.get("url")
                images.append({"photo_id": pid, "url": url, "website_name": website_name})

            offset += len(elems)
            if offset >= total:
                break

            params["from"] = offset
            resp = session.get(SEARCH_ENDPOINT, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            elems = data.get("content_elements", [])
            time.sleep(0.2)

    except requests.exceptions.RequestException as e:
        print(f"Error al obtener imágenes para '{website_name}': {e}")
        return images

    print(f"Se recuperaron {len(images)} imágenes para '{website_name}'.")
    return images


def get_extreme_publish_date(session, website_name, ascending=True):
    """Devuelve la publish_date (ISO) más antigua (ascending=True) o más reciente (False) para stories en el sitio."""
    sort_order = "publish_date:asc" if ascending else "publish_date:desc"
    query_dsl = {
        "query": {"term": {"type": "story"}},
        "_source": ["_id", "publish_date"]
    }
    try:
        params = {"website": website_name, "q": "type:story", "size": 1, "sort": sort_order, "from": 0, "_sourceInclude": ",".join(["_id", "publish_date"]) }
        resp = session.get(SEARCH_ENDPOINT, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        elems = data.get("content_elements", [])
        if not elems:
            return None
        return elems[0].get("publish_date")
    except Exception:
        return None

def save_data_to_csv(all_data, filename):
    """
    Guarda la lista de diccionarios de datos en un archivo CSV.
    """
    if not all_data:
        return

    try:
        # Asegurar directorio destino
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            # Las claves del primer diccionario se usarán como cabeceras
            fieldnames = list(all_data[0].keys())
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            writer.writerows(all_data)

        print(f"\n¡Éxito! Se guardaron {len(all_data)} registros en el archivo '{filename}'.")
    except IOError as e:
        print(f"Error al escribir en el archivo '{filename}': {e}")

if __name__ == "__main__":
    # Volver a leer las variables de entorno (en caso de que el usuario haya creado/actualizado .env recientemente)
    ARC_ACCESS_TOKEN = os.getenv("ARC_ACCESS_TOKEN")
    ORG_ID = os.getenv("ORG_ID")
    WEBSITE_NAMES_STR = os.getenv("WEBSITE_NAMES")
    YEARS_TO_AUDIT_STR = os.getenv("YEARS_TO_AUDIT")
    REPORTS_DIR = os.getenv("REPORTS_DIR", "reports_fotos")

    missing = [n for n, v in (
        ("ARC_ACCESS_TOKEN", ARC_ACCESS_TOKEN),
        ("ORG_ID", ORG_ID),
        ("WEBSITE_NAMES", WEBSITE_NAMES_STR),
        ("YEARS_TO_AUDIT", YEARS_TO_AUDIT_STR),
    ) if not v]

    if missing:
        print("Error: Faltan variables de entorno obligatorias: {}".format(", ".join(missing)))
        print("Por favor crea un archivo '.env' en el directorio del script con las siguientes variables, por ejemplo:")
        print()
        print("ARC_ACCESS_TOKEN=tu_token_aqui")
        print("ORG_ID=tu_org_id")
        print("WEBSITE_NAMES=site1,site2")
        print("YEARS_TO_AUDIT=2021-")
        print()
        sys.exit(1)
    
    # Parsear sitios
    sites_to_process = [s.strip() for s in WEBSITE_NAMES_STR.split(",") if s.strip()] if WEBSITE_NAMES_STR else []

    # Parsear años: acepta formatos "2022,2023,2024" o rangos "2018-2024"
    years_to_process = []
    if YEARS_TO_AUDIT_STR:
        parts = [p.strip() for p in YEARS_TO_AUDIT_STR.split(",") if p.strip()]
        for part in parts:
            if '-' in part:
                start, end = part.split('-', 1)
                # Support open-ended ranges like '2021-' (meaning from 2021 backwards until site min)
                if start and not end:
                    years_to_process.append(part)
                    continue
                try:
                    start_y = int(start)
                    end_y = int(end)
                    years_to_process.extend([str(y) for y in range(start_y, end_y + 1)])
                except ValueError:
                    # if parsing failed, keep the raw part so downstream logic may handle it
                    years_to_process.append(part)
            else:
                years_to_process.append(part)

    # resultados acumulados
    all_results = []

    print(f"Se procesarán {len(sites_to_process)} sitios para los años: {', '.join(years_to_process)}.")

    with requests.Session() as session:
        session.headers.update({
            "Authorization": f"Bearer {ARC_ACCESS_TOKEN}",
            "Content-Type": "application/json"
        })
        
        # Bucle principal para iterar sobre cada sitio y cada año
        for site in sites_to_process:
            # preparar carpeta por sitio
            site_dir = os.path.join(REPORTS_DIR, site)
            os.makedirs(site_dir, exist_ok=True)

            # Interpretar years_to_process para este sitio (soporta rango abierto '2021-')
            resolved_years = []
            for part in years_to_process:
                if part.endswith("-"):
                    # ejemplo '2021-' -> desde 2021 hacia atrás hasta el año mínimo del sitio
                    try:
                        start_y = int(part[:-1])
                    except ValueError:
                        continue
                    # obtener año mínimo del sitio
                    min_date = get_extreme_publish_date(session, site, ascending=True)
                    if not min_date:
                        # no hay datos, saltar
                        continue
                    min_y = int(parse_iso(min_date).year)
                    # años desde start_y hacia min_y
                    for y in range(start_y, min_y - 1, -1):
                        resolved_years.append(str(y))
                else:
                    resolved_years.append(part)

            # eliminar duplicados y ordenar descendente (start year -> older)
            resolved_years = sorted(set(resolved_years), reverse=True)

            for year in resolved_years:
                # Obtener solo los IDs de las notas para este sitio y año
                story_tuples = fetch_story_ids_for_year(session, site, int(year))
                if not story_tuples:
                    print(f"No se encontraron notas para {site} en {year}.")
                    continue

                # Guardar los IDs de las notas con su fecha de publicación y ordenados por fecha
                notas_fn = os.path.join(site_dir, f"notas_publicadas_{site}_{year}.csv")
                try:
                    # ordenar por publish_date (ascendente). Si falta la fecha, la colocamos al final.
                    def _parse_pub(t):
                        # t puede ser (sid, pub) o (sid, pub, url)
                        pub = t[1] if len(t) > 1 else None
                        dt = parse_iso(pub)
                        return dt or datetime.min

                    sorted_tuples = sorted(story_tuples, key=_parse_pub)

                    with open(notas_fn, "w", newline="", encoding="utf-8") as f:
                        writer = csv.writer(f)
                        # incluir columna url si está presente en las tuplas
                        writer.writerow(["story_id", "publish_date", "url"])  # header
                        for t in sorted_tuples:
                            if len(t) >= 3:
                                sid, pub, url = t[0], t[1], t[2]
                            else:
                                sid, pub = t[0], t[1]
                                url = ""
                            writer.writerow([sid, pub, url])
                    print(f"Guardadas {len(sorted_tuples)} notas en '{notas_fn}' (ordenadas por fecha).")
                except IOError as e:
                    print(f"Error al escribir archivo '{notas_fn}': {e}")

    if all_results:
        save_data_to_csv(all_results, OUTPUT_FILENAME)
    else:
        print("\nNo se encontraron referencias a fotos en ninguno de los sitios y años especificados.")