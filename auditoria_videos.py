import os
import requests
import time
from datetime import datetime, timedelta
import sys
import csv
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

ARC_ACCESS_TOKEN = os.getenv("ARC_ACCESS_TOKEN")
ORG_ID = os.getenv("ORG_ID")
WEBSITE_NAMES_STR = os.getenv("WEBSITE_NAMES")

API_BASE_URL = f"https://api.{ORG_ID}.arcpublishing.com/content/v4/search/published"
PAGE_SIZE = 100
MAX_RESULT_WINDOW = 10000
OUTPUT_FILENAME = "todos_los_videos_para_eliminar.csv"

def fetch_video_page(session, from_offset, website_name, query_string="type:video", size=PAGE_SIZE, extra_params=None):
    """
    Realiza una única llamada a la Content API para una página de resultados de un sitio específico.
    """
    params = {
        "website": website_name,
        "q": query_string,
        "size": size,
        "from": from_offset,
        "_sourceInclude": "_id,publish_date",
        "track_total_hits": "true"
    }

    if extra_params:
        params.update(extra_params)

    try:
        response = session.get(API_BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"Error HTTP para el sitio {website_name}: {http_err} - {response.text}")
        raise
    except requests.exceptions.RequestException as err:
        print(f"Error en la solicitud para el sitio {website_name}: {err}")
        raise


def parse_iso(s: str) -> datetime:
    if not s:
        return None
    try:
        if s.endswith("Z"):
            return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
        return datetime.fromisoformat(s)
    except Exception:
        return datetime.strptime(s.split("+")[0], "%Y-%m-%dT%H:%M:%S")


def dt_to_iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def midpoint_dt(start: datetime, end: datetime) -> datetime:
    return start + (end - start) / 2


def format_date_query(start_dt: datetime, end_dt: datetime) -> str:
    start_s = dt_to_iso(start_dt) if isinstance(start_dt, datetime) else str(start_dt)
    end_s = dt_to_iso(end_dt) if isinstance(end_dt, datetime) else str(end_dt)
    return f"type:video AND publish_date:[{start_s} TO {end_s}]"


def fetch_count_for_query(session, website_name, query_string):
    data = fetch_video_page(session, 0, website_name, query_string=query_string, size=1)
    return data.get("count", 0)


def get_extreme_publish_date(session, website_name, ascending=True):
    sort_order = "publish_date:asc" if ascending else "publish_date:desc"
    try:
        data = fetch_video_page(session, 0, website_name, query_string="type:video", size=1, extra_params={"sort": sort_order})
        elems = data.get("content_elements", [])
        if not elems:
            return None
        return elems[0].get("publish_date")
    except Exception:
        return None


def collect_videos_by_date_range(session, website_name, start_dt: datetime, end_dt: datetime):
    """
    Recursively colecta videos en el rango [start_dt, end_dt] subdividiendo cuando una ventana supera MAX_RESULT_WINDOW.
    Devuelve lista de tuplas (arc_id, website_name)
    """
    results = []

    def retrieve_window(s_dt: datetime, e_dt: datetime):
        print(f"Consultando rango {dt_to_iso(s_dt)} .. {dt_to_iso(e_dt)} para sitio '{website_name}'...")
        q = format_date_query(s_dt, e_dt)
        count = fetch_count_for_query(session, website_name, q)
        print(f"  -> count={count} (límite {MAX_RESULT_WINDOW})")
        if count == 0:
            return []
        if count > MAX_RESULT_WINDOW:
            print(f"  -> El rango supera el límite; subdividiendo...")
            mid = midpoint_dt(s_dt, e_dt)
            left = retrieve_window(s_dt, mid)
            right = retrieve_window(mid + timedelta(seconds=1), e_dt)
            return left + right
        items = []
        offset = 0
        while True:
            page = fetch_video_page(session, offset, website_name, query_string=q, size=PAGE_SIZE)
            page_items = [item.get("_id") for item in page.get("content_elements", []) if item.get("_id")]
            if not page_items:
                break
            items.extend([(vid, website_name) for vid in page_items])
            offset += len(page_items)
            print(f"    > recuperados {offset}/{page.get('count', '?')} en ventana {dt_to_iso(s_dt)}..{dt_to_iso(e_dt)}")
            if offset >= page.get("count", 0):
                break
            time.sleep(0.1)
        return items

    results = retrieve_window(start_dt, end_dt)
    return results

def get_videos_for_site(session, website_name):
    """
    Orquesta el proceso para recuperar todos los IDs de video para UN SOLO sitio.
    Devuelve una lista de tuplas (id, website_name).
    """
    print(f"\n--- Iniciando auditoría para el sitio: {website_name} ---")
    
    videos_for_this_site = []
    from_offset = 0
    total_hits = 0

    try:
        initial_data = fetch_video_page(session, from_offset, website_name)
        total_hits = initial_data.get("count", 0)

        if total_hits == 0:
            print(f"No se encontraron videos para el sitio '{website_name}'.")
            return []

        if total_hits > MAX_RESULT_WINDOW:
            print(f"El sitio '{website_name}' tiene {total_hits} elementos (> {MAX_RESULT_WINDOW}). Usando particionado por fecha.")
            try:
                min_date_str = get_extreme_publish_date(session, website_name, ascending=True)
                max_date_str = get_extreme_publish_date(session, website_name, ascending=False)
                if not (min_date_str and max_date_str):
                    print(f"No se pudieron obtener fechas extremas para '{website_name}', abortando particionado.")
                    return []

                min_dt = parse_iso(min_date_str)
                max_dt = parse_iso(max_date_str)
                cutoff_str = os.getenv("DELETE_CUTOFF_DATE", "2024-12-31T23:59:59Z")
                cutoff_dt = parse_iso(cutoff_str)
                effective_end = min(max_dt, cutoff_dt) if cutoff_dt else max_dt
                if min_dt > effective_end:
                    print(f"Todas las publicaciones de '{website_name}' son posteriores al corte {cutoff_str}. No hay nada que borrar.")
                    return []

                return collect_videos_by_date_range(session, website_name, min_dt, effective_end)
            except Exception as e:
                print(f"Error al particionar por fecha para el sitio '{website_name}': {e}")
                return []

        print(f"Se encontraron {total_hits} videos en total para '{website_name}'.")

        video_ids_on_page = [item.get("_id") for item in initial_data.get("content_elements", []) if item.get("_id")]
        videos_for_this_site.extend([(video_id, website_name) for video_id in video_ids_on_page])
        from_offset += len(video_ids_on_page)

        with tqdm(total=total_hits, desc=f"Recuperando de '{website_name}'") as pbar:
            pbar.update(len(video_ids_on_page))

            while from_offset < total_hits:
                page_data = fetch_video_page(session, from_offset, website_name)
                video_ids_on_page = [item.get("_id") for item in page_data.get("content_elements", []) if item.get("_id")]

                if not video_ids_on_page:
                    break

                videos_for_this_site.extend([(video_id, website_name) for video_id in video_ids_on_page])
                pbar.update(len(video_ids_on_page))
                from_offset += len(video_ids_on_page)
                time.sleep(0.2)

    except requests.exceptions.RequestException:
        print(f"\nLa auditoría falló para el sitio '{website_name}'. Continuando con el siguiente.")
        return []

    return videos_for_this_site

def save_ids_to_file(all_videos_data, filename):
    """
    Guarda la lista de datos de video (ID y sitio) en un archivo CSV.
    """
    try:
        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["arc_id", "website_name"])
            for video_data in all_videos_data:
                writer.writerow(video_data)
        print(f"\n¡Éxito! Se guardaron {len(all_videos_data)} IDs de video en el archivo '{filename}'.")
    except IOError as e:
        print(f"Error al escribir en el archivo '{filename}': {e}")

if __name__ == "__main__":
    if not (ARC_ACCESS_TOKEN and ORG_ID and WEBSITE_NAMES_STR):
        print("Error: Asegúrate de que las variables ARC_ACCESS_TOKEN, ORG_ID y WEBSITE_NAMES estén configuradas en tu archivo.env.")
        sys.exit(1)
    else:
        sites_to_process = [s.strip() for s in WEBSITE_NAMES_STR.split(",") if s.strip()] if WEBSITE_NAMES_STR else []
        all_videos_data = []

        print(f"Se procesarán {len(sites_to_process)} sitios.")

        with requests.Session() as session:
            session.headers.update({
                "Authorization": f"Bearer {ARC_ACCESS_TOKEN}",
                "Content-Type": "application/json"
            })
            
            for site in sites_to_process:
                videos_from_site = get_videos_for_site(session, site)
                if videos_from_site:
                    all_videos_data.extend(videos_from_site)

        if all_videos_data:
            save_ids_to_file(all_videos_data, OUTPUT_FILENAME)
        else:
            print("\nNo se encontraron videos en ninguno de los sitios especificados.")