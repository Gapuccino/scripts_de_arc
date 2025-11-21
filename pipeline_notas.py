import os
import requests
import time
import sys
import csv
import argparse
import threading
import random
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

# Cargar variables de entorno desde el archivo.env
load_dotenv()

# --- Constantes de Configuración ---
# IMPORTANTE: Asegúrate de que este token sea de "Acceso Total" (All-access)
ARC_ACCESS_TOKEN = os.getenv("ARC_ACCESS_TOKEN")
ORG_ID = os.getenv("ORG_ID")

# URL base de la Draft API, usada para modificar contenido
DRAFT_API_BASE_URL = f"https://api.{ORG_ID}.arcpublishing.com/draft/v1"

# Configura la sesión de requests para reutilizar la conexión y los encabezados
SESSION = requests.Session()
SESSION.headers.update({
    "Authorization": f"Bearer {ARC_ACCESS_TOKEN}",
    "Content-Type": "application/json",
    "Arc-Priority": "ingestion",  # Header para contenido histórico/migrado
})

# Rate Limiting Global - Optimizado para 999 req/min con margen de seguridad
MAX_WORKERS = 12  # Balanceado para evitar 429s
TARGET_RPS = 13.0 # 13 req/s = 780 req/min (margen amplio de seguridad)

class TokenBucket:
    def __init__(self, tokens, fill_rate):
        self.capacity = float(tokens)
        self._tokens = float(tokens)
        self.fill_rate = float(fill_rate)
        self.timestamp = time.time()
        self.lock = threading.Lock()

    def consume(self, tokens=1):
        with self.lock:
            now = time.time()
            # Rellenar tokens basado en el tiempo transcurrido
            delta = self.fill_rate * (now - self.timestamp)
            self._tokens = min(self.capacity, self._tokens + delta)
            self.timestamp = now
            
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

# Bucket global: Capacidad de ráfaga 18, rellena a 13 req/s
RATE_LIMITER = TokenBucket(tokens=18, fill_rate=TARGET_RPS)

def wait_for_token():
    """Bloquea el hilo hasta que haya token disponible para hacer request."""
    while not RATE_LIMITER.consume():
        time.sleep(0.02)  # Chequeo más frecuente para mejor throughput

def make_request_with_retry(method, url, **kwargs):
    """Wrapper robusto para requests con Rate Limiting y Backoff Exponencial optimizado."""
    retries = 0
    max_retries = 8  # Aumentado para manejar mejor errores transitorios
    backoff = 1.0  # Backoff inicial más bajo
    last_error = "Unknown"
    
    while retries < max_retries:
        # 1. Esperar turno en el rate limiter global
        wait_for_token()
        
        try:
            response = SESSION.request(method, url, **kwargs)
            
            # 2. Manejo de 429 (Too Many Requests) - Optimizado
            if response.status_code == 429:
                last_error = f"HTTP 429 (Rate Limit)"
                # Chequear si hay header Retry-After
                retry_after = response.headers.get('Retry-After')
                if retry_after:
                    try:
                        sleep_time = float(retry_after)
                    except ValueError:
                        sleep_time = backoff + random.uniform(0, 0.5)
                else:
                    # Backoff más agresivo para 429 pero con jitter pequeño
                    sleep_time = min(backoff, 5.0) + random.uniform(0, 0.5)
                
                if retries < 3:
                    # Solo mostrar mensaje en primeros reintentos
                    print(f"  [429] Rate limit. Esperando {sleep_time:.2f}s... (intento {retries+1}/{max_retries})")
                
                time.sleep(sleep_time)
                backoff *= 1.5  # Crecimiento más moderado: 1, 1.5, 2.25, 3.37...
                retries += 1
                continue
            
            # 3. Manejo de errores de servidor (5xx)
            if 500 <= response.status_code < 600:
                last_error = f"HTTP {response.status_code} (Server Error)"
                sleep_time = backoff + random.uniform(0, 0.5)
                if retries < 3:
                    print(f"  [{response.status_code}] Error servidor. Reintentando en {sleep_time:.2f}s... (intento {retries+1}/{max_retries})")
                time.sleep(sleep_time)
                backoff *= 1.5
                retries += 1
                continue

            return response

        except requests.exceptions.RequestException as e:
            last_error = f"Exception: {e}"
            if retries < 3:
                print(f"  Error de conexión: {e}. Reintentando... (intento {retries+1}/{max_retries})")
            time.sleep(backoff)
            backoff *= 1.5
            retries += 1
            
    # Si llegamos aquí, fallaron todos los intentos
    # Lanzamos una excepción para que el caller sepa exactamente qué pasó
    raise Exception(f"Fallaron todos los reintentos para {url}. Último error: {last_error}")


def extract_site_from_filename(path):
    """Try to extract site name from a filename like notas_publicadas_<site>_<year>.csv"""
    name = os.path.basename(path)
    if name.startswith("notas_publicadas_"):
        rest = name[len("notas_publicadas_"):]
        parts = rest.split("_")
        if parts:
            return parts[0]
    # fallback: try second token when splitting by _
    parts = name.split("_")
    if len(parts) >= 3:
        return parts[2]
    return None


def load_rows_from_csv(csv_path):
    """Yield dicts with keys story_id, publish_date, url, site (site may be inferred from filename)."""
    rows = []
    site_guess = extract_site_from_filename(csv_path)
    with open(csv_path, newline='', encoding='utf-8') as f:
        # Intentar detectar si tiene header
        first_line = f.readline()
        f.seek(0)
        
        # Si la primera línea parece un ID (26 caracteres alfanuméricos), no tiene header
        first_value = first_line.split(',')[0].strip()
        has_header = not (len(first_value) == 26 and first_value.isalnum())
        
        if has_header:
            reader = csv.DictReader(f)
            for r in reader:
                story_id = r.get('story_id') or r.get('id') or r.get('_id')
                pub = r.get('publish_date') or r.get('publish') or r.get('date')
                url = r.get('url') or r.get('canonical_url') or r.get('website_url')
                site = r.get('site') or r.get('website') or site_guess
                if story_id:
                    rows.append({'story_id': story_id, 'publish_date': pub, 'url': url, 'site': site})
        else:
            # CSV sin header: columnas son ID, fecha, URL
            reader = csv.reader(f)
            for row in reader:
                if row and len(row) > 0:
                    story_id = row[0].strip()
                    pub = row[1].strip() if len(row) > 1 else None
                    url = row[2].strip() if len(row) > 2 else None
                    if story_id:
                        rows.append({'story_id': story_id, 'publish_date': pub, 'url': url, 'site': site_guess})
    return rows

def get_circulations(story_id):
    """Obtiene la lista de sitios web donde una nota está circulada."""
    url = f"{DRAFT_API_BASE_URL}/story/{story_id}/circulation"
    
    try:
        response = make_request_with_retry("GET", url, timeout=10)
    except Exception as e:
        print(f"  [{story_id}] ERROR FATAL en get_circulations: {e}")
        return None
    
    try:
        if response.status_code == 404:
            return []

        if not response.ok:
            print(f"  [{story_id}] Error HTTP {response.status_code} en get_circulations. Body: {response.text[:100]}")
        
        response.raise_for_status()
        data = response.json()
        # normalize possible shapes: list of circulations or dict wrapper
        circulations = None
        if isinstance(data, list):
            circulations = data
        elif isinstance(data, dict):
            # common wrappers
            for key in ("circulations", "items", "results", "data"):
                v = data.get(key)
                if isinstance(v, list):
                    circulations = v
                    break
            # if nothing found, maybe the dict itself represents a single circulation
            if circulations is None:
                # check if dict looks like a circulation (has website_id)
                if data.get("website_id"):
                    circulations = [data]
                else:
                    # fallback: scan dict values for lists of dicts
                    for v in data.values():
                        if isinstance(v, list) and v and isinstance(v[0], dict):
                            circulations = v
                            break

        if not circulations:
            return []

        website_ids = []
        for circ in circulations:
            if isinstance(circ, dict):
                wid = circ.get("website_id")
                if wid:
                    website_ids.append(wid)
        return website_ids
    except Exception as e:
        print(f"  [{story_id}] EXCEPCION procesando circulaciones: {e}")
        return None

def decirculate_story(story_id, website_ids):
    """Paso 1: Elimina todas las circulaciones de una nota."""
    if not website_ids:
        return True

    all_successful = True
    for website_id in website_ids:
        url = f"{DRAFT_API_BASE_URL}/story/{story_id}/circulation/{website_id}"
        try:
            response = make_request_with_retry("DELETE", url, timeout=10)
            if response.status_code == 404:
                pass # Ya no existe, éxito
            elif response and response.ok:
                pass # Éxito
            else:
                print(f"  FALLO al descircular de '{website_id}'")
                all_successful = False
        except Exception as e:
            print(f"  [{story_id}] ERROR FATAL en decirculate: {e}")
            all_successful = False
            
    return all_successful

def unpublish_story(story_id):
    """Paso 2: Despublica la nota, eliminando su revisión publicada."""
    url = f"{DRAFT_API_BASE_URL}/story/{story_id}/revision/published"
    try:
        response = make_request_with_retry("DELETE", url, timeout=10)
        
        if response.status_code == 404:
            return True # Ya estaba despublicada
            
        if response.ok:
            return True
            
        print(f"  FALLO al despublicar: {response.status_code}")
        return False
    except Exception as e:
        print(f"  [{story_id}] ERROR FATAL en unpublish: {e}")
        return False

def delete_story_permanently(story_id):
    """Paso 4: Borra la nota de forma definitiva e irreversible."""
    url = f"{DRAFT_API_BASE_URL}/story/{story_id}"
    try:
        response = make_request_with_retry("DELETE", url, timeout=10)
        
        if response.status_code == 404:
            return True

        if response and response.ok:
            return True
        
        print(f"  FALLO al borrar permanentemente")
        return False
    except Exception as e:
        print(f"  [{story_id}] ERROR FATAL en delete: {e}")
        return False

def process_story_for_deletion(story_id):
    """
    Ejecuta la secuencia completa de borrado para un ID de nota.
    """
    # PASO 1: DESCIRCULAR
    website_ids = get_circulations(story_id)
    if website_ids is None:
        print(f"  [{story_id}] Error al obtener las circulaciones.")
        return

    if not decirculate_story(story_id, website_ids):
        print(f"  [{story_id}] FALLO en descirculación.")
        return

    # PASO 2: DESPUBLICAR
    if not unpublish_story(story_id):
        print(f"  [{story_id}] FALLO en despublicación.")
        return

    # PASO 4: BORRADO PERMANENTE
    if not delete_story_permanently(story_id):
        print(f"[{story_id}] FALLO en borrado permanente.")


if __name__ == "__main__":
    if not (ARC_ACCESS_TOKEN and ORG_ID):
        print("Error: Asegúrate de que las variables ARC_ACCESS_TOKEN y ORG_ID estén configuradas en tu archivo.env.")
        sys.exit(1)

    parser = argparse.ArgumentParser(description='Pipeline para descircular, despublicar y borrar notas usando Draft API.')
    parser.add_argument('--ids-file', help='Archivo de texto con un ID por línea (default notas_a_borrar.txt)', default='notas_a_borrar.txt')
    parser.add_argument('--csv', help='CSV con header que incluya story_id (puede ser un archivo único)')
    parser.add_argument('--csv-dir', help='Directorio donde buscar CSVs (procesa todos los .csv dentro)')
    parser.add_argument('--limit', type=int, help='Limita el número de notas a procesar (opcional)')
    args = parser.parse_args()

    story_ids = []

    try:
        if args.csv:
            rows = load_rows_from_csv(args.csv)
            for r in rows:
                story_ids.append(r['story_id'])
        elif args.csv_dir:
            # iterate over csv files in directory
            for fname in sorted(os.listdir(args.csv_dir)):
                if not fname.lower().endswith('.csv'):
                    continue
                path = os.path.join(args.csv_dir, fname)
                rows = load_rows_from_csv(path)
                for r in rows:
                    story_ids.append(r['story_id'])
        else:
            # fallback to ids file
            ids_file_path = args.ids_file
            with open(ids_file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    v = line.strip()
                    if v:
                        story_ids.append(v)

        if args.limit:
            story_ids = story_ids[:args.limit]

        if not story_ids:
            print('No se encontraron IDs para procesar.')
            sys.exit(0)

        print(f"Se van a procesar {len(story_ids)} notas.")

        # Ejecución concurrente con ThreadPoolExecutor
        print(f"Iniciando procesamiento concurrente con {MAX_WORKERS} hilos...")
        start_time = time.time()
        processed_count = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Enviamos todas las tareas al pool
            futures = {executor.submit(process_story_for_deletion, sid): sid for sid in story_ids}
            
            for future in as_completed(futures):
                sid = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    print(f"Excepción generada para la nota {sid}: {exc}")
                
                processed_count += 1
                
                # Imprimimos progreso cada 50 notas para mejor feedback
                if processed_count % 50 == 0:
                    elapsed = time.time() - start_time
                    rate = processed_count / elapsed
                    remaining = len(story_ids) - processed_count
                    eta_seconds = remaining / rate if rate > 0 else 0
                    eta_minutes = eta_seconds / 60
                    print(f"Procesadas {processed_count}/{len(story_ids)} notas. Velocidad: {rate:.2f} notas/seg. ETA: {eta_minutes:.1f} min")

        total_time = time.time() - start_time
        print(f"\nProcesamiento finalizado en {total_time:.2f} segundos.")
        print(f"Velocidad promedio: {len(story_ids)/total_time:.2f} notas/segundo.")

    except FileNotFoundError as fe:
        print(f"Error: archivo no encontrado: {fe}")
    except Exception as e:
        print(f"Ocurrió un error inesperado durante la ejecución: {e}")