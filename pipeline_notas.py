import os
import sys
import csv
import argparse
import asyncio
import time
import aiohttp
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# --- Configuración ---
ARC_ACCESS_TOKEN = os.getenv("ARC_ACCESS_TOKEN")
ORG_ID = os.getenv("ORG_ID")
DRAFT_API_BASE_URL = f"https://api.{ORG_ID}.arcpublishing.com/draft/v1"

# CONFIGURACIÓN DE VELOCIDAD
# Límite real: 900 req/min = 15 req/s.
# Límite de 900 req/min.
MAX_REQUESTS_PER_SECOND = 18.0

# Validar credenciales
if not (ARC_ACCESS_TOKEN and ORG_ID):
    print("Error: Faltan variables de entorno (ARC_ACCESS_TOKEN, ORG_ID) en el archivo .env")
    sys.exit(1)

# --- Clase RateLimiter Asíncrono ---
class AsyncRateLimiter:
    """Controla que no se exceda el número de peticiones por segundo de forma precisa."""
    def __init__(self, requests_per_second):
        self.delay = 1.0 / requests_per_second
        self.lock = asyncio.Lock()
        self.last_request_time = 0

    async def wait(self):
        async with self.lock:
            now = time.monotonic()
            elapsed = now - self.last_request_time
            wait_time = self.delay - elapsed
            
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            
            self.last_request_time = time.monotonic()

# --- Funciones de Red ---

async def delete_story_async(session, story_id, site, limiter):
    """
    Intenta borrar la nota manejando reintentos y 429s automáticamente.
    """
    url = f"{DRAFT_API_BASE_URL}/story/{story_id}"
    headers = {
        "Authorization": f"Bearer {ARC_ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "Arc-Priority": "ingestion"
    }

    retries = 0
    max_retries = 5
    backoff = 1.0

    while retries < max_retries:
        # Esperamos nuestro turno según el limitador
        await limiter.wait()

        try:
            async with session.delete(url, headers=headers) as response:
                
                # Caso Éxito o No Existe (404 se considera éxito al borrar)
                if response.status == 204 or response.status == 200 or response.status == 404:
                    # Leemos respuesta para liberar conexión
                    await response.read() 
                    print(f"✅ [{story_id}] Borrada ({site or 'N/A'}) status={response.status}")
                    return True

                # Caso Rate Limit (429)
                if response.status == 429:
                    retry_after = response.headers.get("Retry-After")
                    sleep_time = float(retry_after) if retry_after else backoff
                    print(f"⚠️ [{story_id}] 429 Rate Limit. Esperando {sleep_time:.2f}s...")
                    await asyncio.sleep(sleep_time)
                    backoff *= 1.5 # Backoff exponencial
                    retries += 1
                    continue

                # Otros errores de servidor (5xx)
                if response.status >= 500:
                    print(f"🔥 [{story_id}] Error servidor {response.status}. Reintentando...")
                    await asyncio.sleep(backoff)
                    backoff *= 2
                    retries += 1
                    continue

                # Error desconocido cliente (400, 401, 403)
                print(f"❌ [{story_id}] Error cliente {response.status}. No se reintenta.")
                return False

        except aiohttp.ClientError as e:
            print(f"❌ [{story_id}] Error de conexión: {e}")
            await asyncio.sleep(backoff)
            retries += 1

    print(f"💀 [{story_id}] Falló tras {max_retries} intentos.")
    return False

# --- Carga de Datos ---

def load_rows_from_csv(csv_path):
    """Lee CSVs intentando detectar si tienen header o no."""
    rows = []
    try:
        with open(csv_path, newline='', encoding='utf-8') as f:
            # Leemos un pedazo para detectar formato
            sample = f.read(1024)
            f.seek(0)
            try:
                has_header = csv.Sniffer().has_header(sample)
            except csv.Error:
                has_header = False # Si falla el sniffer, asumimos sin header
            
            f.seek(0)
            
            if has_header:
                reader = csv.DictReader(f)
                for r in reader:
                    # Busca columnas comunes de ID
                    sid = r.get('story_id') or r.get('_id') or r.get('id')
                    if sid: rows.append({'story_id': sid, 'site': r.get('site')})
            else:
                reader = csv.reader(f)
                for r in reader:
                    if r and len(r) > 0: 
                        rows.append({'story_id': r[0].strip(), 'site': None})
    except Exception as e:
        print(f"Error leyendo {csv_path}: {e}")
    return rows

def load_ids(args):
    """Carga los IDs desde archivo TXT, CSV único o directorio de CSVs."""
    story_ids = []
    
    if args.csv:
        rows = load_rows_from_csv(args.csv)
        story_ids.extend([(r['story_id'], r.get('site')) for r in rows])
        
    elif args.csv_dir:
        csv_files = []
        for root, _, files in os.walk(args.csv_dir):
            for fname in sorted(files): # Ordenar archivos dentro de cada directorio
                if fname.lower().endswith('.csv'):
                    path = os.path.join(root, fname)
                    csv_files.append(path)
        
        # Ordenar la lista completa de rutas para asegurar un orden predecible
        csv_files.sort()

        for path in csv_files:
            rows = load_rows_from_csv(path)
            story_ids.extend([(r['story_id'], r.get('site')) for r in rows])
            print(f"Cargados {len(rows)} IDs de {os.path.basename(path)}")
    else:
        # Fallback a archivo txt
        try:
            with open(args.ids_file, 'r', encoding='utf-8') as f:
                story_ids = [(line.strip(), None) for line in f if line.strip()]
        except FileNotFoundError:
            print(f"No se encontró archivo de IDs: {args.ids_file}")
            
    if args.limit:
        story_ids = story_ids[:args.limit]
        
    return story_ids

# --- Main Asíncrono ---

async def main():
    parser = argparse.ArgumentParser(description="Borrado masivo optimizado para Arc XP")
    parser.add_argument('--ids-file', default='notas_a_borrar.txt', help='Archivo TXT con IDs')
    parser.add_argument('--csv', help='Archivo CSV individual')
    parser.add_argument('--csv-dir', help='Directorio de CSVs')
    parser.add_argument('--limit', type=int, help='Límite de notas a procesar')
    args = parser.parse_args()

    # 1. Cargar IDs
    print("--- Iniciando Script de Borrado Optimizado ---")
    items = load_ids(args)
    
    # Filtrar duplicados si es necesario (opcional)
    # items = list(set(items)) 

    if not items:
        print("No hay notas para procesar. Verifica tus archivos.")
        return

    print(f"Total a procesar: {len(items)} notas.")
    print(f"Velocidad configurada: {MAX_REQUESTS_PER_SECOND} req/s")

    # 2. Configurar Rate Limiter y Sesión
    limiter = AsyncRateLimiter(MAX_REQUESTS_PER_SECOND)
    
    # TCPConnector limita conexiones totales para no saturar tu máquina local
    connector = aiohttp.TCPConnector(limit=50) 

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        start_time = time.time()
        
        # Crear tareas
        for story_id, site in items:
            task = delete_story_async(session, story_id, site, limiter)
            tasks.append(task)
        
        # Ejecutar y mostrar progreso
        completed = 0
        total = len(tasks)
        
        # as_completed nos permite iterar a medida que terminan
        for future in asyncio.as_completed(tasks):
            await future
            completed += 1
            if completed % 50 == 0:
                elapsed = time.time() - start_time
                rate = completed / elapsed
                remaining = total - completed
                eta = remaining / rate if rate > 0 else 0
                print(f"--> Progreso: {completed}/{total} | {rate:.2f} req/s | ETA: {eta/60:.1f} min")

        total_time = time.time() - start_time
        print(f"\n✅ Finalizado en {total_time:.2f}s.")
        print(f"📊 Velocidad promedio final: {len(items)/total_time:.2f} req/s")

if __name__ == "__main__":
    # Fix crítico para Windows: evita errores "Event loop is closed"
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Proceso interrumpido por el usuario.")