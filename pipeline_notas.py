import os
import requests
import time
import sys
import csv
import argparse
from dotenv import load_dotenv

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
        reader = csv.DictReader(f)
        for r in reader:
            story_id = r.get('story_id') or r.get('id') or r.get('_id')
            pub = r.get('publish_date') or r.get('publish') or r.get('date')
            url = r.get('url') or r.get('canonical_url') or r.get('website_url')
            site = r.get('site') or r.get('website') or site_guess
            if story_id:
                rows.append({'story_id': story_id, 'publish_date': pub, 'url': url, 'site': site})
    return rows

def get_circulations(story_id):
    """Obtiene la lista de sitios web donde una nota está circulada."""
    url = f"{DRAFT_API_BASE_URL}/story/{story_id}/circulation"
    try:
        response = SESSION.get(url, timeout=10)
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
    except requests.exceptions.RequestException as e:
        print(f"  ERROR al obtener circulaciones para {story_id}: {e}")
        return None

def decirculate_story(story_id, website_ids):
    """Paso 1: Elimina todas las circulaciones de una nota."""
    print(f"-> Paso 1: Descirculando la nota de {len(website_ids)} sitio(s)...")
    if not website_ids:
        print("  La nota no tiene circulaciones para eliminar.")
        return True

    all_successful = True
    for website_id in website_ids:
        url = f"{DRAFT_API_BASE_URL}/story/{story_id}/circulation/{website_id}"
        try:
            response = SESSION.delete(url, timeout=10)
            response.raise_for_status()
            print(f"  - Descirculada de '{website_id}' exitosamente.")
            time.sleep(0.05) # Pausa reducida (Rate limit aumentado a 900)
        except requests.exceptions.RequestException as e:
            print(f"  ERROR al descircular de '{website_id}': {e}")
            all_successful = False
    return all_successful

def unpublish_story(story_id):
    """Paso 2: Despublica la nota, eliminando su revisión publicada."""
    print("-> Paso 2: Despublicando la nota...")
    url = f"{DRAFT_API_BASE_URL}/story/{story_id}/revision/published"
    try:
        response = SESSION.delete(url, timeout=10)
        # Una respuesta 404 (No Encontrado) es aceptable aquí, significa que ya no estaba publicada.
        if response.status_code == 404:
            print("  La nota no tenía una revisión publicada activa (lo cual es correcto).")
            return True
        response.raise_for_status()
        print("  - Nota despublicada exitosamente.")
        return True
    except requests.exceptions.RequestException as e:
        print(f"  ERROR al despublicar la nota: {e}")
        return False

def delete_story_permanently(story_id):
    """Paso 4: Borra la nota de forma definitiva e irreversible."""
    print("-> Paso 4: Borrando la nota permanentemente...")
    url = f"{DRAFT_API_BASE_URL}/story/{story_id}"
    try:
        response = SESSION.delete(url, timeout=10)
        response.raise_for_status()
        print("  - ¡ÉXITO! Nota borrada permanentemente de Arc XP.")
        return True
    except requests.exceptions.RequestException as e:
        print(f"  ERROR al borrar la nota permanentemente: {e}")
        return False

def process_story_for_deletion(story_id):
    """
    Ejecuta la secuencia completa de borrado para un ID de nota.
    """
    print(f"\n--- INICIANDO PROCESO DE BORRADO PARA NOTA: {story_id} ---")

    # PASO 1: DESCIRCULAR
    website_ids = get_circulations(story_id)
    if website_ids is None:
        print("  No se pudo continuar. Error al obtener las circulaciones.")
        return

    if not decirculate_story(story_id, website_ids):
        print("  FALLO en el paso de descirculación. Abortando el proceso para esta nota.")
        return

    # PASO 2: DESPUBLICAR
    if not unpublish_story(story_id):
        print("  FALLO en el paso de despublicación. Abortando el proceso para esta nota.")
        return

    # PASO 3: URL CANÓNICA (se elimina automáticamente con el paso 1)
    print("-> Paso 3: La URL canónica se elimina con la descirculación (completado).")

    # PASO 4: BORRADO PERMANENTE
    delete_story_permanently(story_id)
    print(f"--- Proceso para la nota {story_id} finalizado. ---")


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

        for sid in story_ids:
            process_story_for_deletion(sid)
            time.sleep(0.1) # Pausa reducida entre notas

    except FileNotFoundError as fe:
        print(f"Error: archivo no encontrado: {fe}")
    except Exception as e:
        print(f"Ocurrió un error inesperado durante la ejecución: {e}")