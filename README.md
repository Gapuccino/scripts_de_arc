Auditoría y pipeline de notas (Arc XP)

Este repositorio contiene herramientas para auditar y, si se decide, despublicar y borrar notas (stories) en Arc XP.

Contenido relevante:
- auditoria_notas.py — consulta la API y genera CSVs por sitio/año con story_id, publish_date y url.
- pipeline_notas.py — pipeline que puede descircular, despublicar y borrar notas usando la Draft API.
- requirements.txt — dependencias Python.

IMPORTANTE: No incluyas tu token en el repositorio. Usa un archivo .env en la raíz con las variables de entorno necesarias. .env está en .gitignore.

Variables de entorno requeridas:
- ARC_ACCESS_TOKEN — token Bearer con permisos (All access) para Draft API y búsqueda.
- ORG_ID — id de la organización usado en las URLs de API.

Instalación (PowerShell):
1) Crear y activar un entorno virtual:
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
2) Instalar dependencias:
   python -m pip install --upgrade pip
   python -m pip install -r requirements.txt

Ejecutar auditoría (ejemplo):
   python .\auditoria_notas.py

Ejecutar pipeline en prueba:
   python .\pipeline_notas.py --csv .\reports_fotos\fayerwayer\notas_publicadas_fayerwayer_2021.csv --limit 5

Subir a GitHub (resumen):
1) Crear repo en GitHub (web o 'gh repo create')
2) Inicializar git, añadir y commitear:
   git init
   git add .
   git commit -m "Initial import"
   git remote add origin https://github.com/<user>/<repo>.git
   git branch -M main
   git push -u origin main

Seguridad:
- Nunca subas .env ni tokens. Si accidentalmente subes un token, revócalo inmediatamente.
- Prueba con --limit antes de borrados masivos.

Si quieres, preparo un zip o te doy los comandos exactos para clonar y ejecutar desde otra PC.
