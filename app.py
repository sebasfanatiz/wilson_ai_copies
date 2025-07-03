# app.py
from flask import Flask, render_template, request, send_from_directory, redirect, url_for, flash
from datetime import datetime
import threading
import os
import re
# Asegúrate de importar los errores de ambas librerías si cambias entre ellas
try:
    from openai import BadRequestError
except ImportError:
    # Define un placeholder si no está instalada para evitar errores de importación
    class BadRequestError(Exception): pass

from agente import generar_copies

app = Flask(__name__)

# --- CONFIGURACIÓN ---
app.secret_key = os.getenv("FLASK_SECRET_KEY", "un-secreto-muy-seguro")
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
SALIDAS_DIR = os.path.join(BASE_DIR, 'salidas')
os.makedirs(SALIDAS_DIR, exist_ok=True)


# --- RUTA PRINCIPAL (INDEX) ---
@app.route('/', methods=['GET'])
def index():
    archivos_info = []
    if os.path.exists(SALIDAS_DIR):
        for nombre_base in os.listdir(SALIDAS_DIR):
            ruta_completa = os.path.join(SALIDAS_DIR, nombre_base)
            try:
                timestamp = os.path.getmtime(ruta_completa)
                fecha_hora = datetime.fromtimestamp(timestamp)
                fecha_formateada = fecha_hora.strftime("%d/%m/%Y - %H:%M:%S")
                info = { "timestamp": timestamp, "fecha": fecha_formateada }

                if nombre_base.endswith('.xlsx'):
                    info["nombre"] = nombre_base
                    info["status"] = "success"
                    # << NUEVO: Buscar y leer el archivo de resumen de costos >>
                    ruta_summary = ruta_completa + '.summary'
                    if os.path.exists(ruta_summary):
                        with open(ruta_summary, 'r', encoding='utf-8') as f:
                            info["summary"] = f.read()
                elif nombre_base.endswith('.error'):
                    info["nombre"] = nombre_base.replace('.error', '')
                    info["status"] = "error"
                    with open(ruta_completa, 'r', encoding='utf-8') as f:
                        info["error_msg"] = f.read()
                elif nombre_base.endswith('.processing'):
                    info["nombre"] = nombre_base.replace('.processing', '')
                    info["status"] = "processing"
                else:
                    continue # Ignorar otros archivos como .summary

                archivos_info.append(info)
            except Exception as e:
                print(f"Error procesando el archivo {nombre_base}: {e}")

    archivos_info.sort(key=lambda x: x['timestamp'], reverse=True)
    return render_template('index.html', archivos=archivos_info)


# --- RUTA PARA PROCESAR EL FORMULARIO ---
@app.route('/procesar', methods=['POST'])
def procesar():
    titulo = request.form['titulo_campaña']
    brief  = request.form['brief_campaña']

    safe_title = re.sub(r'[^0-9A-Za-z]+', '_', titulo).strip('_')
    filename      = f"copies_{safe_title}.xlsx"
    path_out      = os.path.join(SALIDAS_DIR, filename)
    path_processing = path_out + '.processing'
    path_error    = path_out + '.error'
    # << NUEVO: Definir la ruta para el archivo de resumen >>
    path_summary  = path_out + '.summary'

    def worker_generar_excel(app_context, titulo_w, brief_w, path_out_w, processing_w, error_w, summary_w):
        with app_context:
            with open(processing_w, 'w') as f: f.write(str(datetime.now()))
            try:
                # << CAMBIO: Capturamos el resumen que devuelve la función >>
                _, cost_summary = generar_copies(titulo_w, brief_w, output_filename=path_out_w)
                print(f"✅ ÉXITO: Archivo {path_out_w} generado.")
                
                # << NUEVO: Guardamos el resumen en su propio archivo >>
                if cost_summary:
                    with open(summary_w, 'w', encoding='utf-8') as f:
                        f.write(cost_summary)

                if os.path.exists(processing_w): os.remove(processing_w)
            except BadRequestError as e:
                mensaje_error = "Límite de tokens superado. El Brief es muy largo." if 'token' in str(e).lower() else f"Error en la petición: {e}"
                print(f"❌ ERROR (BadRequest): {mensaje_error}")
                if os.path.exists(processing_w): os.remove(processing_w)
                with open(error_w, 'w', encoding='utf-8') as f: f.write(mensaje_error)
            except Exception as e:
                mensaje_error = f"Ocurrió un error inesperado: {e}"
                print(f"❌ ERROR (General): {mensaje_error}")
                if os.path.exists(processing_w): os.remove(processing_w)
                with open(error_w, 'w', encoding='utf-8') as f: f.write(mensaje_error)

    # << CAMBIO: Pasamos la ruta del archivo de resumen al thread >>
    thread = threading.Thread(target=worker_generar_excel, args=(app.app_context(), titulo, brief, path_out, path_processing, path_error, path_summary))
    thread.start()

    flash(f"¡Proceso para '{filename}' iniciado! Aparecerá en la lista en unos minutos.", "success")
    return redirect(url_for('index'))


# --- RUTA PARA ELIMINAR REGISTROS ---
@app.route('/eliminar/<path:filename>', methods=['POST'])
def eliminar(filename):
    safe_filename = os.path.basename(filename)
    path_xlsx = os.path.join(SALIDAS_DIR, safe_filename)
    path_error = path_xlsx + '.error'
    path_processing = path_xlsx + '.processing'
    # << NUEVO: Definir la ruta del resumen para eliminarlo también >>
    path_summary = path_xlsx + '.summary'
    
    eliminado = False
    # << CAMBIO: Agregamos el path_summary a la lista de archivos a eliminar >>
    for ruta_archivo in [path_xlsx, path_error, path_processing, path_summary]:
        try:
            if os.path.exists(ruta_archivo):
                os.remove(ruta_archivo)
                eliminado = True
        except Exception as e:
            flash(f"No se pudo eliminar '{ruta_archivo}': {e}", "error")

    if eliminado:
        flash(f"Registro '{safe_filename}' eliminado correctamente.", "success")
    
    return redirect(url_for('index'))


# --- RUTA PARA DESCARGAR ARCHIVOS ---
@app.route('/salidas/<path:filename>')
def descargar(filename):
    return send_from_directory(SALIDAS_DIR, filename, as_attachment=True)


# --- INICIO DE LA APLICACIÓN ---
if __name__ == '__main__':
    app.run(debug=True, port=5001)
