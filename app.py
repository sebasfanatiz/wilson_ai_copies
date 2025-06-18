from flask import Flask, render_template, request, send_from_directory, redirect, url_for, flash
from datetime import datetime
import os
import sys
import re
import threading

# Asegura que Python encuentre mi_agente
#agente_path = os.path.abspath(os.path.join(__file__, '..', '..', 'mi_agente'))
#sys.path.append(agente_path)
from agente import generar_copies
from groq import BadRequestError

app = Flask(__name__)
app.secret_key = 'fanatiz'

# Directorio base y carpeta de salidas
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
SALIDAS_DIR = os.path.join(BASE_DIR, 'salidas')
os.makedirs(SALIDAS_DIR, exist_ok=True)

@app.route('/', methods=['GET'])
def index():
    archivos_info = []
    if os.path.exists(SALIDAS_DIR):
        for nombre_base in os.listdir(SALIDAS_DIR):
            ruta_completa = os.path.join(SALIDAS_DIR, nombre_base)
            timestamp = os.path.getmtime(ruta_completa)
            fecha_hora = datetime.fromtimestamp(timestamp)
            fecha_formateada = fecha_hora.strftime("%d/%m/%Y - %H:%M:%S")

            info = { "timestamp": timestamp, "fecha": fecha_formateada }

            if nombre_base.endswith('.xlsx'):
                info["nombre"] = nombre_base
                info["status"] = "success"
            elif nombre_base.endswith('.error'):
                info["nombre"] = nombre_base.replace('.error', '')
                info["status"] = "error"
                try:
                    with open(ruta_completa, 'r', encoding='utf-8') as f:
                        info["error_msg"] = f.read()
                except Exception:
                    info["error_msg"] = "No se pudo leer el detalle del error."
            elif nombre_base.endswith('.processing'):
                info["nombre"] = nombre_base.replace('.processing', '')
                info["status"] = "processing"
            else:
                continue # Ignoramos otros posibles archivos

            archivos_info.append(info)

    archivos_info.sort(key=lambda x: x['timestamp'], reverse=True)
    return render_template('index.html', archivos=archivos_info)


@app.route('/procesar', methods=['POST'])
def procesar():
    titulo = request.form['titulo_campaña']
    brief  = request.form['brief_campaña']

    safe_title = re.sub(r'[^0-9A-Za-z]+', '_', titulo).strip('_')
    filename   = f"copies_{safe_title}.xlsx"
    path_out   = os.path.join(SALIDAS_DIR, filename)

    # Definimos las rutas de los archivos de estado
    path_processing = path_out + '.processing'
    path_error = path_out + '.error'

    def worker_generar_excel(app_context, titulo_w, brief_w, path_out_w, processing_w, error_w):
        with app_context:
            # 1. Creamos el archivo .processing para indicar que empezamos
            with open(processing_w, 'w') as f:
                f.write(str(datetime.now()))
            
            try:
                generar_copies(titulo_w, brief_w, output_filename=path_out_w)
                print(f"✅ ÉXITO: Archivo {path_out_w} generado.")
                # Si todo va bien, eliminamos el archivo .processing
                if os.path.exists(processing_w):
                    os.remove(processing_w)
            
            except BadRequestError as e:
                mensaje_error = "Límite de tokens superado. El Brief es muy largo, por favor resúmelo." if 'token' in str(e).lower() else f"Error en la petición: {e}"
                print(f"❌ ERROR (BadRequest): {mensaje_error}")
                # Si falla, borramos .processing y creamos .error con el mensaje
                if os.path.exists(processing_w):
                    os.remove(processing_w)
                with open(error_w, 'w', encoding='utf-8') as f:
                    f.write(mensaje_error)

            except Exception as e:
                mensaje_error = f"Ocurrió un error inesperado durante la generación: {e}"
                print(f"❌ ERROR (General): {mensaje_error}")
                # También creamos el archivo .error para cualquier otra falla
                if os.path.exists(processing_w):
                    os.remove(processing_w)
                with open(error_w, 'w', encoding='utf-8') as f:
                    f.write(mensaje_error)

    thread = threading.Thread(
        target=worker_generar_excel,
        args=(app.app_context(), titulo, brief, path_out, path_processing, path_error)
    )
    thread.start()

    flash(f"¡Proceso para '{filename}' iniciado! Aparecerá en la lista de abajo en unos minutos.", "success")
    return redirect(url_for('index'))


@app.route('/eliminar/<path:filename>', methods=['POST'])
def eliminar(filename):
    safe_filename = os.path.basename(filename)
    
    # Buscamos y eliminamos cualquiera de los 3 tipos de archivo (.xlsx, .error, .processing)
    path_xlsx = os.path.join(SALIDAS_DIR, safe_filename)
    path_error = path_xlsx + '.error'
    path_processing = path_xlsx + '.processing'
    
    eliminado = False
    for ruta_archivo in [path_xlsx, path_error, path_processing]:
        try:
            if os.path.exists(ruta_archivo):
                os.remove(ruta_archivo)
                eliminado = True
        except Exception as e:
            flash(f"No se pudo eliminar '{ruta_archivo}': {e}", "error")

    if eliminado:
        flash(f"Registro '{safe_filename}' eliminado correctamente.", "success")
    
    return redirect(url_for('index'))


if __name__ == '__main__':
    app.run(debug=True)
