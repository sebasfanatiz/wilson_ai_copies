from flask import Flask, render_template, request, send_from_directory
from datetime import datetime
import os
import sys
import re
import threading

# Asegura que Python encuentre mi_agente
#agente_path = os.path.abspath(os.path.join(__file__, '..', '..', 'mi_agente'))
#sys.path.append(agente_path)
from agente import generar_copies

app = Flask(__name__)

# Directorio base y carpeta de salidas
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
SALIDAS_DIR = os.path.join(BASE_DIR, 'salidas')
os.makedirs(SALIDAS_DIR, exist_ok=True)

@app.route('/', methods=['GET'])
def index():
    archivos_info = [] # Cambiamos el nombre para que sea más claro
    if os.path.exists(SALIDAS_DIR):
        archivos_nombres = [f for f in os.listdir(SALIDAS_DIR) if f.endswith('.xlsx')]
        
        for nombre_archivo in archivos_nombres:
            ruta_completa = os.path.join(SALIDAS_DIR, nombre_archivo)
            try:
                # Obtenemos la fecha de modificación como un número (timestamp)
                timestamp = os.path.getmtime(ruta_completa)
                # La convertimos a un objeto de fecha y hora legible
                fecha_hora = datetime.fromtimestamp(timestamp)
                # Le damos un formato amigable para Argentina
                fecha_formateada = fecha_hora.strftime("%d/%m/%Y - %H:%M:%S")
                
                archivos_info.append({
                    "nombre": nombre_archivo,
                    "fecha": fecha_formateada,
                    "timestamp": timestamp # guardamos el original para ordenar
                })
            except FileNotFoundError:
                # En el caso improbable de que el archivo se elimine mientras lo leemos
                print(f"No se pudo encontrar el archivo: {nombre_archivo}")

        # Ordenamos la lista por el timestamp (el número), del más nuevo al más viejo
        archivos_info.sort(key=lambda x: x['timestamp'], reverse=True)
        
    return render_template('index.html', resultado=None, archivos=archivos_info)

@app.route('/procesar', methods=['POST'])
def procesar():
    titulo = request.form['titulo_campaña']
    brief  = request.form['brief_campaña']

    safe_title = re.sub(r'[^0-9A-Za-z]+', '_', titulo).strip('_')
    filename   = f"copies_{safe_title}.xlsx"
    path_out   = os.path.join(SALIDAS_DIR, filename)

    # Esta es la función que hará el trabajo pesado.
    # La definimos aquí para que tenga acceso a todas las variables.
    def worker_generar_excel(app_context, titulo_w, brief_w, path_out_w):
        with app_context:
            print(f"Worker iniciado para generar: {path_out_w}")
            try:
                generar_copies(titulo_w, brief_w, output_filename=path_out_w)
                print(f"✅ ÉXITO: Archivo {path_out_w} generado correctamente.")
            except Exception as e:
                print(f"❌ ERROR en el worker: {e}")

    # Creamos un "hilo" (thread) que ejecutará nuestra función worker en segundo plano.
    # Le pasamos el contexto de la aplicación para que funcione correctamente.
    thread = threading.Thread(
        target=worker_generar_excel,
        args=(app.app_context(), titulo, brief, path_out)
    )
    thread.start() # <-- Iniciamos el hilo. NO esperamos a que termine.

    print(f"Hilo para {filename} iniciado. La página ya respondió al usuario.")

    # Respondemos INMEDIATAMENTE al usuario mientras el hilo trabaja por detrás.
    # Le pasamos una variable especial para que sepa que el proceso empezó.
    return render_template(
        'index.html',
        resultado_proceso="iniciado", # <--- Variable clave
        titulo=titulo,
        brief=brief,
        archivos=[] # Pasamos una lista vacía para que no de error al renderizar
    )

@app.route('/eliminar/<path:filename>', methods=['POST'])
def eliminar(filename):
    # --- Medida de Seguridad ---
    # os.path.basename se asegura de que no nos pasen rutas maliciosas como ../../app.py
    # Solo nos quedamos con el nombre del archivo.
    safe_filename = os.path.basename(filename)

    # Construimos la ruta completa de forma segura
    ruta_archivo = os.path.join(SALIDAS_DIR, safe_filename)

    try:
        if os.path.exists(ruta_archivo) and safe_filename.endswith('.xlsx'):
            os.remove(ruta_archivo)
            # Usamos 'flash' para enviar un mensaje de éxito a la página principal
            flash(f"Archivo '{safe_filename}' eliminado correctamente.", "success")
        else:
            flash(f"Error: El archivo '{safe_filename}' no existe o no es válido.", "error")
    except Exception as e:
        flash(f"Ocurrió un error al intentar eliminar el archivo: {e}", "error")

    # Redirigimos al usuario de vuelta a la página principal
    return redirect(url_for('index'))

@app.route('/salidas/<path:filename>')
def descargar(filename):
    return send_from_directory(SALIDAS_DIR, filename, as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True)
