from flask import Flask, render_template, request, send_from_directory
import os
import sys
import re

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
    return render_template('index.html', resultado=None)

@app.route('/procesar', methods=['POST'])
def procesar():
    titulo = request.form['titulo_campaña']
    brief  = request.form['brief_campaña']

    # Sanitiza el título para el nombre de archivo
    safe_title = re.sub(r'[^0-9A-Za-z]+', '_', titulo).strip('_')
    filename   = f"copies_{safe_title}.xlsx"
    path_out   = os.path.join(SALIDAS_DIR, filename)

    try:
        generar_copies(titulo, brief, output_filename=path_out)
        resultado = filename
    except Exception as e:
        resultado = f"ERROR: {e}"

    return render_template(
        'index.html',
        resultado=resultado,
        titulo=titulo,
        brief=brief
    )

@app.route('/salidas/<path:filename>')
def descargar(filename):
    return send_from_directory(SALIDAS_DIR, filename, as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True)
