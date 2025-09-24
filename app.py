# app.py
from flask import Flask, render_template, request, send_from_directory, redirect, url_for, flash, jsonify
import pandas as pd
from datetime import datetime
import threading, os, re
try:
    from openai import BadRequestError
except ImportError:
    class BadRequestError(Exception): pass

from agente import generar_copies
import pandas as pd

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "un-secreto-muy-seguro")
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
SALIDAS_DIR = os.path.join(BASE_DIR, 'salidas')
os.makedirs(SALIDAS_DIR, exist_ok=True)

def _load_leagues():
    # carga únicas de content_name + “Otro”
    try:
        path_content = os.path.join(BASE_DIR, "content_by_country.xlsx")
        df = pd.read_excel(path_content)
        col = None
        for c in df.columns:
            if str(c).strip().lower() == 'content_name':
                col = c; break
        if col is None:
            return ["Otro"]
        leagues = sorted({str(x).strip() for x in df[col].dropna().astype(str) if str(x).strip()})
        return ["Otro"] + leagues
    except Exception as e:
        print(f"Error cargando ligas: {e}")
        return ["Otro"]

def _normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower().replace('\n',' ').replace('  ',' ').replace(' ','_') for c in df.columns]
    return df

def _norm_base_name(plan_name: str) -> str:
    if not isinstance(plan_name, str): return ""
    return plan_name.lower().replace("monthly","").replace("annual","").strip()

def _get_platform_plans_set(df_plans: pd.DataFrame, platform: str):
    mask = df_plans['platform'].fillna("").str.lower() == platform.lower()
    dfp = df_plans[mask]
    base_names = {_norm_base_name(n) for n in dfp['plan_name'].dropna().astype(str)}
    return base_names

def _any_plan_matches_platform(plans_str, platform_base_names):
    if not isinstance(plans_str, str): return False
    listed = [p.strip() for p in plans_str.split(',') if p.strip()]
    return any(_norm_base_name(p) in platform_base_names for p in listed)

def _load_leagues_by_platform(platform: str):
    """Devuelve ['Otro', ligas...] filtradas por plataforma en content_by_country.xlsx."""
    try:
        path_content = os.path.join(BASE_DIR, "content_by_country.xlsx")
        path_plans   = os.path.join(BASE_DIR, "plans_and_pricing.xlsx")
        dfc = _normalize_headers(pd.read_excel(path_content))
        dfp = _normalize_headers(pd.read_excel(path_plans))

        # columnas mínimas
        if 'plans_available' not in dfc.columns or 'content_name' not in dfc.columns:
            return ["Otro"]

        platform_base_names = _get_platform_plans_set(dfp, platform)

        dfc_filt = dfc[dfc['plans_available'].apply(_any_plan_matches_platform, platform_base_names=platform_base_names)]
        ligas = sorted({str(x).strip() for x in dfc_filt['content_name'].dropna().astype(str) if str(x).strip()})
        # “Otro” siempre primero
        return ["Otro"] + ligas
    except Exception as e:
        print(f"Error cargando ligas para plataforma '{platform}': {e}")
        return ["Otro"]

def _split_markets_cell(cell: str):
    if not isinstance(cell, str): return []
    return [t.strip() for t in cell.split(',') if t.strip()]

def _get_default_markets_for_platform(platform: str):
    """Lee plans_and_pricing.xlsx y devuelve lista única de markets para esa plataforma."""
    try:
        path_plans = os.path.join(BASE_DIR, "plans_and_pricing.xlsx")
        df = _normalize_headers(pd.read_excel(path_plans))
        mask = df['platform'].fillna("").str.lower() == platform.lower()
        dfp = df[mask]
        markets = set()
        for mcell in dfp.get('markets', pd.Series(dtype=str)).dropna().astype(str):
            markets.update(_split_markets_cell(mcell))
        return sorted(markets) if markets else ['US/CA','EUROPE','ROW']
    except Exception as e:
        print(f"Error obteniendo markets para plataforma '{platform}': {e}")
        return ['US/CA','EUROPE','ROW']


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
                    continue
                archivos_info.append(info)
            except Exception as e:
                print(f"Error procesando el archivo {nombre_base}: {e}")

    archivos_info.sort(key=lambda x: x['timestamp'], reverse=True)

    plataformas = ["Fanatiz", "L1MAX", "AFA Play"]
    plataforma_default = plataformas[0]  # Fanatiz
    ligas = _load_leagues_by_platform(plataforma_default)

    return render_template('index.html', archivos=archivos_info, plataformas=plataformas, ligas=ligas)

@app.route('/api/ligas', methods=['GET'])
def api_ligas():
    platform = request.args.get('platform', 'Fanatiz')
    ligas = _load_leagues_by_platform(platform)
    return jsonify(ligas)

@app.route('/api/markets', methods=['GET'])
def api_markets():
    platform = request.args.get('platform', 'Fanatiz')
    league   = request.args.get('league', 'Otro')

    try:
        # Si la liga es "Otro" => fallback: todos los mercados de la plataforma
        if league.strip().lower() == 'otro':
            return jsonify(_get_default_markets_for_platform(platform))

        # Si hay liga: filtrar content_by_country por plataforma y liga, y colectar sus markets
        path_content = os.path.join(BASE_DIR, "content_by_country.xlsx")
        path_plans   = os.path.join(BASE_DIR, "plans_and_pricing.xlsx")
        dfc = _normalize_headers(pd.read_excel(path_content))
        dfp = _normalize_headers(pd.read_excel(path_plans))

        if 'plans_available' not in dfc.columns or 'content_name' not in dfc.columns or 'markets_available' not in dfc.columns:
            return jsonify([])

        # filtrar por plataforma (reutilizamos la lógica de planes base)
        def _norm_base_name(plan_name: str):
            if not isinstance(plan_name, str): return ""
            return plan_name.lower().replace("monthly","").replace("annual","").strip()

        def _get_platform_plans_set(df_plans: pd.DataFrame, platform: str):
            mask = df_plans['platform'].fillna("").str.lower() == platform.lower()
            dfp2 = df_plans[mask]
            return {_norm_base_name(n) for n in dfp2['plan_name'].dropna().astype(str)}

        def _any_plan_matches_platform(plans_str, platform_base_names):
            if not isinstance(plans_str, str): return False
            listed = [p.strip() for p in plans_str.split(',') if p.strip()]
            return any(_norm_base_name(p) in platform_base_names for p in listed)

        platform_base_names = _get_platform_plans_set(dfp, platform)
        dfc_plat = dfc[dfc['plans_available'].apply(_any_plan_matches_platform, platform_base_names=platform_base_names)]
        dfc_plat_league = dfc_plat[dfc_plat['content_name'].fillna("").astype(str).str.lower() == league.lower()]

        markets = set()
        for mcell in dfc_plat_league.get('markets_available', pd.Series(dtype=str)).dropna().astype(str):
            markets.update(_split_markets_cell(mcell))

        if not markets:
            # si no hay markets específicos, caemos al fallback de plataforma
            markets = set(_get_default_markets_for_platform(platform))

        return jsonify(sorted(markets))
    except Exception as e:
        print(f"Error /api/markets: {e}")
        return jsonify([])

@app.route('/procesar', methods=['POST'])
def procesar():
    titulo     = request.form['titulo_campaña']
    brief      = request.form['brief_campaña']
    plataforma = request.form.get('plataforma', 'Fanatiz')
    langs_csv  = request.form.get('langs', 'ES').upper()
    liga       = request.form.get('liga', 'Otro')
    markets    = request.form.getlist('markets')  # <<--- acá

    safe_title = re.sub(r'[^0-9A-Za-z]+', '_', titulo).strip('_')
    filename        = f"copies_{safe_title}.xlsx"
    path_out        = os.path.join(SALIDAS_DIR, filename)
    path_processing = path_out + '.processing'
    path_error      = path_out + '.error'
    path_summary    = path_out + '.summary'

    def worker_generar_excel(app_context, titulo_w, brief_w, plataforma_w, langs_w, liga_w, markets_w, path_out_w, processing_w, error_w, summary_w):
        with app_context:
            with open(processing_w, 'w') as f: f.write(str(datetime.now()))
            try:
                _, cost_summary = generar_copies(
                    titulo_w, brief_w,
                    platform_name=plataforma_w,
                    langs_csv=langs_w,
                    league_selection=liga_w,
                    output_filename=path_out_w,
                    markets_selected=markets_w    # <<--- pasamos al agente
                )
                if cost_summary:
                    with open(summary_w, 'w', encoding='utf-8') as f:
                        f.write(cost_summary)
                if os.path.exists(processing_w): os.remove(processing_w)
            except BadRequestError as e:
                msg = "Límite de tokens superado. El Brief es muy largo." if 'token' in str(e).lower() else f"Error en la petición: {e}"
                if os.path.exists(processing_w): os.remove(processing_w)
                with open(error_w, 'w', encoding='utf-8') as f: f.write(msg)
            except Exception as e:
                msg = f"Ocurrió un error inesperado: {e}"
                if os.path.exists(processing_w): os.remove(processing_w)
                with open(error_w, 'w', encoding='utf-8') as f: f.write(msg)

    thread = threading.Thread(
        target=worker_generar_excel,
        args=(app.app_context(), titulo, brief, plataforma, langs_csv, liga, markets, path_out, path_processing, path_error, path_summary)
    )
    thread.start()

    flash(f"¡Proceso para '{filename}' iniciado! Recargá en unos minutos.", "success")
    return redirect(url_for('index'))


@app.route('/eliminar/<path:filename>', methods=['POST'])
def eliminar(filename):
    safe_filename = os.path.basename(filename)
    path_xlsx = os.path.join(SALIDAS_DIR, safe_filename)
    path_error = path_xlsx + '.error'
    path_processing = path_xlsx + '.processing'
    path_summary = path_xlsx + '.summary'
    eliminado = False
    for ruta in [path_xlsx, path_error, path_processing, path_summary]:
        try:
            if os.path.exists(ruta):
                os.remove(ruta); eliminado = True
        except Exception as e:
            flash(f"No se pudo eliminar '{ruta}': {e}", "error")
    if eliminado:
        flash(f"Registro '{safe_filename}' eliminado.", "success")
    return redirect(url_for('index'))

@app.route('/salidas/<path:filename>')
def descargar(filename):
    return send_from_directory(SALIDAS_DIR, filename, as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True, port=5001)






