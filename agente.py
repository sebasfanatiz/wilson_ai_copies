# agente.py

# ==============================================================================
# 1. IMPORTACIONES
# ==============================================================================
import os
import re
import json
import time
import random
import sys
import pandas as pd
from openai import OpenAI
from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill


# ==============================================================================
# 2. CONFIGURACIÓN GLOBAL Y CONSTANTES
# ==============================================================================
# --- Configuración del Modelo y Cliente de OpenAI ---
MODEL_CHAT = "gpt-5-mini"
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# --- Estructura y Reglas de las Campañas ---
CAMPAIGNS_STRUCTURE = {
    "SEM": {"headlines": (15, 30), "long_headlines": (5, 90), "short_description": (1, 60), "long_descriptions": (4, 90)},
    "MetaDemandGen": {"primary_texts": (4, 250), "headlines": (3, 30), "descriptions": (3, 30)},
    "MetaDemandCapture": {"primary_texts": (4, 250), "headlines": (3, 30), "descriptions": (3, 30)},
    "GoogleDemandGen": {"headlines": (5, 30), "short_description": (1, 60), "long_descriptions": (4, 90)},
    "GooglePMAX": {"headlines": (15, 30), "long_headlines": (5, 90), "short_description": (1, 60), "long_descriptions": (4, 90)}
}

MIN_CHARS_BY_FIELD = {
    "primary_texts": 200
}

# --- Mapeos y Datos por Defecto ---
DEFAULT_PLANS_BY_PLATFORM = {
    "Fanatiz": ["Front Row Monthly", "Front Row Annual"],
    "L1MAX": ["L1MAX Full", "L1MAX Móvil"],
    "AFA Play": ["AFA Play", "AFA Play Annual"],
}

COMPANY_BY_PLATFORM = {
    "Fanatiz": "Fanatiz",
    "L1MAX": "L1MAX",
    "AFA Play": "AFA Play",
}


# ==============================================================================
# 3. FUNCIONES DE UTILIDAD (CARGA Y PREPROCESAMIENTO DE DATOS)
# ==============================================================================
# --- Carga de Archivos ---
def cargar_referencias(path): return pd.read_excel(path, sheet_name="Copies")
def cargar_contenidos(path):  return pd.read_excel(path)
def cargar_planes(path):      return pd.read_excel(path)
def cargar_specs(path):       return pd.read_excel(path)

# --- Normalización y Validación ---
def _normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    """ Limpia y estandariza los encabezados de un DataFrame. """
    df = df.copy()
    df.columns = [str(c).strip().lower().replace('\n',' ').replace('  ',' ').replace(' ','_') for c in df.columns]
    return df

def _ensure_columns(df: pd.DataFrame, required_cols: list, df_name: str = "DataFrame"):
    """ Verifica que un DataFrame contenga las columnas requeridas. """
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise KeyError(f"{df_name} no contiene las columnas requeridas: {missing}. Columns disponibles: {list(df.columns)}")

# --- Procesamiento de Cadenas de Texto ---
def _parse_langs(s: str) -> tuple:
    """ Parsea una cadena de idiomas separados por coma a una tupla ordenada. """
    m = {"es","en","pt"}
    if not s: return ("es",)
    items = [t.strip().lower() for t in s.split(",") if t.strip()]
    items = [t for t in items if t in m]
    return tuple(sorted(set(items), key=items.index)) or ("es",)

def _norm_base_name(plan_name: str) -> str:
    """ Normaliza el nombre de un plan eliminando sufijos de periodicidad. """
    if not isinstance(plan_name, str): return ""
    return plan_name.lower().replace("monthly","").replace("annual","").strip()

def _split_markets_cell(cell: str) -> list[str]:
    """ Divide una celda de mercados en una lista de mercados individuales. """
    if not isinstance(cell, str): return []
    parts = []
    for token in cell.split(','):
        tok = token.strip()
        if tok: parts.append(tok)
    return parts

def _seems_english(txt: str) -> bool:
    """ Heurística simple para detectar si un texto parece estar en inglés. """
    if not isinstance(txt, str): return False
    t = txt.lower()
    common_words = (" the ", " to ", " and ", " for ", " with ", " your ", " watch ")
    return sum(word in f" {t} " for word in common_words) >= 2


# ==============================================================================
# 4. FUNCIONES DE LÓGICA DE NEGOCIO (FILTRADO Y BÚSQUEDA)
# ==============================================================================
def get_platform_plans_set(df_plans, platform):
    """ Obtiene los nombres de planes base y completos para una plataforma. """
    if 'platform' not in df_plans.columns: raise KeyError("'platform' no existe en df_plans")
    if 'plan_name' not in df_plans.columns: raise KeyError("'plan_name' no existe en df_plans")
    mask = df_plans['platform'].fillna("").str.lower() == platform.lower()
    dfp = df_plans[mask]
    base_names = {_norm_base_name(n) for n in dfp['plan_name'].dropna().astype(str)}
    full_names = set(dfp['plan_name'].dropna().astype(str))
    return base_names, full_names

def any_plan_matches_platform(plans_str, platform_base_names):
    """ Verifica si algún plan en una cadena coincide con los planes base de una plataforma. """
    if not isinstance(plans_str, str): return False
    listed_plans = [p.strip() for p in plans_str.split(',') if p.strip()]
    return any(_norm_base_name(p) in platform_base_names for p in listed_plans)

def get_default_markets_for_platform(df_plans: pd.DataFrame, platform: str) -> list[str]:
    """ Obtiene la lista de mercados por defecto para una plataforma. """
    mask = df_plans['platform'].fillna("").str.lower() == platform.lower()
    dfp = df_plans[mask]
    markets_set = set()
    for mcell in dfp['markets'].dropna().astype(str):
        for m in _split_markets_cell(mcell):
            if m: markets_set.add(m)
    if not markets_set:
        return ['US/CA','EUROPE','ROW'] # Fallback
    return sorted(markets_set)

def obtener_info_contenido(campaign_name, brief, content_df, plans_df, platform, league_or_other):
    """ Busca y filtra el contenido relevante para una campaña específica. """
    search_text = (campaign_name + " " + brief).lower()
    final_rows_df = pd.DataFrame()
    platform_base_names, _ = get_platform_plans_set(plans_df, platform)

    content_df = content_df.copy()
    # Pre-filtro por plataforma usando los planes disponibles
    mask_platform = content_df['plans_available'].apply(any_plan_matches_platform, platform_base_names=platform_base_names)
    content_df = content_df[mask_platform]

    # Filtro por liga si se especifica
    if league_or_other and league_or_other.lower() != "otro":
        content_df = content_df[content_df['content_name'].fillna("").astype(str).str.lower() == league_or_other.lower()]

    if content_df.empty:
        print(f"ADVERTENCIA: No hay contenidos que coincidan con plataforma='{platform}' y liga='{league_or_other}'.")
        return final_rows_df

    print("Iniciando búsqueda de contenido... (Paso 1: Buscando por nombre de plan)")
    def check_plan_in_text(plans_str, text_to_search):
        if not isinstance(plans_str, str): return False
        full_plans = [p.strip().lower() for p in plans_str.split(',') if p.strip()]
        base_plans = {_norm_base_name(p) for p in full_plans}
        return any((bp in text_to_search) and (bp in platform_base_names) for bp in base_plans if bp)

    mask_plan = content_df['plans_available'].apply(check_plan_in_text, text_to_search=search_text)
    plan_matched_rows = content_df[mask_plan]

    if not plan_matched_rows.empty:
        print(f"Coincidencia por plan encontrada en {len(plan_matched_rows)} fila(s).")
        print("  -> Afinando búsqueda por nombre de contenido...")
        mask_content_refine = plan_matched_rows['content_name'].str.lower().apply(lambda name: name in search_text if pd.notna(name) else False)
        secondary_matched_rows = plan_matched_rows[mask_content_refine]
        final_rows_df = secondary_matched_rows if not secondary_matched_rows.empty else plan_matched_rows
    else:
        print("Búsqueda por plan no exitosa. (Paso 2: Buscando por nombre de contenido como fallback)")
        mask_content_fallback = content_df['content_name'].str.lower().apply(lambda name: name in search_text if pd.notna(name) else False)
        content_matched_rows = content_df[mask_content_fallback]
        final_rows_df = content_matched_rows

    if final_rows_df.empty:
        print("ADVERTENCIA: No se encontró contenido para esta campaña (con filtro de plataforma/liga).")
    return final_rows_df


# ==============================================================================
# 5. FUNCIONES DE INTERACCIÓN CON LA IA
# ==============================================================================
def chat_create(messages, model=None, max_retries=3, timeout=300, **kwargs):
    """ Realiza una llamada a la API de Chat de OpenAI con reintentos. """
    model = model or MODEL_CHAT
    attempt = 0
    last_err = None
    while attempt <= max_retries:
        try:
            resp = client.chat.completions.create(model=model, messages=messages, timeout=timeout, **kwargs)
            return resp
        except Exception as e:
            last_err = e
            wait = (2**attempt) + random.uniform(0, 0.5)
            print(f"[chat_create] Intento {attempt+1}/{max_retries+1} falló: {e}. Reintentando en {wait:.1f}s...")
            sys.stdout.flush()
            time.sleep(wait)
            attempt += 1
    raise last_err

def limpiar_json(texto: str) -> dict:
    """ Extrae y decodifica un objeto JSON de una cadena de texto. """
    print("Intentando limpiar y decodificar la respuesta JSON...")
    start = texto.find('{')
    end = texto.rfind('}') + 1
    if start == -1 or end == 0:
        print("ADVERTENCIA: No se encontró un objeto JSON válido en la respuesta de la IA.")
        return {}
    json_str = texto[start:end]
    try:
        return json.loads(json_str, strict=False)
    except json.JSONDecodeError as e:
        print(f"ERROR decodificando JSON: {e}")
        print(f"Respuesta problemática: {json_str}")
        return {}

def _expand_batch(texts: list, min_chars: int, max_chars: int, lang: str = 'es') -> tuple:
    """ Expande textos que no cumplen con un mínimo de caracteres usando la IA. """
    idxs_to_expand = [i for i, t in enumerate(texts) if isinstance(t, str) and t.strip() and len(t.strip()) < min_chars]
    if not idxs_to_expand:
        return texts, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    
    bloques = "\n".join(f"Texto {i+1}: \"{texts[i]}\"" for i in idxs_to_expand)
    prompts_by_lang = {
        'en': ("You are an expert copywriter who lengthens texts in English while staying under a max character limit.",
               f"Rewrite the following texts so that EACH output has AT LEAST {min_chars} and AT MOST {max_chars} characters. Return only the rewritten texts, one per line, WITHOUT prefixes or quotes."),
        'pt': ("Você é um redator especialista que alonga textos em português dentro de um limite máximo de caracteres.",
               f"Reescreva para que CADA resultado tenha PELO MENOS {min_chars} e NO MÁXIMO {max_chars} caracteres. Retorne apenas os textos reescritos, um por linha, SEM prefixos ou aspas."),
        'es': ("Eres un redactor experto que expande textos en español sin superar un límite de caracteres.",
               f"Reescribe para que CADA salida tenga COMO MÍNIMO {min_chars} y COMO MÁXIMO {max_chars} caracteres. Devuelve solo los textos reescritos, uno por línea, SIN prefijos ni comillas.")
    }
    system_message, prompt_instructions = prompts_by_lang.get(lang, prompts_by_lang['es'])
    
    resp = chat_create(model=MODEL_CHAT, messages=[{"role": "system", "content": system_message}, {"role": "user", "content": f"{prompt_instructions}\n\n{bloques}"}])
    usage = resp.usage or {}
    lines = [l.strip() for l in resp.choices[0].message.content.splitlines() if l.strip()]
    
    cleaned_lines = []
    for line in lines:
        cleaned_line = re.sub(r'^\s*Texto\s*\d+:\s*"?', '', line).removesuffix('"').strip()
        cleaned_lines.append(cleaned_line)
        
    output_texts = list(texts)
    for i, new_text in zip(idxs_to_expand, cleaned_lines):
        output_texts[i] = new_text[:max_chars] if len(new_text) > max_chars else new_text
        
    return output_texts, usage

def preparar_batch(texts: list, limit: int, tipo: str, lang: str = 'es') -> tuple:
    """ Acorta o expande textos en un lote para cumplir con los límites de caracteres. """
    df = pd.DataFrame({"Original": texts, "Reescrito": texts.copy()})
    total_usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}

    # 1. Acortar textos que exceden el límite
    idxs_long = df[df["Original"].fillna("").astype(str).str.len() > limit].index.tolist()
    if idxs_long:
        bloques = "\n".join(f'Texto {i+1}: "{df.at[i, "Original"]}"' for i in idxs_long)
        prompts_by_lang = {
            'en': ("You are an expert copywriter who shortens texts in English.", f"Rewrite under {limit} characters. Return only the rewritten texts, one per line, WITHOUT prefixes or quotes."),
            'pt': ("Você é um redator especialista que encurta textos em português.", f"Reescreva com menos de {limit} caracteres. Retorne um por linha, SEM prefixos nem aspas."),
            'es': ("Eres un redactor experto que acorta textos en español.", f"Reescribe con MENOS de {limit} caracteres. Devuelve uno por línea, SIN prefijos ni comillas.")
        }
        system_message, prompt_instructions = prompts_by_lang.get(lang, prompts_by_lang['es'])
        
        resp = chat_create(model=MODEL_CHAT, messages=[{"role": "system", "content": system_message}, {"role": "user", "content": f"{prompt_instructions}\n\n{bloques}"}])
        usage = resp.usage or {}
        for k in total_usage: total_usage[k] += getattr(usage, k, 0)
        
        lines = [l.strip() for l in resp.choices[0].message.content.splitlines() if l.strip()]
        cleaned_lines = [re.sub(r'^\s*Texto\s*\d+:\s*"?', '', line).removesuffix('"').strip() for line in lines]
        
        for i, new_text in zip(idxs_long, cleaned_lines):
            df.at[i, "Reescrito"] = new_text[:limit]

    # 2. Expandir textos que no cumplen el mínimo (si aplica)
    min_chars = MIN_CHARS_BY_FIELD.get(tipo)
    if isinstance(min_chars, int) and min_chars > 0:
        current_texts = df["Reescrito"].fillna("").astype(str).tolist()
        if any(t.strip() and len(t.strip()) < min_chars for t in current_texts):
            expanded, usage2 = _expand_batch(current_texts, min_chars, limit, lang=lang)
            for k in total_usage: total_usage[k] += getattr(usage2, k, 0)
            df["Reescrito"] = expanded
            
    return df["Reescrito"].tolist(), total_usage

def traducir_batch(texts: list, target_lang: str) -> tuple:
    """ Traduce un lote de textos a un idioma objetivo usando la IA. """
    if not texts or all(not t for t in texts) or target_lang not in ("en", "pt"):
        return texts, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        
    lang_name = "English (US)" if target_lang == "en" else "Português (Brasil)"
    block = json.dumps(texts, ensure_ascii=False, indent=2)
    prompt = f'Eres un traductor profesional. Traduce la lista JSON al idioma {lang_name}. NO abrevies. Devuelve SÓLO un objeto JSON con la clave "translations".\n{block}'
    
    try:
        resp = chat_create(
            model=MODEL_CHAT,
            response_format={"type": "json_object"},
            messages=[{"role": "system", "content": "You are an expert translator designed to output JSON."}, {"role": "user", "content": prompt}]
        )
        raw_content = resp.choices[0].message.content
        return json.loads(raw_content).get("translations", texts), resp.usage
    except Exception as e:
        print(f"ERROR en traducción a '{target_lang}': {e}.")
        return texts, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}


# ==============================================================================
# 6. FUNCIÓN DE GENERACIÓN DE PROMPT
# ==============================================================================
def generar_prompt_multi(briefs, ref_df, content_info, plan_info, specs_df, base_lang='es'):
    """ Construye el prompt principal para la generación de copies. """
    # Muestreo de ejemplos de referencia, priorizando 'primary_texts'
    N_EXAMPLES = min(5, len(ref_df))
    campo_col = next((c for c in ['campo', 'Campo'] if c in ref_df.columns), None)
    
    if campo_col:
        df_primary = ref_df[ref_df[campo_col].astype(str).str.lower() == 'primary_texts']
        take_primary = min(2, len(df_primary), N_EXAMPLES)
        primary_sample = df_primary.sample(n=take_primary, random_state=42) if take_primary > 0 else pd.DataFrame()
        df_rest = ref_df.drop(primary_sample.index)
        remaining = N_EXAMPLES - len(primary_sample)
        rest_sample = df_rest.sample(n=min(remaining, len(df_rest)), random_state=43) if remaining > 0 else pd.DataFrame()
        ejemplos = pd.concat([primary_sample, rest_sample]).sample(frac=1, random_state=99).reset_index(drop=True)
    else:
        ejemplos = ref_df.sample(n=N_EXAMPLES, random_state=42)

    def _pick(row, *names): return next((row[n] for n in names if n in row.index), "")
    
    block_ej = "\n".join(
        f"- [{_pick(r,'market','Market')}][{_pick(r,'idioma','Idioma')}] {_pick(r,'platform','Platform')} {_pick(r,'tipo','Tipo')} {_pick(r,'campo','Campo')}: \"{_pick(r,'texto','Texto')}\""
        for _, r in ejemplos.iterrows()
    )

    template = {m: {c: {f: ([] if cnt > 1 else "") for f, (cnt, _) in fields.items()} for c, fields in CAMPAIGNS_STRUCTURE.items()} for m in content_info['markets']}
    
    info = [
        f"Contenido: {content_info['content_name']}",
        f"Detalles: {content_info['details']}" if content_info['details'] else None,
        f"Idioma base de redacción: {base_lang.upper()}",
        "Planes y precios disponibles (USA EL SÍMBOLO DE MONEDA EXACTO):"
    ]
    info = [i for i in info if i is not None]

    for m, pls in plan_info.items():
        if not pls:
            info.append(f"- Mercado {m}: Sin planes definidos")
            continue
        plan_descs = []
        for p in pls:
            price_info = f"{p.get('plan_name','').strip()} {p.get('currency_symbol','').strip()}{p.get('price','').strip()}/{p.get('recurring_period','').strip()}"
            if p.get('has_discount') and p.get('marketing_discount'):
                price_info += f" ¡EN OFERTA! ({p['marketing_discount']})"
            plan_descs.append(price_info)
        info.append(f"- Mercado {m}: {'; '.join(plan_descs)}")
    
    specs = []
    if 'campaign' in specs_df.columns and 'platform' in specs_df.columns:
        valid_campaigns = set(CAMPAIGNS_STRUCTURE.keys())
        specs_filtered = specs_df[
            specs_df['campaign'].isin(valid_campaigns) &
            specs_df['platform'].isin({"Google", "Meta"})
        ]
        for _, s in specs_filtered.iterrows():
            specs.append(f"{s.get('platform','')} {s.get('campaign','')} "
                         f"genera {s.get('quantity',0)} {s.get('title','')} (máx {s.get('characters',0)} car.); "
                         f"{s.get('style','')}; {s.get('details','')}; objetivo: {s.get('objective','')}")

    prompt = f"""
Eres un generador experto de copies para marketing digital.

Empresa: {briefs['company']}
Campaña: {briefs['campaign_name']}
Brief: {briefs['campaign_brief']}
Extras: {briefs['extras']}

{chr(10).join(info)}

Devuelve SÓLO un JSON con la estructura exacta:
{json.dumps(template, ensure_ascii=False, indent=2)}

Ejemplos de copies exitosos:
{block_ej}

Especificaciones detalladas por campaña:
{chr(10).join(specs)}

Reglas Fundamentales:
- Límites de Caracteres: respeta estrictamente los límites y expande si son muy cortos.
- Estilo: lenguaje emocional, metáforas de fútbol y urgencia.
- Demand Capture: mencionar descuentos/ofertas/precios cuando aplique.
- Plan anual: mencionar el descuento si existe.
- Moneda: usar EXACTAMENTE el símbolo indicado.
- Para "primary_texts": genera textos entre {MIN_CHARS_BY_FIELD.get('primary_texts',0)} y el máximo permitido.
- Idioma base: escribe TODOS los textos exclusivamente en Español (ES), sin mezclar idiomas. Las traducciones se harán después.
""".strip()
    return prompt


# ==============================================================================
# 7. FUNCIÓN DE GENERACIÓN DE ARCHIVO EXCEL
# ==============================================================================
def generar_excel_multi(data: dict, output_langs: tuple = ("es",), filename: str = "copies.xlsx") -> dict:
    """ Procesa los datos generados, traduce y crea un archivo Excel. """
    all_tasks = []
    total_usage = {"prompt_tokens": 0, "completion_tokens": 0}

    # Fase 1: Preparar textos en español
    print("--- Iniciando Fase 1: Procesamiento de copies en ES ---")
    for market, market_data in data.items():
        for campaign, fields in CAMPAIGNS_STRUCTURE.items():
            plat = {'SEM':'Google','GoogleDemandGen':'Google','GooglePMAX':'Google','MetaDemandGen':'Meta','MetaDemandCapture':'Meta'}[campaign]
            tp   = {'SEM':'SEM','GoogleDemandGen':'DemandGen','GooglePMAX':'PMAX','MetaDemandGen':'DemandGen','MetaDemandCapture':'DemandCapture'}[campaign]
            campaign_data = market_data.get(campaign, {})
            for field, (count, limit) in fields.items():
                original_texts = campaign_data.get(field, [])
                if not isinstance(original_texts, list): original_texts = [original_texts]
                original_texts.extend([""] * (count - len(original_texts)))

                if not any(t.strip() for t in original_texts):
                    es_texts, usage = [""] * count, {}
                else:
                    es_texts, usage = preparar_batch(original_texts, limit, field, lang='es')
                    if any(_seems_english(t) for t in es_texts):
                        es_texts, usage_fix = preparar_batch(es_texts, limit, field, lang='es') # Forzar reescritura en ES
                        for k in total_usage: total_usage[k] += getattr(usage_fix, k, 0)
                
                for k in total_usage: total_usage[k] += getattr(usage, k, 0)
                all_tasks.append({"market": market, "platform": plat, "tipo": tp, "campo": field, "count": count, "limit": limit, "es_texts": es_texts})

    # Fase 2: Traducción
    need_en = "en" in output_langs
    need_pt = "pt" in output_langs
    if need_en or need_pt:
        print("--- Iniciando Fase 2: Traducción ---")

    rows = []
    for task in all_tasks:
        es_texts, limit, campo = task['es_texts'], task['limit'], task['campo']
        en_texts, pt_texts = [], []
        has_content = any(t.strip() for t in es_texts)

        if need_en and has_content:
            en_texts_raw, en_usage1 = traducir_batch(es_texts, 'en')
            en_texts, en_usage2 = preparar_batch(en_texts_raw, limit, campo, lang='en')
            for k in total_usage: total_usage[k] += getattr(en_usage1, k, 0) + getattr(en_usage2, k, 0)
            
        if need_pt and has_content:
            pt_texts_raw, pt_usage1 = traducir_batch(es_texts, 'pt')
            pt_texts, pt_usage2 = preparar_batch(pt_texts_raw, limit, campo, lang='pt')
            for k in total_usage: total_usage[k] += getattr(pt_usage1, k, 0) + getattr(pt_usage2, k, 0)

        langs_map = {'es': es_texts}
        if need_en: langs_map['en'] = en_texts
        if need_pt: langs_map['pt'] = pt_texts
        
        for lang, texts in langs_map.items():
            for i in range(task['count']):
                txt = texts[i] if i < len(texts) else ""
                if txt:
                    rows.append({
                        "Market": task['market'], "Platform": task['platform'], "Tipo": task['tipo'],
                        "Campo": task['campo'], "Título": f"{task['campo']} {i+1}", "Idioma": lang,
                        "Texto": txt, "Caracteres": len(txt), "Max Caracteres": limit,
                        "Check": 1 if len(txt) <= limit else 0
                    })
    
    # Creación del archivo Excel
    df_master = pd.DataFrame(rows)
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        df_master.to_excel(writer, sheet_name='Todos los Copies', index=False)
        for (platform, tipo), df_group in df_master.groupby(['Platform', 'Tipo']):
            df_group.sort_values(by=['Market', 'Idioma', 'Campo']).to_excel(writer, sheet_name=f"{platform} {tipo}", index=False)

    # Formateo del archivo Excel
    wb = load_workbook(filename)
    red_font = Font(color="9C0006")
    red_fill = PatternFill(fill_type="solid", start_color="FFC7CE", end_color="FFC7CE")
    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        for col in ws.columns:
            column_letter = col[0].column_letter
            if ws[f"{column_letter}1"].value == 'Texto':
                ws.column_dimensions[column_letter].width = 50
                continue
            max_length = max(len(str(cell.value)) for cell in col if cell.value is not None)
            ws.column_dimensions[column_letter].width = max_length + 2

        if sheet_name == 'Todos los Copies':
            for row in ws.iter_rows(min_row=2):
                if row[9].value == 0:  # Columna 'Check'
                    for cell in row:
                        cell.font = red_font
                        cell.fill = red_fill
    wb.save(filename)
    return total_usage


# ==============================================================================
# 8. FUNCIÓN PRINCIPAL (ORQUESTADOR)
# ==============================================================================
def generar_copies(
    campaign_name: str,
    campaign_brief: str,
    platform_name: str = "Fanatiz",
    langs_csv: str = "ES",
    league_selection: str = "Otro",
    output_filename: str = "copies_generadas.xlsx",
    markets_selected: list[str] | None = None
) -> tuple:
    """
    Función principal que orquesta la generación de copies, desde la carga de
    datos hasta la creación del archivo Excel final.
    """
    print(f"Iniciando generación para '{campaign_name}' | Plataforma: {platform_name} | Liga: {league_selection} | Langs: {langs_csv}")
    
    # --- 1. Carga y Preparación de Datos ---
    total_usage = {"prompt_tokens": 0, "completion_tokens": 0}
    base_path = os.path.abspath(os.path.dirname(__file__))
    df_refs = _normalize_headers(cargar_referencias(os.path.join(base_path, "Mejor_Performing_Copies_Paid_Fanatiz.xlsx")))
    df_content = _normalize_headers(cargar_contenidos(os.path.join(base_path, "content_by_country.xlsx")))
    df_plans = _normalize_headers(cargar_planes(os.path.join(base_path, "plans_and_pricing.xlsx")))
    df_specs = _normalize_headers(cargar_specs(os.path.join(base_path, "platforms_and_campaigns_specs.xlsx")))

    _ensure_columns(df_plans, ['platform', 'plan_name', 'markets', 'recurring_period'], 'plans_and_pricing')
    _ensure_columns(df_content, ['content_name', 'markets_available', 'plans_available', 'content_languages'], 'content_by_country')

    # --- 2. Búsqueda y Filtrado de Contenido Relevante ---
    relevant_content_df = obtener_info_contenido(campaign_name, campaign_brief, df_content, df_plans, platform_name, league_selection)
    
    # Fallback si no se encuentra contenido
    if relevant_content_df.empty or (league_selection and league_selection.lower() == "otro"):
        print(f"\n⚠️ Fallback a contenido por defecto para la plataforma '{platform_name}' (liga '{league_selection}')")
        relevant_content_df = pd.DataFrame({
            'content_name': [campaign_name],
            'markets_available': [",".join(get_default_markets_for_platform(df_plans, platform_name))],
            'plans_available': [",".join(DEFAULT_PLANS_BY_PLATFORM.get(platform_name, []))],
            'content_languages': [langs_csv.upper()],
            'content_details': [f'Contenido general de {platform_name}']
        })
    
    # --- 3. Determinación de Mercados a Procesar ---
    all_markets = {item.strip() for m_list in relevant_content_df['markets_available'].dropna().str.split(',') for item in m_list if item.strip()}
    platform_markets = set(get_default_markets_for_platform(df_plans, platform_name))
    markets_to_process = sorted(list(all_markets & platform_markets)) if platform_markets else sorted(list(all_markets))

    print(f"Mercados potenciales: {markets_to_process}")
    
    if markets_selected:
        markets_selected_norm = {m.strip().upper() for m in markets_selected}
        markets_to_process = [m for m in markets_to_process if m.upper() in markets_selected_norm]
        print(f"Mercados filtrados por selección del usuario: {markets_to_process}")

    if not markets_to_process:
        msg = "Error: no se encontraron mercados válidos para procesar tras aplicar los filtros."
        print(f"⚠️ {msg}")
        return output_filename, msg

    # --- 4. Generación de Copies por Mercado ---
    final_data_for_excel = {}
    platform_base_names, _ = get_platform_plans_set(df_plans, platform_name)

    for market in markets_to_process:
        print("\n" + "="*20 + f" PROCESANDO MERCADO: {market} " + "="*20)
        
        market_content_df = relevant_content_df[relevant_content_df['markets_available'].str.contains(market, na=False, case=False)]
        
        plans_in_content = {p.strip() for p_list in market_content_df['plans_available'].dropna().str.split(',') for p in p_list if p.strip()}
        valid_plans = {p for p in plans_in_content if _norm_base_name(p) in platform_base_names}

        if not valid_plans:
            print(f"  ⚠️ No se encontraron planes válidos para '{platform_name}' en '{market}'. Saltando...")
            continue

        content_info = {
            "content_name": ", ".join(market_content_df['content_name'].unique()),
            "details": "", "markets": [market]
        }

        # Búsqueda de información de precios
        plan_info_market = []
        for plan_name in sorted(list(valid_plans)):
            mask = (
                (df_plans["platform"].fillna("").str.lower() == platform_name.lower()) &
                (df_plans["plan_name"].str.contains(plan_name.split()[0], case=False, na=False)) &
                (df_plans["markets"].str.contains(market, case=False, na=False))
            )
            if "annual" in plan_name.lower(): mask &= (df_plans["recurring_period"].str.lower() == "annual")
            if "monthly" in plan_name.lower(): mask &= (df_plans["recurring_period"].str.lower() == "monthly")
            
            sel = df_plans[mask]
            if not sel.empty: plan_info_market.append(sel.iloc[0].to_dict())
        
        briefs = {
            'campaign_name': campaign_name, 'campaign_brief': campaign_brief,
            'company': COMPANY_BY_PLATFORM.get(platform_name, platform_name), 'extras': f'Plataforma objetivo: {platform_name}'
        }

        prompt = generar_prompt_multi(briefs, df_refs, content_info, {market: plan_info_market}, df_specs, base_lang='es')
        
        resp = chat_create(model=MODEL_CHAT, messages=[{'role': 'system', 'content': 'You are a helpful assistant.'}, {'role': 'user', 'content': prompt}])
        total_usage["prompt_tokens"] += resp.usage.prompt_tokens
        total_usage["completion_tokens"] += resp.usage.completion_tokens

        market_data = limpiar_json(resp.choices[0].message.content)
        if market in market_data:
            final_data_for_excel[market] = market_data[market]
        else:
            print(f"ADVERTENCIA: La respuesta de la IA no contenía la clave esperada '{market}'.")

    # --- 5. Creación del Archivo de Salida y Resumen de Costos ---
    if not final_data_for_excel:
        return output_filename, "Error: No se generaron copies válidos después de procesar todos los mercados."

    excel_usage = generar_excel_multi(final_data_for_excel, output_langs=_parse_langs(langs_csv), filename=output_filename)
    total_usage["prompt_tokens"] += excel_usage["prompt_tokens"]
    total_usage["completion_tokens"] += excel_usage["completion_tokens"]

    # Cálculo de costos
    PRICE_PER_MILLION_INPUT = 0.250
    PRICE_PER_MILLION_OUTPUT = 2.000
    input_cost = (total_usage["prompt_tokens"] / 1_000_000) * PRICE_PER_MILLION_INPUT
    output_cost = (total_usage["completion_tokens"] / 1_000_000) * PRICE_PER_MILLION_OUTPUT
    total_cost = input_cost + output_cost

    summary = (
        f"🧾 **Campaña:** {campaign_name}\n"
        f"📝 **Brief:** {campaign_brief}\n\n"
        f"📊 **Resumen de Consumo y Costo** 💰\n"
        f"-----------------------------------------\n"
        f"Tokens Entrada: {total_usage['prompt_tokens']:,}\n"
        f"Tokens Salida:  {total_usage['completion_tokens']:,}\n"
        f"**Tokens Totales:** {total_usage['prompt_tokens'] + total_usage['completion_tokens']:,}\n"
        f"Costo Total Estimado: **${total_cost:.4f} USD**\n"
    )
    print(summary)
    print(f"¡Proceso completado! Archivo guardado en: {output_filename}")
    return output_filename, summary
