# agente.py
import os, re, json, time, random, sys
import pandas as pd
from openai import OpenAI
from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill

# --------- CONFIG GLOBAL ----------
MODEL_CHAT = "gpt-5-mini"  # mantener consistente con tu script final
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

CAMPAIGNS_STRUCTURE = {
    "SEM": {"headlines": (15, 30), "long_headlines": (5, 90), "short_description": (1, 60), "long_descriptions": (4, 90)},
    "MetaDemandGen": {"primary_texts": (4, 250), "headlines": (3, 30), "descriptions": (3, 30)},
    "MetaDemandCapture": {"primary_texts": (4, 250), "headlines": (3, 30), "descriptions": (3, 30)},
    "GoogleDemandGen": {"headlines": (5, 30), "short_description": (1, 60), "long_descriptions": (4, 90)},
    "GooglePMAX": {"headlines": (15, 30), "long_headlines": (5, 90), "short_description": (1, 60), "long_descriptions": (4, 90)}
}

MIN_CHARS_BY_FIELD = {"primary_texts": 200}

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

def _parse_langs(s: str):
    m = {"es","en","pt"}
    if not s: return ("es",)
    items = [t.strip().lower() for t in s.split(",") if t.strip()]
    items = [t for t in items if t in m]
    return tuple(sorted(set(items), key=items.index)) or ("es",)

# ---------- util / carga ----------
def cargar_referencias(path): return pd.read_excel(path, sheet_name="Copies")
def cargar_contenidos(path):  return pd.read_excel(path)
def cargar_planes(path):      return pd.read_excel(path)
def cargar_specs(path):       return pd.read_excel(path)

def _normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower().replace('\n',' ').replace('  ',' ').replace(' ','_') for c in df.columns]
    return df

def _ensure_columns(df: pd.DataFrame, required_cols: list, df_name: str = "DataFrame"):
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise KeyError(f"{df_name} no contiene las columnas requeridas: {missing}. Columns disponibles: {list(df.columns)}")

def _norm_base_name(plan_name: str) -> str:
    if not isinstance(plan_name, str): return ""
    return plan_name.lower().replace("monthly","").replace("annual","").strip()

def get_platform_plans_set(df_plans, platform):
    if 'platform' not in df_plans.columns: raise KeyError("'platform' no existe en df_plans")
    if 'plan_name' not in df_plans.columns: raise KeyError("'plan_name' no existe en df_plans")
    mask = df_plans['platform'].fillna("").str.lower() == platform.lower()
    dfp = df_plans[mask]
    base_names = {_norm_base_name(n) for n in dfp['plan_name'].dropna().astype(str)}
    full_names = set(dfp['plan_name'].dropna().astype(str))
    return base_names, full_names

def any_plan_matches_platform(plans_str, platform_base_names):
    if not isinstance(plans_str, str): return False
    listed = [p.strip() for p in plans_str.split(',') if p.strip()]
    return any(_norm_base_name(p) in platform_base_names for p in listed)

def _split_markets_cell(cell: str) -> list[str]:
    if not isinstance(cell, str): return []
    parts = []
    for token in cell.split(','):
        tok = token.strip()
        if tok: parts.append(tok)
    return parts

def get_default_markets_for_platform(df_plans: pd.DataFrame, platform: str) -> list[str]:
    mask = df_plans['platform'].fillna("").str.lower() == platform.lower()
    dfp = df_plans[mask]
    markets_set = set()
    for mcell in dfp['markets'].dropna().astype(str):
        for m in _split_markets_cell(mcell):
            if m: markets_set.add(m)
    if not markets_set:
        return ['US/CA','EUROPE','ROW']
    return sorted(markets_set)

# ---------- IA ----------
def chat_create(messages, model=None, max_retries=3, timeout=300, **kwargs):
    model = model or MODEL_CHAT
    attempt = 0
    last_err = None
    while attempt <= max_retries:
        try:
            resp = client.chat.completions.create(model=model, messages=messages, timeout=timeout, **kwargs)
            return resp
        except Exception as e:
            last_err = e
            wait = (2**attempt) + random.uniform(0,0.5)
            print(f"[chat_create] intento {attempt+1}/{max_retries+1} falló: {e}. Reintentando en {wait:.1f}s...")
            sys.stdout.flush()
            time.sleep(wait)
            attempt += 1
    raise last_err

def limpiar_json(texto):
    print("Intentando limpiar y decodificar la respuesta JSON...")
    start = texto.find('{'); end = texto.rfind('}') + 1
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

# ---------- búsqueda de contenido con filtro por plataforma y liga ----------
def obtener_info_contenido(campaign_name, brief, content_df, plans_df, platform, league_or_other):
    search_text = (campaign_name + " " + brief).lower()
    final_rows_df = pd.DataFrame()

    platform_base_names, _ = get_platform_plans_set(plans_df, platform)
    content_df = content_df.copy()
    # pre-filtro por plataforma en la columna plans_available
    mask_platform = content_df['plans_available'].apply(any_plan_matches_platform, platform_base_names=platform_base_names)
    content_df = content_df[mask_platform]

    # si eligieron una liga (no “Otro”), filtramos por content_name exacto
    if league_or_other and league_or_other.lower() != "otro":
        content_df = content_df[content_df['content_name'].fillna("").astype(str).str.lower() == league_or_other.lower()]

    if content_df.empty:
        print(f"ADVERTENCIA: no hay contenidos que matcheen plataforma='{platform}' y liga='{league_or_other}'.")
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
        final_rows_df = content_matched_rows if not content_matched_rows.empty else pd.DataFrame()

    if final_rows_df.empty:
        print("ADVERTENCIA: No se encontró contenido para esta campaña (con filtro de plataforma/league).")
    return final_rows_df

# ---------- preprocesamiento ----------
def _expand_batch(texts, min_chars, max_chars, lang='es'):
    idxs = [i for i, t in enumerate(texts) if isinstance(t, str) and t.strip() and len(t.strip()) < min_chars]
    if not idxs:
        return texts, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    bloques = "\n".join(f"Texto {i+1}: \"{texts[i]}\"" for i in idxs)

    if lang == 'en':
        system_message = "You are an expert copywriter who lengthens texts in English while staying under a max character limit."
        prompt_instructions = (f"Rewrite the following texts so that EACH output has AT LEAST {min_chars} and AT MOST {max_chars} characters. Keep meaning and tone. Return only the rewritten texts, one per line, WITHOUT prefixes or quotes.")
    elif lang == 'pt':
        system_message = "Você é um redator especialista que alonga textos em português dentro de um limite máximo de caracteres."
        prompt_instructions = (f"Reescreva para que CADA resultado tenha PELO MENOS {min_chars} e NO MÁXIMO {max_chars} caracteres. Retorne apenas os textos reescritos, um por linha, SEM prefixos ou aspas.")
    else:
        system_message = "Eres un redactor experto que expande textos en español sin superar un límite de caracteres."
        prompt_instructions = (f"Reescribe para que CADA salida tenga COMO MÍNIMO {min_chars} y COMO MÁXIMO {max_chars} caracteres. Devuelve solo los textos reescritos, uno por línea, SIN prefijos ni comillas.")

    resp = chat_create(
        model=MODEL_CHAT,
        messages=[{"role":"system","content":system_message},{"role":"user","content":f"{prompt_instructions}\n\n{bloques}"}]
    )
    usage = resp.usage or {}
    lines = [l.strip() for l in resp.choices[0].message.content.splitlines() if l.strip()]
    cleaned = []
    for line in lines:
        cl = re.sub(r'^\s*Texto\s*\d+:\s*"?', '', line)
        if cl.endswith('"'): cl = cl[:-1]
        cleaned.append(cl.strip())
    out = list(texts)
    for i, new in zip(idxs, cleaned):
        out[i] = new[:max_chars] if len(new) > max_chars else new
    return out, usage

def preparar_batch(texts, limit, tipo, lang='es'):
    df = pd.DataFrame({"Original": texts, "Reescrito": texts.copy()})
    usage_total = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}

    # recorte si exceden
    mask_long = df["Original"].fillna("").astype(str).str.len() > limit
    idxs_long = df[mask_long].index.tolist()
    if idxs_long:
        bloques = "\n".join(f'Texto {i+1}: "{df.at[i,"Original"]}"' for i in idxs_long)
        if lang == 'en':
            system_message = "You are an expert copywriter who shortens texts in English."
            prompt_instructions = f"Rewrite under {limit} characters, keep meaning. Return only the rewritten texts, one per line, WITHOUT prefixes or quotes."
        elif lang == 'pt':
            system_message = "Você é um redator especialista que encurta textos em português."
            prompt_instructions = f"Reescreva com menos de {limit} caracteres. Retorne um por linha, SEM prefixos nem aspas."
        else:
            system_message = "Eres un redactor experto que acorta textos en español."
            prompt_instructions = f"Reescribe con MENOS de {limit} caracteres. Devuelve uno por línea, SIN prefijos ni comillas."
        resp = chat_create(model=MODEL_CHAT, messages=[{"role":"system","content":system_message},{"role":"user","content":f"{prompt_instructions}\n\n{bloques}"}])
        usage = resp.usage or {}
        for k in usage_total: usage_total[k] += getattr(usage,k,0) if hasattr(usage,k) else usage.get(k,0)
        lines = [l.strip() for l in resp.choices[0].message.content.splitlines() if l.strip()]
        cleaned_lines = []
        for line in lines:
            cl = re.sub(r'^\s*Texto\s*\d+:\s*"?', '', line)
            if cl.endswith('"'): cl = cl[:-1]
            cleaned_lines.append(cl.strip())
        for i, new in zip(idxs_long, cleaned_lines):
            df.at[i,"Reescrito"] = new if len(new) <= limit else new[:limit]

    # expansión si corresponde
    min_chars = MIN_CHARS_BY_FIELD.get(tipo)
    if isinstance(min_chars, int) and min_chars > 0:
        current = df["Reescrito"].fillna("").astype(str).tolist()
        if any(t.strip() and len(t.strip()) < min_chars for t in current):
            expanded, usage2 = _expand_batch(current, min_chars, limit, lang=lang)
            for k in usage_total: usage_total[k] += getattr(usage2,k,0) if hasattr(usage2,k) else usage2.get(k,0)
            df["Reescrito"] = expanded

    return df["Reescrito"].tolist(), usage_total

def traducir_batch(texts, target):
    if not texts or all(not t for t in texts): return texts, {"prompt_tokens":0,"completion_tokens":0,"total_tokens":0}
    if target not in ("en","pt"):             return texts, {"prompt_tokens":0,"completion_tokens":0,"total_tokens":0}
    lang = "English (US)" if target == "en" else "Português (Brasil)"
    block = json.dumps(texts, ensure_ascii=False, indent=2)
    prompt = f"""
Eres un traductor profesional. Traduce la lista JSON al idioma {lang}.
NO abrevies ni uses '...'. Devuelve SOLO un objeto JSON con "translations".
{block}
""".strip()
    try:
        resp = chat_create(
            model=MODEL_CHAT,
            response_format={"type":"json_object"},
            messages=[{"role":"system","content":"You are an expert translator designed to output JSON."},
                      {"role":"user","content":prompt}]
        )
        raw = resp.choices[0].message.content
        return json.loads(raw).get("translations", texts), resp.usage
    except Exception as e:
        print(f"ERROR en traducción {target}: {e}.")
        return texts, {"prompt_tokens":0,"completion_tokens":0,"total_tokens":0}

# ---------- prompt ----------
def generar_prompt_multi(briefs, ref_df, content_info, plan_info, specs_df):
    # muestreo con >=2 primary_texts
    campo_col = 'campo' if 'campo' in ref_df.columns else ('Campo' if 'Campo' in ref_df.columns else None)
    N_EXAMPLES = min(5, len(ref_df))
    if campo_col:
        df_primary = ref_df[ref_df[campo_col].astype(str).str.lower() == 'primary_texts']
        take_primary = min(2, len(df_primary), N_EXAMPLES)
        primary_sample = df_primary.sample(n=take_primary, random_state=42) if take_primary>0 else ref_df.iloc[0:0]
        df_rest = ref_df.drop(primary_sample.index)
        remaining = N_EXAMPLES - len(primary_sample)
        rest_sample = df_rest.sample(n=min(remaining, len(df_rest)), random_state=43) if remaining>0 else df_rest.iloc[0:0]
        ejemplos = pd.concat([primary_sample, rest_sample], ignore_index=True)
        if len(ejemplos)>1: ejemplos = ejemplos.sample(frac=1, random_state=99).reset_index(drop=True)
    else:
        ejemplos = ref_df.sample(n=N_EXAMPLES, random_state=42)

    def _pick(r,*names):
        for n in names:
            if n in r.index: return r[n]
        return ""

    block_ej = "\n".join(
        f"- [{_pick(r,'market','Market')}][{_pick(r,'idioma','Idioma')}] {_pick(r,'platform','Platform')} {_pick(r,'tipo','Tipo')} {_pick(r,'campo','Campo')}: \"{_pick(r,'texto','Texto')}\""
        for _, r in ejemplos.iterrows()
    )

    template = {m: {c: {f: ([] if cnt>1 else "") for f,(cnt,_) in fields.items()} for c,fields in CAMPAIGNS_STRUCTURE.items()} for m in content_info['markets']}

    info = [f"Contenido: {content_info['content_name']}"]
    if content_info['details']: info.append(f"Detalles: {content_info['details']}")
    info.append(f"Idiomas: {', '.join(content_info['languages'])}")
    info.append("Planes y precios disponibles (USA EL SÍMBOLO DE MONEDA EXACTO QUE SE MUESTRA):")
    for m, pls in plan_info.items():
        plan_descriptions = []
        if not pls:
            desc = "Sin planes definidos"
        else:
            for p in pls:
                plan_name = p.get('plan_name','').strip()
                cur = str(p.get('currency_symbol','') or '').strip()
                price = str(p.get('price','') or '').strip()
                period = str(p.get('recurring_period','') or '').strip()
                price_info = f"{plan_name} {cur}{price}/{period}" if (cur and price and period) else (plan_name or "Plan")
                if p.get('has_discount') and p.get('marketing_discount'):
                    price_info += f" ¡EN OFERTA! ({p['marketing_discount']})"
                plan_descriptions.append(price_info)
            desc = "; ".join(plan_descriptions)
        info.append(f"- Mercado {m}: {desc}")

    valid_campaigns=set(CAMPAIGNS_STRUCTURE.keys())
    specs_filtered = specs_df[specs_df.get('campaign','').isin(valid_campaigns)] if 'campaign' in specs_df.columns else specs_df
    if 'platform' in specs_df.columns:
        specs_filtered = specs_filtered[specs_filtered['platform'].isin({"Google","Meta"})]
    specs = []
    for _, s in specs_filtered.iterrows():
        plat = str(s.get('platform','')).strip()
        camp = str(s.get('campaign','')).strip()
        qty  = int(s.get('quantity',0) or 0)
        title= str(s.get('title','')).strip()
        chars= int(s.get('characters',0) or 0)
        style= str(s.get('style','')).strip()
        det  = str(s.get('details','')).strip()
        obj  = str(s.get('objective','')).strip()
        specs.append(f"{plat} {camp}: genera {qty} {title} (máx {chars} car.); {style}; {det}; objetivo: {obj}")

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
- Límites de Caracteres: respeta estrictamente los límites y expande si <60%.
- Estilo: lenguaje emocional, metáforas de fútbol y urgencia.
- Demand Capture: mencionar descuentos/ofertas/precios cuando aplique.
- Plan anual: mencionar el descuento si existe.
- Moneda: usar EXACTAMENTE el símbolo indicado en 'Planes y precios'.
- Para "primary_texts": genera textos entre {MIN_CHARS_BY_FIELD.get('primary_texts',0)} y el máximo permitido del campo.
""".strip()
    return prompt

# ---------- Excel ----------
def generar_excel_multi(data, output_langs=("es",), filename="copies.xlsx"):
    rows, all_tasks = [], []
    total_usage = {"prompt_tokens":0,"completion_tokens":0}

    print("--- Iniciando Fase 1: ES ---")
    for market, market_data in data.items():
        for campaign, fields in CAMPAIGNS_STRUCTURE.items():
            plat = {'SEM':'Google','GoogleDemandGen':'Google','GooglePMAX':'Google','MetaDemandGen':'Meta','MetaDemandCapture':'Meta'}[campaign]
            tp   = {'SEM':'SEM','GoogleDemandGen':'DemandGen','GooglePMAX':'PMAX','MetaDemandGen':'DemandGen','MetaDemandCapture':'DemandCapture'}[campaign]
            campaign_data = market_data.get(campaign,{})
            for field,(count,limit) in fields.items():
                original_texts = campaign_data.get(field, [])
                if not isinstance(original_texts, list): original_texts = [original_texts]
                while len(original_texts) < count: original_texts.append("")
                if not any(t.strip() for t in original_texts):
                    es_texts = [""]*count
                    usage = {"prompt_tokens":0,"completion_tokens":0}
                else:
                    es_texts, usage = preparar_batch(original_texts, limit, field, lang='es')
                    u = usage if isinstance(usage, dict) else usage.dict()
                    total_usage["prompt_tokens"]  += u.get('prompt_tokens',0)
                    total_usage["completion_tokens"] += u.get('completion_tokens',0)
                all_tasks.append({"market":market,"platform":plat,"tipo":tp,"campo":field,"count":count,"limit":limit,"es_texts":es_texts})

    need_en = ("en" in output_langs); need_pt = ("pt" in output_langs)
    if need_en or need_pt:
        print("--- Iniciando Fase 2: Traducción ---")
        total_tasks = len(all_tasks)

    for idx, task in enumerate(all_tasks, start=1):
        es_texts, limit, campo = task['es_texts'], task['limit'], task['campo']
        en_texts, pt_texts = [], []
        has_content = any(t.strip() for t in es_texts)

        if need_en and has_content:
            en_texts_raw, en_usage1 = traducir_batch(es_texts, 'en')
            u1 = en_usage1 if isinstance(en_usage1, dict) else en_usage1.dict()
            total_usage["prompt_tokens"]  += u1.get('prompt_tokens',0)
            total_usage["completion_tokens"] += u1.get('completion_tokens',0)
            en_texts, en_usage2 = preparar_batch(en_texts_raw, limit, campo, lang='en')
            u2 = en_usage2 if isinstance(en_usage2, dict) else en_usage2.dict()
            total_usage["prompt_tokens"]  += u2.get('prompt_tokens',0)
            total_usage["completion_tokens"] += u2.get('completion_tokens',0)

        if need_pt and has_content:
            pt_texts_raw, pt_usage1 = traducir_batch(es_texts, 'pt')
            u3 = pt_usage1 if isinstance(pt_usage1, dict) else pt_usage1.dict()
            total_usage["prompt_tokens"]  += u3.get('prompt_tokens',0)
            total_usage["completion_tokens"] += u3.get('completion_tokens',0)
            pt_texts, pt_usage2 = preparar_batch(pt_texts_raw, limit, campo, lang='pt')
            u4 = pt_usage2 if isinstance(pt_usage2, dict) else pt_usage2.dict()
            total_usage["prompt_tokens"]  += u4.get('prompt_tokens',0)
            total_usage["completion_tokens"] += u4.get('completion_tokens',0)

        langs_for_rows = [('es', es_texts)]
        if need_en: langs_for_rows.append(('en', en_texts))
        if need_pt: langs_for_rows.append(('pt', pt_texts))

        for i in range(task['count']):
            for lang, texts in langs_for_rows:
                txt = texts[i] if i < len(texts) else ""
                if txt:
                    rows.append({
                        "Market": task['market'], "Platform": task['platform'], "Tipo": task['tipo'],
                        "Campo": task['campo'], "Título": f"{task['campo']} {i+1}",
                        "Idioma": lang, "Texto": txt, "Caracteres": len(txt),
                        "Max Caracteres": limit, "Check": 1 if len(txt) <= limit else 0
                    })

    df_master = pd.DataFrame(rows)
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        df_master.to_excel(writer, sheet_name='Todos los Copies', index=False)
        for (platform, tipo), df_campaign in df_master.groupby(['Platform','Tipo']):
            sheet_name = f"{platform} {tipo}"
            df_sorted = df_campaign.sort_values(by=['Market','Idioma','Campo'])
            df_sorted.to_excel(writer, sheet_name=sheet_name, index=False)

    wb = load_workbook(filename)
    if 'Todos los Copies' in wb.sheetnames:
        ws_master = wb['Todos los Copies']
        red_font = Font(color="9C0006"); red_fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
        for row in ws_master.iter_rows(min_row=2, max_row=ws_master.max_row):
            if row[8].value == 0:
                for cell in row: cell.font = red_font; cell.fill = red_fill
    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        for col in ws.columns:
            max_length = 0; column_letter = col[0].column_letter
            if ws[f"{column_letter}1"].value == 'Texto': ws.column_dimensions[column_letter].width = 50; continue
            for cell in col:
                try:
                    if len(str(cell.value)) > max_length: max_length = len(str(cell.value))
                except: pass
            ws.column_dimensions[column_letter].width = (max_length + 2)
    wb.save(filename)
    return total_usage

# -------- principal para la webapp --------
def generar_copies(
    campaign_name: str,
    campaign_brief: str,
    platform_name: str = "Fanatiz",
    langs_csv: str = "ES",
    league_selection: str = "Otro",
    output_filename: str = "copies_generadas.xlsx"
) -> tuple:
    """
    Genera copies con filtro por plataforma y liga.
    - league_selection: si es "Otro" o no hay match -> fallback a defaults de la plataforma.
    - langs_csv: "ES" o "ES,EN,PT"
    """
    print(f"Iniciando generación para '{campaign_name}' | Plataforma: {platform_name} | Liga: {league_selection} | Langs: {langs_csv}")
    OUTPUT_LANGS = _parse_langs(langs_csv)

    total_usage = {"prompt_tokens":0,"completion_tokens":0}
    base_path = os.path.abspath(os.path.dirname(__file__))
    df_refs    = _normalize_headers(cargar_referencias(os.path.join(base_path,"Mejor_Performing_Copies_Paid_Fanatiz.xlsx")))
    df_content = _normalize_headers(cargar_contenidos(os.path.join(base_path,"content_by_country.xlsx")))
    df_plans   = _normalize_headers(cargar_planes(os.path.join(base_path,"plans_and_pricing.xlsx")))
    df_specs   = _normalize_headers(cargar_specs(os.path.join(base_path,"platforms_and_campaigns_specs.xlsx")))

    _ensure_columns(df_plans,   ['platform','plan_name','markets','recurring_period'], 'plans_and_pricing')
    _ensure_columns(df_content, ['content_name','markets_available','plans_available','content_languages'], 'content_by_country')

    platform_base_names, _ = get_platform_plans_set(df_plans, platform_name)

    relevant_content_df = obtener_info_contenido(
        campaign_name, campaign_brief, df_content, df_plans, platform_name, league_selection
    )

    # fallback si está vacío o usuario eligió "Otro"
    if relevant_content_df.empty or (league_selection and league_selection.lower() == "otro"):
        print(f"\n⚠️ Fallback por plataforma '{platform_name}' (liga '{league_selection}')")
        default_plans = DEFAULT_PLANS_BY_PLATFORM.get(platform_name, [])
        default_markets = get_default_markets_for_platform(df_plans, platform_name)
        default_markets_str = ",".join(default_markets) if default_markets else "US/CA,EUROPE,ROW"
        relevant_content_df = pd.DataFrame({
            'content_name': [campaign_name],
            'markets_available': [default_markets_str],
            'plans_available': [",".join(default_plans)],
            'content_languages': [",".join([l.upper() for l in OUTPUT_LANGS])],
            'content_details': [f'Contenido general de {platform_name}']
        })

    # mercados a procesar (intersección con la plataforma)
    all_markets = set()
    relevant_content_df['markets_available'].dropna().str.split(',').apply(
        lambda lst: all_markets.update(item.strip() for item in lst if item.strip())
    )
    markets_to_process = sorted(list(all_markets))
    platform_markets = set(get_default_markets_for_platform(df_plans, platform_name))
    if platform_markets:
        markets_to_process = sorted(set(markets_to_process) & platform_markets)

    print(f"Mercados a procesar: {markets_to_process}")
    final_data_for_excel = {}

    def is_market_in_cell(available_markets, target_market):
        if not isinstance(available_markets, str): return False
        market_list = [m.strip().lower() for m in available_markets.split(',')]
        return target_market.lower() in market_list

    for market in markets_to_process:
        print("\n" + "="*20 + f" PROCESANDO {market} " + "="*20)
        market_content_df = relevant_content_df[relevant_content_df['markets_available'].str.contains(market, na=False)]
        content_names = ", ".join(market_content_df['content_name'].unique())
        plans_available = set()
        market_content_df['plans_available'].dropna().str.split(',').apply(lambda lst: plans_available.update(p.strip() for p in lst if p.strip()))
        # quedarse con planes de la plataforma
        plans_available = {p for p in plans_available if _norm_base_name(p) in platform_base_names}
        if not plans_available:
            print(f"  ⚠️ No quedaron planes válidos para la plataforma '{platform_name}' en '{market}'.")
            continue

        content_info = {"content_name": content_names, "languages": [l.upper() for l in OUTPUT_LANGS], "details": "", "markets": [market]}

        plan_info = {}
        matches = []
        print(f"  -> Buscando precios para: {sorted(list(plans_available))}")
        for plan_nom in sorted(list(plans_available)):
            m = re.match(r"(.+?)\s+(monthly|annual)$", plan_nom, flags=re.IGNORECASE)
            name, period = (m.group(1), m.group(2).capitalize()) if m else (plan_nom, None)
            mask = (
                (df_plans["platform"].fillna("").str.lower() == platform_name.lower()) &
                (df_plans["plan_name"].str.lower() == name.lower()) &
                (df_plans["markets"].apply(is_market_in_cell, target_market=market))
            )
            if period:
                mask &= (df_plans["recurring_period"].astype(str).str.lower() == period.lower())
            sel = df_plans[mask]
            if not sel.empty:
                matches.append(sel.iloc[0].to_dict())
        plan_info[market] = matches

        briefs = {
            'campaign_name': campaign_name,
            'campaign_brief': campaign_brief,
            'company': COMPANY_BY_PLATFORM.get(platform_name, platform_name),
            'company_context': '...',
            'value_proposition': '...',
            'extras': f'Plataforma objetivo: {platform_name}'
        }

        prompt = generar_prompt_multi(briefs, df_refs, content_info, plan_info, df_specs)
        resp = chat_create(model=MODEL_CHAT, messages=[{'role':'system','content':'You are a helpful assistant.'},{'role':'user','content':prompt}])
        total_usage["prompt_tokens"] += resp.usage.prompt_tokens
        total_usage["completion_tokens"] += resp.usage.completion_tokens

        market_data = limpiar_json(resp.choices[0].message.content)
        if market in market_data:
            final_data_for_excel[market] = market_data[market]
        else:
            print(f"ADVERTENCIA: la respuesta no contenía la clave '{market}'.")

    if not final_data_for_excel:
        return output_filename, "Error: no se generaron copies válidos."

    excel_usage = generar_excel_multi(final_data_for_excel, output_langs=_parse_langs(langs_csv), filename=output_filename)
    total_usage["prompt_tokens"] += excel_usage["prompt_tokens"]
    total_usage["completion_tokens"] += excel_usage["completion_tokens"]

    PRICE_PER_MILLION_INPUT = 0.250
    PRICE_PER_MILLION_OUTPUT = 2.000
    input_cost  = (total_usage["prompt_tokens"] / 1_000_000) * PRICE_PER_MILLION_INPUT
    output_cost = (total_usage["completion_tokens"] / 1_000_000) * PRICE_PER_MILLION_OUTPUT
    total_cost  = input_cost + output_cost

    summary = (
        f"📊 **Resumen de Consumo y Costo** 💰\n"
        f"-----------------------------------------\n"
        f"Modelo Utilizado: {MODEL_CHAT}\n"
        f"Tokens Entrada: {total_usage['prompt_tokens']:,}\n"
        f"Tokens Salida:  {total_usage['completion_tokens']:,}\n"
        f"**Tokens Totales:** {total_usage['prompt_tokens'] + total_usage['completion_tokens']:,}\n"
        f"-----------------------------------------\n"
        f"Costo Entrada: ${input_cost:.6f} USD\n"
        f"Costo Salida:  ${output_cost:.6f} USD\n"
        f"**Costo Total Estimado:** **${total_cost:.6f} USD**\n"
    )
    print(summary)
    print(f"¡Proceso completado! Archivo guardado en: {output_filename}")
    return output_filename, summary

