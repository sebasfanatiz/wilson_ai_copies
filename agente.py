import os
import re
import json
import pandas as pd
import time
from openai import OpenAI
from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill

# --- CONFIGURACIÓN GLOBAL ---
# Inicializa el cliente de OpenAI
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
MODEL_CHAT = "gpt-4o"  # Se recomienda gpt-4o para máxima calidad

# Estructura de campañas (constante)
CAMPAIGNS_STRUCTURE = {
    "SEM": {"headlines": (15, 30), "long_headlines": (5, 90), "short_description": (1, 60), "long_descriptions": (4, 90)},
    "MetaDemandGen": {"primary_texts": (4, 250), "headlines": (3, 30), "descriptions": (3, 30)},
    "MetaDemandCapture": {"primary_texts": (4, 250), "headlines": (3, 30), "descriptions": (3, 30)},
    "GoogleDemandGen": {"headlines": (5, 30), "short_description": (1, 60), "long_descriptions": (4, 90)},
    "GooglePMAX": {"headlines": (15, 30), "long_headlines": (5, 90), "short_description": (1, 60), "long_descriptions": (4, 90)}
}

# ---------------------------
# --- FUNCIONES AUXILIARES ---
# ---------------------------

def cargar_referencias(path): return pd.read_excel(path, sheet_name="Copies")
def cargar_contenidos(path): return pd.read_excel(path)
def cargar_planes(path): return pd.read_excel(path)
def cargar_specs(path): return pd.read_excel(path)

def obtener_info_contenido(brief, content_df, plans_df):
    # (Esta función no necesita cambios)
    brief_lower = brief.lower()
    filas = content_df[content_df["content_name"].str.lower().str.contains(brief_lower, na=False)]
    if filas.empty:
        filas = content_df[content_df["content_country"].str.lower().str.contains(brief_lower, na=False)]
    if filas.empty:
        filas = content_df.head(1)
    row = filas.iloc[0]
    content_info = { "content_name": row["content_name"], "languages": [l.strip() for l in str(row.get("content_languages","")).split(",") if l.strip()], "details": row.get("content_details",""), "markets": [m.strip() for m in str(row.get("markets_available","")).split(",") if m.strip()] }
    plans_list = [p.strip() for p in str(row.get("plans_available","")).split(",") if p.strip()]
    plan_info = {}
    for market in content_info["markets"]:
        matches = []
        for plan_nom in plans_list:
            m = re.match(r"(.+?)\s+(monthly|annual)$", plan_nom, flags=re.IGNORECASE)
            name, period = (m.group(1), m.group(2).capitalize()) if m else (plan_nom, None)
            mask = plans_df["plan_name"].str.lower() == name.lower()
            if period: mask &= plans_df["recurring_period"] == period
            sel = plans_df[mask]
            if not sel.empty:
                p = sel.iloc[0]
                matches.append({ "plan_name": p["plan_name"], "recurring_period": p["recurring_period"], "price": p["price"], "currency": p["currency"], "currency_symbol": p["currency_symbol"], "has_discount": p["has_discount"], "marketing_discount": p["marketing_discount"] })
        plan_info[market] = matches
    return content_info, plan_info

def limpiar_json(texto):
    start, end = texto.find('{'), texto.rfind('}')+1
    return json.loads(texto[start:end], strict=False)

def preparar_batch(texts, limit, tipo, lang='es'):
    # (Esta función no necesita cambios)
    df = pd.DataFrame({"Original": texts, "Reescrito": texts.copy()})
    mask = df["Original"].fillna("").astype(str).str.len() > limit
    idxs = df[mask].index.tolist()
    usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    if idxs:
        bloques = "\n".join(f'Texto {i+1}: "{df.at[i,"Original"]}"' for i in idxs)
        if lang == 'en':
            prompt_instructions = f"Your only task is to rewrite the following texts. It is CRITICAL and MANDATORY that each resulting text is under {limit} characters. Maintain the original meaning. Return only the rewritten texts, one per line."
            system_message = "You are an expert copywriter who shortens texts in English."
        elif lang == 'pt':
            prompt_instructions = f"Sua única tarefa é reescrever os textos a seguir. É CRÍTICO e OBRIGATÓRIO que cada texto resultante tenha menos de {limit} caracteres. Mantenha o significado original. Retorne apenas os textos reescritos, um por linha."
            system_message = "Você é um redator especialista que encurta textos em português."
        else:
            prompt_instructions = f"Tu única tarea es reescribir los siguientes textos. Es CRÍTICO y OBLIGATORIO que cada texto resultante tenga menos de {limit} caracteres. Mantén el significado original. Devuelve solo los textos reescritos, uno por línea."
            system_message = "Eres un redactor experto que acorta textos en español."
        prompt = f"{prompt_instructions}\n\n{bloques}".strip()
        resp = client.chat.completions.create(model=MODEL_CHAT, messages=[{"role": "system", "content": system_message}, {"role": "user", "content": prompt}], temperature=0.3)
        usage = resp.usage
        lines = [l.strip() for l in resp.choices[0].message.content.splitlines() if l.strip()]
        for i, new in zip(idxs, lines):
            df.at[i, "Reescrito"] = new if len(new) <= limit else new[:limit]
    return df["Reescrito"].tolist(), usage

def traducir_batch(texts, target):
    # (Esta función no necesita cambios)
    if not texts or all(not t for t in texts): return texts, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    if target not in ("en", "pt"): return texts, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    lang = "English (US)" if target == "en" else "Português (Brasil)"
    block = json.dumps(texts, ensure_ascii=False, indent=2)
    prompt = f"""
Eres un traductor profesional experto. Traduce la siguiente lista de textos en formato JSON al idioma {lang}.
Preserva el tono y el significado original.
REGLA IMPORTANTE: NO ABREVIES NI USES '...' EN TUS TRADUCCIONES. ENTREGA SIEMPRE LAS FRASES COMPLETAS, INCLUSO SI SON LARGAS.
Devuelve ÚNICAMENTE y SOLO un objeto JSON válido con la clave "translations", que debe contener la lista de textos traducidos. No incluyas ninguna otra palabra o explicación en tu respuesta.

{block}
""".strip()
    try:
        resp = client.chat.completions.create(model=MODEL_CHAT, response_format={"type": "json_object"}, messages=[{"role": "system", "content": "You are an expert translator designed to output JSON and never abbreviate your answers."}, {"role": "user", "content": prompt}], temperature=0.1)
        raw_json_response = resp.choices[0].message.content
        return json.loads(raw_json_response).get("translations", texts), resp.usage
    except Exception as e:
        print(f"ERROR en la traducción al '{target}': {e}. Devolviendo textos originales.")
        return texts, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}

def generar_prompt_multi(briefs, ref_df, content_info, plan_info, specs_df):
    # (Esta función no necesita cambios)
    ejemplos = ref_df.sample(n=min(10, len(ref_df)), random_state=42)
    block_ej = "\n".join(f"- [{r['Market']}][{r['Idioma']}] {r['Platform']} {r['Tipo']} {r['Campo']}: \"{r['Texto']}\"" for _, r in ejemplos.iterrows())
    template = {m: {c: {f: ([] if cnt > 1 else "") for f, (cnt, _) in fields.items()} for c, fields in CAMPAIGNS_STRUCTURE.items()} for m in content_info['markets']}
    info = [f"Contenido: {content_info['content_name']}"]
    if content_info['details']: info.append(f"Detalles: {content_info['details']}")
    info.append(f"Idiomas: {', '.join(content_info['languages'])}")
    info.append("Planes y precios disponibles (Presta atención a las ofertas):")
    for m, pls in plan_info.items():
        plan_descriptions = []
        if not pls: desc = "Sin planes definidos"
        else:
            for p in pls:
                price_info = f"{p['plan_name']} {p['currency_symbol']}{p['price']}/{p['recurring_period']}"
                if p.get('has_discount') and p.get('marketing_discount'): price_info += f" ¡EN OFERTA! ({p['marketing_discount']})"
                plan_descriptions.append(price_info)
            desc = "; ".join(plan_descriptions)
        info.append(f"- Mercado {m}: {desc}")
    specs = [f"{s['platform']} {s['campaign']}: genera {int(s['quantity'])} {s['title']} (máx {int(s['characters'])} car.); {s['style']}; {s['details']}; objetivo: {s['objective']}" for _, s in specs_df.iterrows()]
    prompt = f"""
Eres un generador experto de copies para marketing digital. Tu objetivo es crear textos persuasivos y efectivos.

Empresa: {briefs['company']}
Contexto: {briefs['company_context']}
Propuesta valor: {briefs['value_proposition']}
Campaña: {briefs['campaign_name']}
Brief: {briefs['campaign_brief']}
Extras: {briefs['extras']}

{chr(10).join(info)}

Devuelve SÓLO un JSON con la estructura exacta que se te proporciona a continuación. No agregues ninguna palabra o explicación fuera del JSON.
{json.dumps(template, ensure_ascii=False, indent=2)}

Ejemplos de copies exitosos (úsalos como inspiración de estilo y tono):
{block_ej}

Especificaciones detalladas por campaña (SÍGUELAS AL PIE DE LA LETRA):
{chr(10).join(specs)}

Reglas Fundamentales:
- Límites de Caracteres: Respeta estrictamente los límites de caracteres indicados en las especificaciones y expande si <60%.
- Estilo: Usa un lenguaje emocional, metáforas relacionadas al fútbol y la pasión, y crea un sentido de urgencia.
- Para Demand Capture: Para cualquier campaña de tipo "DemandCapture", mencionar explícitamente los descuentos, ofertas o precios especiales disponibles en la lista de planes. Usa esa información para crear urgencia y atraer al usuario a comprar.
- Siempre que se mencione el plan anual, mencionar el descuento con el que viene ese plan.
""".strip()
    return prompt

def generar_excel_multi(data, filename="copies_final.xlsx"):
    # (Esta función no necesita cambios)
    rows, all_tasks = [], []
    total_usage = {"prompt_tokens": 0, "completion_tokens": 0}
    print("--- Iniciando Fase 1: Generación y ajuste de copies en Español ---")
    for market, market_data in data.items():
        for campaign, fields in CAMPAIGNS_STRUCTURE.items():
            plat = {'SEM': 'Google', 'GoogleDemandGen': 'Google', 'GooglePMAX': 'Google', 'MetaDemandGen': 'Meta', 'MetaDemandCapture': 'Meta'}[campaign]
            tp = {'SEM': 'SEM', 'GoogleDemandGen': 'DemandGen', 'GooglePMAX': 'PMAX', 'MetaDemandGen': 'DemandGen', 'MetaDemandCapture': 'DemandCapture'}[campaign]
            campaign_data = market_data.get(campaign, {})
            for field, (count, limit) in fields.items():
                original_texts = campaign_data.get(field, [])
                if not isinstance(original_texts, list): original_texts = [original_texts]
                while len(original_texts) < count: original_texts.append("")
                es_texts, usage = preparar_batch(original_texts, limit, field, lang='es')
                if usage:
                    usage_dict = usage if isinstance(usage, dict) else usage.dict()
                    total_usage["prompt_tokens"] += usage_dict['prompt_tokens']
                    total_usage["completion_tokens"] += usage_dict['completion_tokens']
                all_tasks.append({ "market": market, "platform": plat, "tipo": tp, "campo": field, "count": count, "limit": limit, "es_texts": es_texts })
    time.sleep(1)
    print("--- Iniciando Fase 2: Traducción a Inglés y Portugués ---")
    for task in all_tasks:
        es_texts, limit = task['es_texts'], task['limit']
        en_texts_raw, en_usage1 = traducir_batch(es_texts, 'en')
        usage1_dict = en_usage1 if isinstance(en_usage1, dict) else en_usage1.dict()
        total_usage["prompt_tokens"] += usage1_dict['prompt_tokens']
        total_usage["completion_tokens"] += usage1_dict['completion_tokens']
        en_texts, en_usage2 = preparar_batch(en_texts_raw, limit, task['campo'], lang='en')
        usage2_dict = en_usage2 if isinstance(en_usage2, dict) else en_usage2.dict()
        total_usage["prompt_tokens"] += usage2_dict['prompt_tokens']
        total_usage["completion_tokens"] += usage2_dict['completion_tokens']
        time.sleep(1)
        pt_texts_raw, pt_usage1 = traducir_batch(es_texts, 'pt')
        usage3_dict = pt_usage1 if isinstance(pt_usage1, dict) else pt_usage1.dict()
        total_usage["prompt_tokens"] += usage3_dict['prompt_tokens']
        total_usage["completion_tokens"] += usage3_dict['completion_tokens']
        pt_texts, pt_usage2 = preparar_batch(pt_texts_raw, limit, task['campo'], lang='pt')
        usage4_dict = pt_usage2 if isinstance(pt_usage2, dict) else pt_usage2.dict()
        total_usage["prompt_tokens"] += usage4_dict['prompt_tokens']
        total_usage["completion_tokens"] += usage4_dict['completion_tokens']
        time.sleep(1)
        for i in range(task['count']):
            for lang, texts in [('es', es_texts), ('en', en_texts), ('pt', pt_texts)]:
                txt = texts[i] if i < len(texts) else ""
                if txt: rows.append({ "Market": task['market'], "Platform": task['platform'], "Tipo": task['tipo'], "Campo": task['campo'], "Título": f"{task['campo']} {i + 1}", "Idioma": lang, "Texto": txt, "Caracteres": len(txt), "Max Caracteres": limit, "Check": 1 if len(txt) <= limit else 0 })
    df_master = pd.DataFrame(rows)
    print("Creando archivo Excel con múltiples solapas por Plataforma y Tipo...")
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        df_master.to_excel(writer, sheet_name='Todos los Copies', index=False)
        for (platform, tipo), df_campaign in df_master.groupby(['Platform', 'Tipo']):
            sheet_name = f"{platform} {tipo}"
            print(f"Creando solapa para: {sheet_name}")
            df_sorted = df_campaign.sort_values(by=['Market', 'Idioma', 'Campo'])
            df_sorted.to_excel(writer, sheet_name=sheet_name, index=False)
    print("Aplicando formato final al archivo Excel...")
    wb = load_workbook(filename)
    if 'Todos los Copies' in wb.sheetnames:
        ws_master = wb['Todos los Copies']
        red_font = Font(color="9C0006")
        red_fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
        for row in ws_master.iter_rows(min_row=2, max_row=ws_master.max_row):
            if row[8].value == 0:
                for cell in row:
                    cell.font = red_font
                    cell.fill = red_fill
    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        for col in ws.columns:
            max_length = 0
            column_letter = col[0].column_letter
            if ws[f"{column_letter}1"].value == 'Texto':
                ws.column_dimensions[column_letter].width = 50
                continue
            for cell in col:
                try:
                    if len(str(cell.value)) > max_length: max_length = len(str(cell.value))
                except: pass
            adjusted_width = (max_length + 2)
            ws.column_dimensions[column_letter].width = adjusted_width
    wb.save(filename)
    return total_usage

# -------------------------------------------
# --- FUNCIÓN PRINCIPAL DE LA WEBAPP ---
# -------------------------------------------
def generar_copies(campaign_name: str, campaign_brief: str, output_filename: str = "copies_generadas.xlsx") -> tuple:
    """
    Función principal que orquesta todo el proceso de generación de copies.
    Devuelve una tupla: (nombre_del_archivo, resumen_del_costo_string)
    """
    print(f"Iniciando la generación de copies para la campaña: '{campaign_name}'")
    
    total_usage = {"prompt_tokens": 0, "completion_tokens": 0}
    
    briefs_config = {
        "company": os.getenv("COMPANY_NAME", "Fanatiz"),
        "company_context": os.getenv("COMPANY_CONTEXT", "Empresa de streaming de deportes..."),
        "value_proposition": os.getenv("VALUE_PROPOSITION", "Fútbol 100% legal y seguro..."),
        "campaign_name": campaign_name,
        "campaign_brief": campaign_brief,
        "extras": os.getenv("CAMPAIGN_EXTRAS", "")
    }

    base_path = os.path.abspath(os.path.dirname(__file__))
    
    print("Cargando archivos de referencia...")
    df_refs = cargar_referencias(os.path.join(base_path, "Mejor_Performing_Copies_Paid_Fanatiz.xlsx"))
    df_content = cargar_contenidos(os.path.join(base_path, "content_by_country.xlsx"))
    df_plans = cargar_planes(os.path.join(base_path, "plans_and_pricing.xlsx"))
    df_specs = cargar_specs(os.path.join(base_path, "platforms_and_campaigns_specs.xlsx"))
    
    print("Procesando información de contenido y planes...")
    content_info, plan_info = obtener_info_contenido(briefs_config["campaign_brief"], df_content, df_plans)
    
    print("Generando prompt para la IA...")
    prompt = generar_prompt_multi(briefs_config, df_refs, content_info, plan_info, df_specs)
    
    print("Llamando a la API de IA para la generación inicial de copies...")
    resp = client.chat.completions.create(
        model=MODEL_CHAT,
        messages=[{"role": "system", "content": "You are a helpful assistant."}, {"role": "user", "content": prompt}],
        temperature=0.3
    )
    total_usage["prompt_tokens"] += resp.usage.prompt_tokens
    total_usage["completion_tokens"] += resp.usage.completion_tokens
    
    data = limpiar_json(resp.choices[0].message.content)
    
    print("Generando archivo Excel final con traducciones y ajustes...")
    excel_usage = generar_excel_multi(data, filename=output_filename)
    total_usage["prompt_tokens"] += excel_usage["prompt_tokens"]
    total_usage["completion_tokens"] += excel_usage["completion_tokens"]
    
    # --- CÁLCULO FINAL Y RESUMEN DE COSTOS ---
    PRICE_PER_MILLION_INPUT = 5.0
    PRICE_PER_MILLION_OUTPUT = 15.0

    input_cost = (total_usage["prompt_tokens"] / 1_000_000) * PRICE_PER_MILLION_INPUT
    output_cost = (total_usage["completion_tokens"] / 1_000_000) * PRICE_PER_MILLION_OUTPUT
    total_cost = input_cost + output_cost

    summary = (
        f"📊 **Resumen de Consumo y Costo** 💰\n"
        f"-----------------------------------------\n"
        f"Modelo Utilizado: {MODEL_CHAT}\n"
        f"Tokens de Entrada (Prompt): {total_usage['prompt_tokens']:,}\n"
        f"Tokens de Salida (Completion): {total_usage['completion_tokens']:,}\n"
        f"**Tokens Totales:** {total_usage['prompt_tokens'] + total_usage['completion_tokens']:,}\n"
        f"-----------------------------------------\n"
        f"Costo de Entrada: ${input_cost:.6f} USD\n"
        f"Costo de Salida: ${output_cost:.6f} USD\n"
        f"**Costo Total Estimado:** **${total_cost:.6f} USD**\n"
    )
    
    print(summary)
    print(f"¡Proceso completado! Archivo guardado en: {output_filename}")
    
    return output_filename, summary
