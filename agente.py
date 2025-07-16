import os
import re
import json
import pandas as pd
import time
from openai import OpenAI
from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill

# --- CONFIGURACIÓN GLOBAL ---
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
MODEL_CHAT = "gpt-4o"

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

def obtener_info_contenido(campaign_name, brief, content_df, plans_df):
    search_text = (campaign_name + " " + brief).lower()
    final_rows_df = pd.DataFrame()

    print("Iniciando búsqueda de contenido... (Paso 1: Buscando por nombre de plan)")
    def check_plan_in_text(plans_str, text_to_search):
        if not isinstance(plans_str, str): return False
        full_plans = [p.strip().lower() for p in plans_str.split(',') if p.strip()]
        base_plans = {p.replace('monthly', '').replace('annual', '').strip() for p in full_plans}
        return any(base_plan in text_to_search for base_plan in base_plans if base_plan)

    mask_plan = content_df['plans_available'].apply(check_plan_in_text, text_to_search=search_text)
    plan_matched_rows = content_df[mask_plan]

    if not plan_matched_rows.empty:
        print(f"Coincidencia por plan encontrada en {len(plan_matched_rows)} fila(s).")
        print("  -> Afinando búsqueda por nombre de contenido...")
        mask_content_refine = plan_matched_rows['content_name'].str.lower().apply(lambda name: name in search_text if pd.notna(name) else False)
        secondary_matched_rows = plan_matched_rows[mask_content_refine]
        if not secondary_matched_rows.empty:
            print(f"  -> Búsqueda afinada exitosa. Usando {len(secondary_matched_rows)} fila(s) específicas.")
            final_rows_df = secondary_matched_rows
        else:
            print("  -> No se pudo afinar. Usando todas las filas coincidentes con el plan.")
            final_rows_df = plan_matched_rows
    else:
        print("Búsqueda por plan no exitosa. (Paso 2: Buscando por nombre de contenido como fallback)")
        mask_content_fallback = content_df['content_name'].str.lower().apply(lambda name: name in search_text if pd.notna(name) else False)
        content_matched_rows = content_df[mask_content_fallback]
        if not content_matched_rows.empty:
            print(f"✅ Coincidencia encontrada por contenido en {len(content_matched_rows)} fila(s).")
            final_rows_df = content_matched_rows
        else:
            print("Búsqueda por contenido también falló.")

    if final_rows_df.empty:
        print("ADVERTENCIA: No se encontró contenido para esta campaña.")
    
    return final_rows_df

def limpiar_json(texto):
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
        print(f"ERROR: Falló la decodificación del JSON. Error: {e}")
        print(f"Respuesta problemática que se intentó decodificar: {json_str}")
        return {}

def preparar_batch(texts, limit, tipo, lang='es'):
    df = pd.DataFrame({"Original": texts, "Reescrito": texts.copy()})
    mask = df["Original"].fillna("").astype(str).str.len() > limit
    idxs = df[mask].index.tolist()
    usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}

    if idxs:
        bloques = "\n".join(f'Texto {i+1}: "{df.at[i,"Original"]}"' for i in idxs)
        
        if lang == 'en':
            prompt_instructions = f"Your only task is to rewrite the following texts. It is CRITICAL that each resulting text is under {limit} characters. Maintain the original meaning. Return only the rewritten texts, one per line, WITHOUT prefixes like 'Texto 1:' or quotes."
            system_message = "You are an expert copywriter who shortens texts in English."
        elif lang == 'pt':
            prompt_instructions = f"Sua única tarefa é reescrever os textos a seguir. É CRÍTICO que cada texto resultante tenha menos de {limit} caracteres. Mantenha o significado original. Retorne apenas os textos reescritos, um por linha, SEM prefixos como 'Texto 1:' ou aspas."
            system_message = "Você é um redator especialista que encurta textos em português."
        else: # Español por defecto
            prompt_instructions = f"Tu única tarea es reescribir los siguientes textos. Es CRÍTICO y OBLIGATORIO que cada texto resultante tenga menos de {limit} caracteres. Mantén el significado original. Devuelve solo los textos reescritos, uno por línea, SIN prefijos como 'Texto 1:' ni comillas."
            system_message = "Eres un redactor experto que acorta textos en español."

        prompt = f"{prompt_instructions}\n\n{bloques}".strip()
        
        resp = client.chat.completions.create(
            model=MODEL_CHAT,
            messages=[{"role": "system", "content": system_message},
                      {"role": "user", "content": prompt}],
            temperature=0.3
        )
        usage = resp.usage
        lines = [l.strip() for l in resp.choices[0].message.content.splitlines() if l.strip()]

        # --- INICIO DE LA LIMPIEZA DE SEGURIDAD ---
        cleaned_lines = []
        for line in lines:
            # Elimina cualquier prefijo como "Texto 1: " y las comillas de los extremos.
            cleaned_line = re.sub(r'^\s*Texto\s*\d+:\s*"?', '', line)
            if cleaned_line.endswith('"'):
                cleaned_line = cleaned_line[:-1]
            cleaned_lines.append(cleaned_line.strip())
        
        lines = cleaned_lines # Usamos las líneas ya limpias
        # --- FIN DE LA LIMPIEZA DE SEGURIDAD ---

        for i, new in zip(idxs, lines):
            df.at[i, "Reescrito"] = new if len(new) <= limit else new[:limit]
    
    return df["Reescrito"].tolist(), usage

def traducir_batch(texts, target):
    if not texts or all(not t for t in texts): return texts, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    if target not in ("en", "pt"): return texts, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    lang = "English (US)" if target == "en" else "Português (Brasil)"
    block = json.dumps(texts, ensure_ascii=False, indent=2)
    prompt = f"""
Eres un traductor profesional experto. Traduce la siguiente lista de textos en formato JSON al idioma {lang}.
Preserva el tono y el significado original.
REGLA IMPORTANTE: NO ABREVIES NI USES '...' EN TUS TRADUCCIONES. ENTREGA SIEMPRE LAS FRASES COMPLETAS.
Devuelve ÚNICAMENTE y SOLO un objeto JSON válido con la clave "translations".
{block}
""".strip()
    try:
        resp = client.chat.completions.create(model=MODEL_CHAT, response_format={"type": "json_object"}, messages=[{"role": "system", "content": "You are an expert translator designed to output JSON..."}, {"role": "user", "content": prompt}], temperature=0.1)
        return json.loads(resp.choices[0].message.content).get("translations", texts), resp.usage
    except Exception as e:
        print(f"ERROR en la traducción al '{target}': {e}.")
        return texts, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}

def generar_prompt_multi(briefs, ref_df, content_info, plan_info, specs_df):
    ejemplos = ref_df.sample(n=min(10, len(ref_df)), random_state=42)
    block_ej = "\n".join(f"- [{r['Market']}][{r['Idioma']}] {r['Platform']} {r['Tipo']} {r['Campo']}: \"{r['Texto']}\"" for _, r in ejemplos.iterrows())
    template = {m: {c: {f: ([] if cnt > 1 else "") for f, (cnt, _) in fields.items()} for c, fields in CAMPAIGNS_STRUCTURE.items()} for m in content_info['markets']}
    info = [f"Contenido: {content_info['content_name']}"]
    if content_info['details']: info.append(f"Detalles: {content_info['details']}")
    info.append(f"Idiomas: {', '.join(content_info['languages'])}")
    info.append("Planes y precios disponibles (USA EL SÍMBOLO DE MONEDA EXACTO QUE SE MUESTRA):")
    for m, pls in plan_info.items():
        plan_descriptions = []
        if not pls: desc = "Sin planes definidos"
        else:
            for p in pls:
                price_info = f"{p['plan_name']} {p['currency_symbol']}{p['price']}/{p['recurring_period']}"
                has_discount = str(p.get('has_discount', 'no')).lower() == 'yes'
                marketing_discount = p.get('marketing_discount')
                if has_discount and marketing_discount: price_info += f" ¡EN OFERTA! ({marketing_discount}%)"
                plan_descriptions.append(price_info)
            desc = "; ".join(plan_descriptions)
        info.append(f"- Mercado {m}: {desc}")
    specs = [f"{s['platform']} {s['campaign']}: genera {int(s['quantity'])} {s['title']} (máx {int(s['characters'])} car.); {s['style']}; {s['details']}; objetivo: {s['objective']}" for _, s in specs_df.iterrows()]
    prompt = f"""
Eres un generador experto de copies...

Empresa: {briefs['company']}
Campaña: {briefs['campaign_name']}
Brief: {briefs['campaign_brief']}

{chr(10).join(info)}

Devuelve SÓLO un JSON con esta estructura:
{json.dumps(template, ensure_ascii=False, indent=2)}

Ejemplos de copies exitosos:
{block_ej}

Especificaciones detalladas por campaña:
{chr(10).join(specs)}

Reglas Fundamentales:
- Límites de Caracteres: Respeta estrictamente los límites de caracteres indicados en las especificaciones y expande si <60%.
- Estilo: Usa un lenguaje emocional, metáforas relacionadas al fútbol y la pasión, y crea un sentido de urgencia.
- Para Demand Capture: Para cualquier campaña de tipo "DemandCapture", mencionar explícitamente los descuentos, ofertas o precios especiales disponibles en la lista de planes. Usa esa información para crear urgencia y atraer al usuario a comprar.
- Siempre que se mencione el plan anual, mencionar el descuento con el que viene ese plan.
- Símbolo de Moneda: Es CRÍTICO y OBLIGATORIO que uses el símbolo de moneda EXACTO que se especifica para cada plan en la sección 'Planes y precios'. No asumas ni uses un símbolo diferente.
""".strip()
    return prompt

def generar_excel_multi(data, filename="copies_final.xlsx"):
    rows, all_tasks = [], []
    total_usage = {"prompt_tokens": 0, "completion_tokens": 0}
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
    for task in all_tasks:
        es_texts, limit = task['es_texts'], task['limit']
        en_texts_raw, en_usage1 = traducir_batch(es_texts, 'en')
        usage1_dict = en_usage1 if isinstance(en_usage1, dict) else en_usage1.dict()
        total_usage["prompt_tokens"] += usage1_dict['prompt_tokens']; total_usage["completion_tokens"] += usage1_dict['completion_tokens']
        en_texts, en_usage2 = preparar_batch(en_texts_raw, limit, task['campo'], lang='en')
        usage2_dict = en_usage2 if isinstance(en_usage2, dict) else en_usage2.dict()
        total_usage["prompt_tokens"] += usage2_dict['prompt_tokens']; total_usage["completion_tokens"] += usage2_dict['completion_tokens']
        pt_texts_raw, pt_usage1 = traducir_batch(es_texts, 'pt')
        usage3_dict = pt_usage1 if isinstance(pt_usage1, dict) else pt_usage1.dict()
        total_usage["prompt_tokens"] += usage3_dict['prompt_tokens']; total_usage["completion_tokens"] += usage3_dict['completion_tokens']
        pt_texts, pt_usage2 = preparar_batch(pt_texts_raw, limit, task['campo'], lang='pt')
        usage4_dict = pt_usage2 if isinstance(pt_usage2, dict) else pt_usage2.dict()
        total_usage["prompt_tokens"] += usage4_dict['prompt_tokens']; total_usage["completion_tokens"] += usage4_dict['completion_tokens']
        for i in range(task['count']):
            for lang, texts in [('es', es_texts), ('en', en_texts), ('pt', pt_texts)]:
                txt = texts[i] if i < len(texts) else ""
                if txt: rows.append({ "Market": task['market'], "Platform": task['platform'], "Tipo": task['tipo'], "Campo": task['campo'], "Título": f"{task['campo']} {i + 1}", "Idioma": lang, "Texto": txt, "Caracteres": len(txt), "Max Caracteres": limit, "Check": 1 if len(txt) <= limit else 0 })
    df_master = pd.DataFrame(rows)
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        df_master.to_excel(writer, sheet_name='Todos los Copies', index=False)
        for (platform, tipo), df_campaign in df_master.groupby(['Platform', 'Tipo']):
            sheet_name = f"{platform} {tipo}"
            df_sorted = df_campaign.sort_values(by=['Market', 'Idioma', 'Campo'])
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
    
    base_path = os.path.abspath(os.path.dirname(__file__))
    print("Cargando archivos de referencia...")
    df_refs = cargar_referencias(os.path.join(base_path, "Mejor_Performing_Copies_Paid_Fanatiz.xlsx"))
    df_content = cargar_contenidos(os.path.join(base_path, "content_by_country.xlsx"))
    df_plans = cargar_planes(os.path.join(base_path, "plans_and_pricing.xlsx"))
    df_specs = cargar_specs(os.path.join(base_path, "platforms_and_campaigns_specs.xlsx"))

    # 1. Intentamos obtener el contenido específico de la campaña
    relevant_content_df = obtener_info_contenido(campaign_name, campaign_brief, df_content, df_plans)
    
    # 2. Si no se encuentra, se usa la configuración por defecto
    if relevant_content_df.empty:
        print("\n⚠️ ADVERTENCIA: No se encontró contenido específico. Usando configuración por defecto (Plan Front Row).")
        default_data = {
            'content_name': [campaign_name],
            'markets_available': ['US/CA,EUROPE,ROW'],
            'plans_available': ['Front Row Monthly,Front Row Annual'],
            'content_languages': ['ES,EN,PT'],
            'content_details': ['']
        }
        relevant_content_df = pd.DataFrame(default_data)

    # 3. Extraemos la lista de mercados únicos y procesamos cada uno
    all_markets = set()
    relevant_content_df['markets_available'].dropna().str.split(',').apply(
        lambda lst: all_markets.update(item.strip() for item in lst if item.strip())
    )
    markets_to_process = sorted(list(all_markets))
    print(f"\nMercados a procesar para esta campaña: {markets_to_process}")

    final_data_for_excel = {}

    for market in markets_to_process:
        print("\n" + "="*20 + f" PROCESANDO MERCADO: {market} " + "="*20)
        
        market_content_df = relevant_content_df[relevant_content_df['markets_available'].str.contains(market, na=False)]
        
        content_names = ", ".join(market_content_df['content_name'].unique())
        languages = set(); market_content_df['content_languages'].dropna().str.split(',').apply(lambda lst: languages.update(l.strip() for l in lst if l.strip()))
        plans_available = set(); market_content_df['plans_available'].dropna().str.split(',').apply(lambda lst: plans_available.update(p.strip() for p in lst if p.strip()))

        content_info = { "content_name": content_names, "languages": sorted(list(languages)), "details": "", "markets": [market] }
        
        plan_info = {}
        matches = []
        
        def is_market_in_cell(available_markets, target_market):
            if not isinstance(available_markets, str): return False
            market_list = [m.strip().lower() for m in available_markets.split(',')]
            return target_market.lower() in market_list

        for plan_nom in sorted(list(plans_available)):
            m = re.match(r"(.+?)\s+(monthly|annual)$", plan_nom, flags=re.IGNORECASE)
            name, period = (m.group(1), m.group(2).capitalize()) if m else (plan_nom, None)

            mask = ((df_plans["plan_name"].str.lower() == name.lower()) & (df_plans["markets"].apply(is_market_in_cell, target_market=market)))
            if period: mask &= (df_plans["recurring_period"] == period)
            sel = df_plans[mask]
            
            if not sel.empty: matches.append(sel.iloc[0].to_dict())

        plan_info[market] = matches

        briefs = { 'campaign_name': campaign_name, 'campaign_brief': campaign_brief, 'company': 'Fanatiz', 'company_context': '...', 'value_proposition': '...', 'extras': '' }
        
        prompt = generar_prompt_multi(briefs, df_refs, content_info, plan_info, df_specs)
        
        resp = client.chat.completions.create( model=MODEL_CHAT, messages=[{'role':'system','content':'You are a helpful assistant.'}, {'role':'user','content':prompt}], temperature=0.3 )
        
        total_usage["prompt_tokens"] += resp.usage.prompt_tokens
        total_usage["completion_tokens"] += resp.usage.completion_tokens
        
        raw_response = resp.choices[0].message.content
        market_data = limpiar_json(raw_response)
        
        if market in market_data:
            final_data_for_excel[market] = market_data[market]
        else:
             print(f"ADVERTENCIA: La respuesta de la IA para {market} no contenía la clave de mercado esperada.")

    if not final_data_for_excel:
        return output_filename, "Error: La IA no generó copies válidos después de procesar los mercados."

    excel_usage = generar_excel_multi(final_data_for_excel, filename=output_filename)
    total_usage["prompt_tokens"] += excel_usage["prompt_tokens"]
    total_usage["completion_tokens"] += excel_usage["completion_tokens"]
    
    PRICE_PER_MILLION_INPUT = 5.0
    PRICE_PER_MILLION_OUTPUT = 15.0
    input_cost = (total_usage["prompt_tokens"] / 1_000_000) * PRICE_PER_MILLION_INPUT
    output_cost = (total_usage["completion_tokens"] / 1_000_000) * PRICE_PER_MILLION_OUTPUT
    total_cost = input_cost + output_cost
    summary = (f"📊 **Resumen de Consumo y Costo** 💰\n" f"-----------------------------------------\n" f"Modelo Utilizado: {MODEL_CHAT}\n" f"Tokens de Entrada (Prompt): {total_usage['prompt_tokens']:,}\n" f"Tokens de Salida (Completion): {total_usage['completion_tokens']:,}\n" f"**Tokens Totales:** {total_usage['prompt_tokens'] + total_usage['completion_tokens']:,}\n" f"-----------------------------------------\n" f"Costo de Entrada: ${input_cost:.6f} USD\n" f"Costo de Salida: ${output_cost:.6f} USD\n" f"**Costo Total Estimado:** **${total_cost:.6f} USD**\n")
    
    print(summary)
    print(f"¡Proceso completado! Archivo guardado en: {output_filename}")
    
    return output_filename, summary
