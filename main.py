import requests
import json
import time
import os
from sqlalchemy import create_engine, text
from datetime import datetime
import re

# Configurações de ambiente
PIPZ_KEY = os.getenv("PIPZ_TOKEN")
PIPZ_SECRET = os.getenv("PIPZ_SECRET")
DB_URL = os.getenv("DB_URL")

def format_date_to_db(date_str):
    if not date_str or str(date_str).lower() in ["none", "null", ""]: return None
    clean = str(date_str).replace("T", " ").replace("Z", "").split(" ")[0].replace("-", "/")
    for fmt in ("%Y/%m/%d", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(clean, fmt).strftime("%Y-%m-%d")
        except: continue
    return None

def format_timestamp(ts_str):
    if not ts_str: return None
    try:
        return datetime.strptime(ts_str[:19], "%Y-%m-%dT%H:%M:%S")
    except: return None

def normalize_genero(val):
    """Tratamento de gênero para evitar erro de sobreposição (ex: 'h' em mulher)"""
    if not val or str(val).lower() in ["none", "null", ""]:
        return "Não informado"
    txt = str(val).lower().strip()
    # Verifica Feminino primeiro para evitar conflito com o 'h'
    if txt.startswith(('m', 'f', 'mu')) or "fem" in txt or "mulher" in txt:
        return "Feminino"
    if txt.startswith(('h', 'mas')) or "homem" in txt:
        return "Masculino"
    return "Outros"

def normalize_etnia(f_dict):
    """Busca e padroniza etnia conforme lógica do Power BI"""
    campos = ["gc_2026_lp2_etnia", "gc_2026_lp2_qual_etnia", "[GC 2026 LP2] etnia", "[GC 2026 LP2] qual etnia"]
    vals = [str(f_dict.get(c) or "") for c in campos]
    texto = " ".join(vals).lower()
    if "bran" in texto: return "Branca"
    if "pard" in texto: return "Parda"
    if "pret" in texto or "negr" in texto: return "Preta"
    if "indi" in texto: return "Indígena"
    if "amar" in texto: return "Amarela"
    return "Outra" if texto.strip() else None

def extract_fields_logic(contact_full):
    if not contact_full: return {}
    data = {}
    for k, v in contact_full.items():
        if not isinstance(v, (dict, list)): data[k] = v
    fieldsets = contact_full.get('fieldsets', [])
    for fs in fieldsets:
        for field in fs.get('fields', []):
            name, label, val = field.get('name'), field.get('label'), field.get('value')
            if name: data[name] = val
            if label: data[label] = val
    return data

def get_contact_detail(contact_id):
    url = f"https://campuscaldeira.pipz.io/api/v1/contact/{contact_id}/"
    params = {"extra_fields": "1", "api_key": PIPZ_KEY, "api_secret": PIPZ_SECRET}
    for _ in range(3):
        res = requests.get(url, params=params, headers={"Accept": "application/json"})
        if res.status_code == 200: return res.json()
        elif res.status_code == 429: time.sleep(5)
    return None

def fetch_contact_list(list_id):
    url = "https://campuscaldeira.pipz.io/api/v1/contact/"
    all_contacts = []
    offset = 0
    while offset < 250: # LIMITE DE 250
        params = {"list_id": list_id, "limit": 100, "offset": offset, "api_key": PIPZ_KEY, "api_secret": PIPZ_SECRET}
        res = requests.get(url, params=params)
        if res.status_code == 200:
            objs = res.json().get('objects', [])
            if not objs: break
            all_contacts.extend(objs)
            offset += 100
        else: break
    return all_contacts

def process():
    if not DB_URL: return
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        print("--- INICIANDO SYNC (LIMITE 250) ---")
        for list_id, handler in [("141", "lp1"), ("144", "lp2")]:
            summary_list = fetch_contact_list(list_id)
            for summary in summary_list:
                detail = get_contact_detail(summary['id'])
                f = extract_fields_logic(detail)
                if not f: continue

                raw_cpf = f.get("gc_2026_lp1_cpf") or f.get("gc_2026_lp2_cpf") or f.get("cpf") or f.get("CPF")
                nums_cpf = re.sub(r'\D', '', str(raw_cpf)) if raw_cpf else None
                final_cpf = nums_cpf if nums_cpf and len(nums_cpf) >= 11 else f"ID_{f.get('id')}"
                birth = format_date_to_db(f.get('birthdate') or f.get('birthday') or f.get('revisar_data_de_nascimento'))
                tel = f.get('mobile_phone') or f.get('phone') or f.get('telefone') or f.get('gc_2026_lp1_telefone')
                dt_cad = format_timestamp(f.get('creation_date'))

                trans = conn.begin()
                try:
                    conn.execute(text("""
                        INSERT INTO form_gc.pessoas (cpf, email, nome, data_nascimento, telefone)
                        VALUES (:cpf, :email, :nome, :birth, :tel)
                        ON CONFLICT (cpf) DO UPDATE SET 
                            email = EXCLUDED.email, nome = EXCLUDED.nome, 
                            data_nascimento = COALESCE(EXCLUDED.data_nascimento, form_gc.pessoas.data_nascimento),
                            telefone = COALESCE(EXCLUDED.telefone, form_gc.pessoas.telefone)
                    """), {"cpf": final_cpf, "email": f.get('email'), "nome": f.get('name'), "birth": birth, "tel": tel})

                    if handler == "lp1":
                        alumni = f.get("gc2026_codigo_alumni") or f.get("gc_2026_codigo_alumni") or f.get("[2025] CUPOM GC 2025")
                        sabendo = f.get("gc_2026_lp1_origem") or f.get("gc_2026_lp1_como_ficou_sabendo") or f.get("[GC 2026 LP1] Origem")
                        conn.execute(text("""
                            INSERT INTO form_gc.lp1_respostas (pessoa_id, edicao, estado, cidade, como_ficou_sabendo, codigo_indicacao, data_cadastro, data_resposta)
                            VALUES ((SELECT id FROM form_gc.pessoas WHERE cpf = :cpf), '2026', :est, :cid, :sab, :cod, :dt, NOW())
                            ON CONFLICT DO NOTHING
                        """), {"cpf": final_cpf, "est": f.get('state'), "cid": f.get('city_name'), "sab": sabendo, "cod": alumni, "dt": dt_cad})

                    if handler == "lp2":
                        genero = normalize_genero(f.get("gc_2026_lp2_genero") or f.get("gc_2026_genero") or f.get("gender") or f.get("[GC 2026 LP2] Gênero"))
                        etnia = normalize_etnia(f)
                        trabalha = "Sim" if "sim" in str(f.get("gc_2026_lp2_voce_trabalha") or "").lower() else "Não"
                        conn.execute(text("""
                            INSERT INTO form_gc.lp2_respostas (pessoa_id, edicao, trilha, escola, genero, etnia, trabalha, data_cadastro)
                            VALUES ((SELECT id FROM form_gc.pessoas WHERE cpf = :cpf), '2026', :tri, :esc, :gen, :etn, :tra, :dt)
                            ON CONFLICT DO NOTHING
                        """), {
                            "cpf": final_cpf, "tri": f.get("gc_2026_lp2_trilha_educacional"), "esc": f.get("gc_2026_lp2_qual_escola"), 
                            "gen": genero, "etn": etnia, "tra": trabalha, "dt": dt_cad
                        })
                    trans.commit()
                except Exception as e:
                    trans.rollback()
                    print(f"Erro no ID {f.get('id')}: {e}")
        print("--- PROCESSO FINALIZADO ---")

if __name__ == "__main__":
    process()