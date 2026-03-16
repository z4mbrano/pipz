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
    """Tratamento rigoroso de gênero para evitar erro de 'h' em mulher"""
    if not val or str(val).lower() in ["none", "null", ""]:
        return "Não informado"
    txt = str(val).lower().strip()
    # Prioriza feminino para evitar conflito com 'h'
    if txt.startswith(('f', 'mu')) or "mulher" in txt or "fem" in txt:
        return "Feminino"
    if txt.startswith(('h', 'mas')) or "homem" in txt:
        return "Masculino"
    return "Outros"

def normalize_etnia(f):
    """Busca etnia em múltiplos campos e padroniza"""
    raw = (f.get("gc_2026_lp2_etnia") or f.get("gc_2026_lp2_qual_etnia") or 
           f.get("[GC 2026 LP2] etnia") or f.get("[GC 2026 LP2] qual etnia") or 
           f.get("[2025] ETNIA") or f.get("etnia") or "")
    txt = str(raw).lower()
    if "bran" in txt: return "Branca"
    if "pard" in txt: return "Parda"
    if "pret" in txt or "negr" in txt: return "Preta"
    if "indi" in txt: return "Indígena"
    if "amar" in txt: return "Amarela"
    return "Outra" if txt.strip() else None

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
    while offset < 250: # LIMITE SOLICITADO
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
        print("--- INICIANDO SYNC PROFUNDO (250 CONTATOS) ---")
        for list_id, handler in [("141", "lp1"), ("144", "lp2")]:
            summary_list = fetch_contact_list(list_id)
            print(f"Processando Lista {list_id}...")
            for summary in summary_list:
                detail = get_contact_detail(summary['id'])
                f = extract_fields_logic(detail)
                if not f: continue

                # PESSOA (Mantido o que funciona)
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
                            email = COALESCE(EXCLUDED.email, form_gc.pessoas.email),
                            nome = COALESCE(EXCLUDED.nome, form_gc.pessoas.nome),
                            data_nascimento = COALESCE(EXCLUDED.data_nascimento, form_gc.pessoas.data_nascimento),
                            telefone = COALESCE(EXCLUDED.telefone, form_gc.pessoas.telefone)
                    """), {"cpf": final_cpf, "email": f.get('email'), "nome": f.get('name'), "birth": birth, "tel": tel})

                    if handler == "lp1":
                        alumni = f.get("gc2026_codigo_alumni") or f.get("gc_2026_codigo_alumni") or f.get("[GC2026] codigo alumni") or f.get("[2025] CUPOM GC 2025")
                        sabendo = f.get("gc_2026_lp1_origem") or f.get("[GC 2026 LP1] Origem") or f.get("[2025] Como ficou sabendo do Geração Caldeira?") or f.get("gc_2026_lp1_como_ficou_sabendo")
                        
                        conn.execute(text("""
                            INSERT INTO form_gc.lp1_respostas (pessoa_id, edicao, estado, cidade, como_ficou_sabendo, codigo_indicacao, data_cadastro, data_resposta)
                            VALUES ((SELECT id FROM form_gc.pessoas WHERE cpf = :cpf), '2026', :est, :cid, :sab, :cod, :dt, NOW())
                            ON CONFLICT (pessoa_id, edicao) DO UPDATE SET 
                                como_ficou_sabendo = COALESCE(EXCLUDED.como_ficou_sabendo, form_gc.lp1_respostas.como_ficou_sabendo),
                                codigo_indicacao = COALESCE(EXCLUDED.codigo_indicacao, form_gc.lp1_respostas.codigo_indicacao)
                        """), {"cpf": final_cpf, "est": f.get('state'), "cid": f.get('city_name'), "sab": sabendo, "cod": alumni, "dt": dt_cad})

                    if handler == "lp2":
                        genero = normalize_genero(f.get("gc_2026_lp2_genero") or f.get("gc_2026_genero") or f.get("[GC 2026 LP2] Gênero") or f.get("[GC 2026] Genero") or f.get("gender"))
                        etnia = normalize_etnia(f)
                        trabalha = "Sim" if "sim" in str(f.get("gc_2026_lp2_voce_trabalha") or f.get("[GC 2026 LP2] você trabalha") or "").lower() else "Não"
                        
                        conn.execute(text("""
                            INSERT INTO form_gc.lp2_respostas (pessoa_id, edicao, trilha, escola, genero, etnia, trabalha, data_cadastro)
                            VALUES ((SELECT id FROM form_gc.pessoas WHERE cpf = :cpf), '2026', :tri, :esc, :gen, :etn, :tra, :dt)
                            ON CONFLICT (pessoa_id, edicao) DO UPDATE SET 
                                trilha = COALESCE(EXCLUDED.trilha, form_gc.lp2_respostas.trilha),
                                genero = EXCLUDED.genero,
                                etnia = COALESCE(EXCLUDED.etnia, form_gc.lp2_respostas.etnia),
                                trabalha = EXCLUDED.trabalha
                        """), {
                            "cpf": final_cpf, "tri": f.get("gc_2026_lp2_trilha_educacional") or f.get("[GC 2026 LP2] trilha educacional"),
                            "esc": f.get("gc_2026_lp2_qual_escola") or f.get("[GC 2026 LP2] qual escola") or f.get("Nome da escola"),
                            "gen": genero, "etn": etnia, "tra": trabalha, "dt": dt_cad
                        })
                    trans.commit()
                except Exception as e:
                    trans.rollback()
                    print(f"Erro ID {f.get('id')}: {e}")
        print("--- SYNC FINALIZADO ---")

if __name__ == "__main__":
    process()