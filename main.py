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
    """Converte o creation_date do Pipz para timestamp do Postgres"""
    if not ts_str: return None
    try:
        # Exemplo Pipz: 2026-03-13T17:56:55+0000
        return datetime.strptime(ts_str[:19], "%Y-%m-%dT%H:%M:%S")
    except:
        return None

def extract_fields_logic(contact):
    """Extrai campos seguindo a lógica do Power BI (Nomes técnicos exatos)"""
    data = {}
    # Campos da Raiz
    data['id'] = contact.get('id')
    data['email'] = contact.get('email')
    data['name'] = contact.get('name')
    data['gender_root'] = contact.get('gender')
    data['creation_date'] = contact.get('creation_date')
    data['state_root'] = contact.get('state')
    data['city_root'] = contact.get('city_name')

    # Varre Fieldsets
    fs_data = contact.get('fieldsets', {})
    fs_list = fs_data.values() if isinstance(fs_data, dict) else fs_data if isinstance(fs_data, list) else []
    for fs in fs_list:
        if isinstance(fs, dict):
            for field in fs.get('fields', []):
                name = field.get('name', '')
                if name: data[name] = field.get('value')
    return data

def fetch_pipz(list_id):
    """Busca contatos paginados (100 por vez para evitar erro 429)"""
    url = "https://campuscaldeira.pipz.io/api/v1/contact/"
    all_objs = []
    offset = 0
    while offset < 200: # Limite de teste aumentado para 200
        params = {
            "list_id": list_id, "limit": 100, "offset": offset,
            "extra_fields": 1, "include_fieldsets": 1,
            "api_key": PIPZ_KEY, "api_secret": PIPZ_SECRET
        }
        res = requests.get(url, params=params, headers={"Accept": "application/json"})
        if res.status_code == 200:
            objs = res.json().get('objects', [])
            if not objs: break
            all_objs.extend(objs)
            offset += 100
        elif res.status_code == 429:
            time.sleep(10)
            continue
        else: break
    return all_objs

def process():
    if not DB_URL: return
    engine = create_engine(DB_URL)
    
    with engine.connect() as conn:
        print("--- INICIANDO CONEXÃO E SINCRONIZAÇÃO ---")
        
        for list_id, handler in [("141", "lp1"), ("144", "lp2")]:
            contacts = fetch_pipz(list_id)
            print(f"Processando Lista {list_id}: {len(contacts)} contatos.")
            
            for c in contacts:
                f = extract_fields_logic(c)
                
                # --- DADOS PESSOA ---
                raw_cpf = f.get("gc_2026_lp1_cpf") or f.get("gc_2026_lp2_cpf") or f.get("cpf") or f.get("CPF")
                nums_cpf = re.sub(r'\D', '', str(raw_cpf)) if raw_cpf else None
                final_cpf = nums_cpf if nums_cpf and len(nums_cpf) >= 11 else f"ID_{f['id']}"
                
                birth = format_date_to_db(f.get('birthdate') or f.get('Birthdate') or f.get('revisar_data_de_nascimento'))
                tel = f.get('mobile_phone') or f.get('phone') or f.get('telefone')
                data_inscricao = format_timestamp(f['creation_date'])

                trans = conn.begin()
                try:
                    # UPSERT Pessoa
                    conn.execute(text("""
                        INSERT INTO form_gc.pessoas (cpf, email, nome, data_nascimento, telefone)
                        VALUES (:cpf, :email, :nome, :birth, :tel)
                        ON CONFLICT (cpf) DO UPDATE SET 
                            email = COALESCE(EXCLUDED.email, form_gc.pessoas.email),
                            nome = COALESCE(EXCLUDED.nome, form_gc.pessoas.nome)
                    """), {"cpf": final_cpf, "email": f['email'], "nome": f['name'], "birth": birth, "tel": tel})

                    # --- LÓGICA LP1 ---
                    if handler == "lp1":
                        alumni = f.get("gc2026_codigo_alumni") or f.get("gc_2026_codigo_alumni")
                        sabendo = f.get("gc_2026_lp1_origem") or f.get("gc_2026_lp1_como_ficou_sabendo")
                        conn.execute(text("""
                            INSERT INTO form_gc.lp1_respostas (pessoa_id, edicao, estado, cidade, como_ficou_sabendo, codigo_indicacao, data_cadastro, data_resposta)
                            VALUES ((SELECT id FROM form_gc.pessoas WHERE cpf = :cpf), '2026', :est, :cid, :sab, :alu, :dt, NOW())
                            ON CONFLICT DO NOTHING
                        """), {"cpf": final_cpf, "est": f['state_root'], "cid": f['city_root'], "sab": sabendo, "alu": alumni, "dt": data_inscricao})

                    # --- LÓGICA LP2 ---
                    if handler == "lp2":
                        # Gênero (Igual Power BI)
                        val_gen = f.get("gc_2026_lp2_genero") or f.get("gc_2026_genero") or f.get("gender_root")
                        gen_txt = str(val_gen or "").lower()
                        genero = "Masculino" if "h" in gen_txt or "mas" in gen_txt else "Feminino" if "mu" in gen_txt or "f" in gen_txt else "Outros"
                        
                        # Trabalho (Igual Power BI)
                        trab_lp2 = str(f.get("gc_2026_lp2_voce_trabalha") or "").lower()
                        trab_emp = str(f.get("_gc_2026_lp2_voc_trabalha_em_alguma_empresa") or "").lower()
                        trabalha = "Sim" if "sim" in trab_lp2 else "Não" if "n" in trab_lp2 else ("Não" if "n" in trab_emp or trab_emp == "" else "Sim")

                        # Etnia (Igual Power BI)
                        etnia_raw = str(f.get("gc_2026_lp2_etnia") or "").lower() + " " + str(f.get("gc_2026_lp2_qual_etnia") or "").lower()
                        etnia = "Branca" if "bran" in etnia_raw else "Parda" if "pard" in etnia_raw else "Preta" if "pret" in etnia_raw or "negr" in etnia_raw else "Outra"

                        conn.execute(text("""
                            INSERT INTO form_gc.lp2_respostas (pessoa_id, edicao, trilha, escola, genero, etnia, trabalha, data_cadastro)
                            VALUES ((SELECT id FROM form_gc.pessoas WHERE cpf = :cpf), '2026', :tri, :esc, :gen, :etn, :tra, :dt)
                            ON CONFLICT DO NOTHING
                        """), {
                            "cpf": final_cpf, "tri": f.get("gc_2026_lp2_trilha_educacional"),
                            "esc": f.get("gc_2026_lp2_qual_escola"), "gen": genero, 
                            "etn": etnia, "tra": trabalha, "dt": data_inscricao
                        })
                    trans.commit()
                except Exception as e:
                    trans.rollback()
                    print(f"Erro no contato {f['id']}: {e}")

if __name__ == "__main__":
    process()