import requests
import json
import time
import os
from sqlalchemy import create_engine, text
from datetime import datetime
import re

# Captura de variáveis de ambiente
PIPZ_KEY = os.getenv("PIPZ_TOKEN")
PIPZ_SECRET = os.getenv("PIPZ_SECRET")
DB_URL = os.getenv("DB_URL")

def format_date_to_db(date_str):
    """Trata formatos ISO (2010-04-25T...) e PT-BR (25/04/2010)"""
    if not date_str or str(date_str).lower() in ["none", "null", ""]: return None
    # Remove o T e o Z de formatos ISO
    clean = str(date_str).replace("T", " ").replace("Z", "").split(" ")[0]
    clean = clean.replace("-", "/")
    
    for fmt in ("%Y/%m/%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(clean, fmt).strftime("%Y-%m-%d")
        except: continue
    return None

def clean_cpf(cpf_str):
    if not cpf_str or str(cpf_str).lower() in ["none", "null"]: return None
    nums = re.sub(r'\D', '', str(cpf_str))
    return nums if len(nums) >= 11 else None

def extract_all_fields(contact):
    """Varre fieldsets e campos customizados de forma profunda"""
    data = {}
    # 1. Campos da Raiz
    for k, v in contact.items():
        if not isinstance(v, (dict, list)):
            data[k] = v

    # 2. Varre fieldsets (Pipz v1 pode enviar como lista ou dicionário)
    fs_data = contact.get('fieldsets')
    if fs_data:
        fs_list = fs_data.values() if isinstance(fs_data, dict) else fs_data if isinstance(fs_data, list) else []
        for fs in fs_list:
            if isinstance(fs, dict):
                for field in fs.get('fields', []):
                    label = field.get('label', '').strip()
                    name = field.get('name', '').strip()
                    val = field.get('value')
                    if label: data[label] = val
                    if name: data[name] = val
    return data

def fetch_pipz(list_id):
    """Busca contatos usando inteiros (1) para extra_fields, conforme padrão Pipz"""
    params = {
        "list_id": list_id, 
        "limit": 50, 
        "extra_fields": 1, 
        "include_fieldsets": 1,
        "api_key": PIPZ_KEY, 
        "api_secret": PIPZ_SECRET
    }
    url = "https://campuscaldeira.pipz.io/api/v1/contact/"
    try:
        res = requests.get(url, params=params, headers={"Accept": "application/json"}, timeout=30)
        if res.status_code == 200:
            return res.json().get('objects', [])
        print(f"Erro na Lista {list_id}: {res.status_code}")
    except Exception as e:
        print(f"Falha na requisição: {e}")
    return []

def process():
    if not DB_URL: return
    engine = create_engine(DB_URL)
    
    with engine.begin() as conn:
        print("--- INICIANDO CONEXÃO ---")
        
        for list_id in ["141", "144"]:
            contacts = fetch_pipz(list_id)
            print(f"Lista {list_id}: {len(contacts)} contatos.")
            
            for c in contacts:
                f = extract_all_fields(c)
                
                # --- MAPEAMENTO CPF (Múltiplas tentativas baseadas na sua lista) ---
                raw_cpf = (f.get("CPF") or f.get("[2025] CPF") or 
                           f.get("gc_2026_lp1_cpf") or f.get("gc_2026_lp2_cpf") or 
                           f.get("cpf"))
                
                cpf_limpo = clean_cpf(raw_cpf)
                final_cpf = cpf_limpo if cpf_limpo else f"ID_{c.get('id')}"
                
                # Data e Telefone
                birth = format_date_to_db(c.get('birthdate') or f.get('Birthdate') or f.get('revisar_data_de_nascimento'))
                tel = c.get('mobile_phone') or c.get('phone') or f.get('telefone')

                # UPSERT PESSOA
                p_res = conn.execute(text("""
                    INSERT INTO form_gc.pessoas (cpf, email, nome, data_nascimento, telefone)
                    VALUES (:cpf, :email, :nome, :birth, :tel)
                    ON CONFLICT (cpf) DO UPDATE SET 
                        email = EXCLUDED.email, nome = EXCLUDED.nome, 
                        telefone = EXCLUDED.telefone, data_nascimento = EXCLUDED.data_nascimento
                    RETURNING id
                """), {
                    "cpf": final_cpf, "email": c.get("email"), "nome": c.get("name"),
                    "birth": birth, "tel": str(tel)[:20] if tel else None
                })
                pessoa_id = p_res.fetchone()[0]

                # --- RESPOSTAS LP1 (141) ---
                if list_id == "141":
                    sabendo = f.get("[GC 2026 LP1] Origem") or f.get("[2025] Como ficou sabendo do Geração Caldeira?")
                    conn.execute(text("""
                        INSERT INTO form_gc.lp1_respostas (pessoa_id, edicao, estado, cidade, como_ficou_sabendo, data_resposta)
                        VALUES (:p_id, '2026', :est, :cid, :sab, NOW())
                        ON CONFLICT DO NOTHING
                    """), {
                        "p_id": pessoa_id, 
                        "est": c.get("state") or f.get("[GC 2026 LP1] Estado"),
                        "cid": c.get("city_name") or f.get("[GC2026] LP1 Cidades"),
                        "sab": sabendo
                    })

                # --- RESPOSTAS LP2 (144) ---
                if list_id == "144":
                    g_raw = str(f.get('[GC 2026 LP2] Gênero') or f.get('[GC 2026] Genero') or f.get('[2025] GÊNERO') or "").lower()
                    if any(x in g_raw for x in ["homem", "masc", "male"]): genero = "Masculino"
                    elif any(x in g_raw for x in ["mulher", "fem", "female"]): genero = "Feminino"
                    else: genero = "Outros"

                    conn.execute(text("""
                        INSERT INTO form_gc.lp2_respostas (pessoa_id, edicao, trilha, escola, genero, etnia, trabalha)
                        VALUES (:p_id, '2026', :trilha, :esc, :gen, :etn, :trab)
                        ON CONFLICT DO NOTHING
                    """), {
                        "p_id": pessoa_id, 
                        "trilha": f.get("[GC 2026 LP2] trilha educacional") or f.get("[2025] TRILHAS 2025"),
                        "esc": f.get("[GC 2026 LP2] qual escola") or f.get("Nome da escola"),
                        "gen": genero,
                        "etn": f.get("[GC 2026 LP2] qual etnia") or f.get("[GC 2026 LP2] etnia") or f.get("[2025] ETNIA"),
                        "trab": f.get("[GC 2026 LP2] você trabalha") or f.get("[2025] VOCÊ TRABALHA?")
                    })
        print("--- PROCESSO FINALIZADO ---")

if __name__ == "__main__":
    process()