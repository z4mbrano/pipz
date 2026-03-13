import requests
import pandas as pd
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
    """Converte datas (DD/MM/YYYY ou ISO) para o formato do banco (YYYY-MM-DD)"""
    if not date_str or str(date_str).lower() in ["none", "null", ""]: return None
    date_clean = str(date_str).split(" ")[0].replace("-", "/")
    for fmt in ("%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(date_clean, fmt).strftime("%Y-%m-%d")
        except: continue
    return None

def clean_cpf(cpf_str):
    """Remove pontos e traços. Retorna None se for inválido."""
    if not cpf_str or str(cpf_str).lower() in ["none", "null"]: return None
    nums = re.sub(r'\D', '', str(cpf_str))
    return nums if len(nums) >= 11 else None

def get_flexible_fields(contact):
    """Extrai campos da raiz e de dentro dos fieldsets (suporta lista ou dict)"""
    mapping = {}
    
    # 1. Pega campos da raiz (email, name, phone, etc)
    for k, v in contact.items():
        if not isinstance(v, (dict, list)):
            mapping[k] = v
            
    # 2. Pega campos dos fieldsets
    fs_data = contact.get('fieldsets', {})
    # Se for dict, pega os valores. Se for lista, usa ela mesma.
    fs_list = fs_data.values() if isinstance(fs_data, dict) else fs_data if isinstance(fs_data, list) else []
    
    for fs in fs_list:
        if isinstance(fs, dict):
            for field in fs.get('fields', []):
                name = field.get('name')
                label = field.get('label')
                val = field.get('value')
                if name: mapping[name] = val
                if label: mapping[label] = val
    return mapping

def fetch_pipz(list_id):
    """Busca 20 contatos para o teste"""
    params = {
        "list_id": list_id, "limit": "20", "extra_fields": "true",
        "api_key": PIPZ_KEY, "api_secret": PIPZ_SECRET
    }
    url = "https://campuscaldeira.pipz.io/api/v1/contact/"
    res = requests.get(url, params=params, headers={"Accept": "application/json"})
    return res.json().get('objects', []) if res.status_code == 200 else []

def process():
    if not DB_URL: return
    engine = create_engine(DB_URL)
    
    with engine.begin() as conn: # Gerencia transação automaticamente (evita deadlock)
        print("--- CONEXÃO ESTABELECIDA ---")
        
        for list_id in ["141", "144"]:
            contacts = fetch_pipz(list_id)
            print(f"Lista {list_id}: {len(contacts)} contatos encontrados.")
            
            for c in contacts:
                f = get_flexible_fields(c)
                
                # Debug no primeiro contato para conferirmos os nomes dos campos
                if contacts.index(c) == 0:
                    print(f"\n--- CAMPOS DISPONÍVEIS NA LISTA {list_id} (Top 15) ---")
                    print(list(f.keys())[:15])

                # --- MAPEAMENTO PESSOAS ---
                raw_cpf = f.get("CPF") or f.get("gc_2026_lp1_cpf") or f.get("gc_2026_lp2_cpf")
                cpf_limpo = clean_cpf(raw_cpf)
                final_cpf = cpf_limpo if cpf_limpo else f"ID_{c.get('id')}"
                
                # Data e Telefone (Nativos ou Custom)
                birth = format_date_to_db(c.get('birthdate') or f.get('Birthdate') or f.get('revisar_data_de_nascimento'))
                tel = c.get('mobile_phone') or c.get('phone') or f.get('telefone') or f.get('Mobile phone')

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
                p_id = p_res.fetchone()[0]

                # --- RESPOSTAS LP1 (141) ---
                if list_id == "141":
                    sabendo = f.get("[GC 2026 LP1] Origem") or f.get("[2025] Como ficou sabendo do Geração Caldeira?")
                    conn.execute(text("""
                        INSERT INTO form_gc.lp1_respostas (pessoa_id, edicao, estado, cidade, como_ficou_sabendo, data_resposta)
                        VALUES (:p_id, '2026', :est, :cid, :sab, NOW())
                        ON CONFLICT DO NOTHING
                    """), {
                        "p_id": p_id, 
                        "est": c.get("state") or f.get("[GC 2026 LP1] Estado"),
                        "cid": c.get("city") or f.get("[GC2026] LP1 Cidades"),
                        "sab": sabendo
                    })

                # --- RESPOSTAS LP2 (144) ---
                if list_id == "144":
                    g_raw = str(f.get('[GC 2026 LP2] Gênero') or f.get('[2025] GÊNERO') or c.get('gender') or "").lower()
                    if any(x in g_raw for x in ["homem", "masc", "male"]): genero = "Masculino"
                    elif any(x in g_raw for x in ["mulher", "fem", "female"]): genero = "Feminino"
                    else: genero = "Outros"

                    conn.execute(text("""
                        INSERT INTO form_gc.lp2_respostas (pessoa_id, edicao, trilha, escola, genero, etnia, trabalha)
                        VALUES (:p_id, '2026', :trilha, :esc, :gen, :etn, :trab)
                        ON CONFLICT DO NOTHING
                    """), {
                        "p_id": p_id, 
                        "trilha": f.get("[GC 2026 LP2] trilha educacional") or f.get("[2025] TRILHAS 2025"),
                        "esc": f.get("[GC 2026 LP2] qual escola") or f.get("Nome da escola"),
                        "gen": genero,
                        "etn": f.get("[GC 2026 LP2] etnia") or f.get("[2025] ETNIA"),
                        "trab": f.get("[GC 2026 LP2] você trabalha") or f.get("[2025] VOCÊ TRABALHA?")
                    })
        print("--- PROCESSO FINALIZADO COM SUCESSO ---")

if __name__ == "__main__":
    process()