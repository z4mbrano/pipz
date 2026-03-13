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
    for fmt in ("%Y/%m/%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(clean, fmt).strftime("%Y-%m-%d")
        except: continue
    return None

def clean_cpf(cpf_str):
    if not cpf_str or str(cpf_str).lower() in ["none", "null"]: return None
    nums = re.sub(r'\D', '', str(cpf_str))
    return nums if len(nums) >= 11 else None

def normalize_key(key):
    """Remove acentos, espaços extras e põe em minúsculo para busca segura"""
    if not key: return ""
    key = key.lower().strip()
    key = re.sub(r'[àáâãäå]', 'a', key)
    key = re.sub(r'[èéêë]', 'e', key)
    key = re.sub(r'[ìíîï]', 'i', key)
    key = re.sub(r'[òóôõö]', 'o', key)
    key = re.sub(r'[ùúûü]', 'u', key)
    key = re.sub(r'[ç]', 'c', key)
    return key

def extract_all_fields(contact):
    """Mapeia campos por nome técnico E por label normalizado"""
    data = {}
    # Campos base
    for k, v in contact.items():
        if not isinstance(v, (dict, list)): data[k] = v

    # Fieldsets
    fs_data = contact.get('fieldsets', {})
    fs_list = fs_data.values() if isinstance(fs_data, dict) else fs_data if isinstance(fs_data, list) else []
    
    for fs in fs_list:
        if isinstance(fs, dict):
            for field in fs.get('fields', []):
                label = field.get('label', '')
                name = field.get('name', '')
                val = field.get('value')
                if label: data[normalize_key(label)] = val
                if name: data[name] = val
    return data

def fetch_pipz_page(list_id, offset=0):
    url = "https://campuscaldeira.pipz.io/api/v1/contact/"
    params = {
        "list_id": list_id, "limit": 100, "offset": offset,
        "extra_fields": 1, "include_fieldsets": 1,
        "api_key": PIPZ_KEY, "api_secret": PIPZ_SECRET
    }
    for attempt in range(3):
        try:
            res = requests.get(url, params=params, timeout=30)
            if res.status_code == 200:
                return res.json().get('objects', [])
            if res.status_code == 429:
                time.sleep(20 * (attempt + 1))
        except: time.sleep(5)
    return []

def process():
    if not DB_URL: return
    engine = create_engine(DB_URL)
    
    with engine.connect() as conn:
        print("--- CONEXÃO ESTABELECIDA ---")
        
        for list_id, handler in [("141", "lp1"), ("144", "lp2")]:
            offset = 0
            while True:
                contacts = fetch_pipz_page(list_id, offset)
                if not contacts: break
                
                print(f"Lista {list_id}: Processando batch de {len(contacts)} contatos (Offset: {offset})")
                
                for c in contacts:
                    f = extract_all_fields(c)
                    
                    # 1. MAPEAMENTO PESSOAS (Fallback agressivo)
                    raw_cpf = f.get(normalize_key("CPF")) or f.get(normalize_key("[2025] CPF")) or f.get("gc_2026_lp1_cpf")
                    final_cpf = clean_cpf(raw_cpf) or f"ID_{c.get('id')}"
                    
                    birth = format_date_to_db(c.get('birthdate') or f.get(normalize_key('revisar data de nascimento')))
                    tel = c.get('mobile_phone') or c.get('phone') or f.get(normalize_key('telefone'))

                    trans = conn.begin()
                    try:
                        p_res = conn.execute(text("""
                            INSERT INTO form_gc.pessoas (cpf, email, nome, data_nascimento, telefone)
                            VALUES (:cpf, :email, :nome, :birth, :tel)
                            ON CONFLICT (cpf) DO UPDATE SET 
                                email = COALESCE(EXCLUDED.email, form_gc.pessoas.email),
                                nome = COALESCE(EXCLUDED.nome, form_gc.pessoas.nome),
                                data_nascimento = COALESCE(EXCLUDED.data_nascimento, form_gc.pessoas.data_nascimento),
                                telefone = COALESCE(EXCLUDED.telefone, form_gc.pessoas.telefone)
                            RETURNING id
                        """), {"cpf": final_cpf, "email": c.get("email"), "nome": c.get("name"), "birth": birth, "tel": str(tel)[:20] if tel else None})
                        pessoa_id = p_res.fetchone()[0]

                        # 2. LP1 (141)
                        if handler == "lp1":
                            conn.execute(text("""
                                INSERT INTO form_gc.lp1_respostas (pessoa_id, edicao, estado, cidade, como_ficou_sabendo, data_resposta)
                                VALUES (:p_id, '2026', :est, :cid, :sab, NOW()) ON CONFLICT DO NOTHING
                            """), {
                                "p_id": pessoa_id, 
                                "est": c.get("state") or f.get(normalize_key("[GC 2026 LP1] Estado")),
                                "cid": c.get("city_name") or f.get(normalize_key("[GC2026] LP1 Cidades")),
                                "sab": f.get(normalize_key("[GC 2026 LP1] Origem")) or f.get(normalize_key("[2025] Como ficou sabendo do Geração Caldeira?"))
                            })

                        # 3. LP2 (144)
                        if handler == "lp2":
                            # Lógica Gênero (Fallback sem acento)
                            g_raw = str(f.get(normalize_key('[GC 2026 LP2] Genero')) or f.get(normalize_key('[GC 2026] Genero')) or c.get('gender') or "").lower()
                            genero = "Masculino" if "homem" in g_raw or "masc" in g_raw else "Feminino" if "mulher" in g_raw or "fem" in g_raw else "Outros"
                            
                            # Lógica Trabalho (Inferred)
                            regime = str(f.get(normalize_key("[GC 2026 LP2] regime trabalho")) or "").lower()
                            trab_raw = f.get(normalize_key("[GC 2026 LP2] voce trabalha"))
                            trabalha = "Não" if "nao trabalho" in regime else (trab_raw or "Não")

                            conn.execute(text("""
                                INSERT INTO form_gc.lp2_respostas (pessoa_id, edicao, trilha, escola, genero, etnia, trabalha)
                                VALUES (:p_id, '2026', :trilha, :esc, :gen, :etn, :trab) ON CONFLICT DO NOTHING
                            """), {
                                "p_id": pessoa_id, 
                                "trilha": f.get(normalize_key("[GC 2026 LP2] trilha educacional")),
                                "esc": f.get(normalize_key("[GC 2026 LP2] qual escola")) or f.get(normalize_key("Nome da escola")),
                                "gen": genero, "etn": f.get(normalize_key("[GC 2026 LP2] etnia")), "trab": trabalha
                            })
                        trans.commit()
                    except Exception as e:
                        trans.rollback()
                        print(f"Erro ID {c.get('id')}: {e}")
                
                if len(contacts) < 100: break
                offset += 100
                time.sleep(2)
        print("--- PROCESSO FINALIZADO ---")

if __name__ == "__main__":
    process()