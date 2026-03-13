import requests
import pandas as pd
import time
import os
from sqlalchemy import create_engine, text

# Captura de variáveis de ambiente
PIPZ_KEY = os.getenv("PIPZ_TOKEN")
PIPZ_SECRET = os.getenv("PIPZ_SECRET")
DB_URL = os.getenv("DB_URL")

def fetch_pipz(list_id):
    contacts = []
    offset = 0
    limit = 1000
    while True:
        url = f"https://campuscaldeira.pipz.io/api/v1/contact/"
        params = {
            "list_id": list_id, 
            "limit": "20",#limit, 
            "offset": offset,
            "extra_fields": "1", 
            "api_key": PIPZ_KEY, 
            "api_secret": PIPZ_SECRET
        }
        res = requests.get(url, params=params, headers={"Accept": "application/json"})
        if res.status_code == 429:
            time.sleep(15)
            continue
        if res.status_code != 200:
            print(f"Erro na API Pipz: {res.status_code}")
            break
        data = res.json()
        objs = data.get('objects', [])
        if not objs: break
        contacts.extend(objs)
        offset += limit
        if len(objs) < limit: break
    return contacts

def get_fields(contact):
    """Extrai campos ignorando maiúsculas/minúsculas e espaços"""
    f_dict = {}
    for fs in contact.get('fieldsets', []):
        for f in fs.get('fields', []):
            name = f.get('name', '').strip()
            f_dict[name] = f.get('value')
    return f_dict

def process():
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        print("Conexão estabelecida.")
        for list_id in ["141", "144"]:
            contacts = fetch_pipz(list_id)
            print(f"Iniciando lista {list_id} ({len(contacts)} contatos)...")
            
            for c in contacts:
                f = get_fields(c)
                
                # --- MAPEMANTO PESSOAS ---
                cpf = f.get("CPF") or f.get("gc_2026_lp1_cpf") or f.get("gc_2026_lp2_cpf")
                if not cpf: cpf = f"ID_{c.get('id')}"
                
                # Data de nascimento nativa do Pipz ou do campo específico
                birth = c.get('birthdate') or f.get('revisar_data_de_nascimento')
                tel = c.get('mobile_phone') or c.get('phone') or f.get('telefone')

                trans = conn.begin()
                try:
                    p_res = conn.execute(text("""
                        INSERT INTO form_gc.pessoas (cpf, email, nome, data_nascimento, telefone)
                        VALUES (:cpf, :email, :nome, :birth, :tel)
                        ON CONFLICT (cpf) DO UPDATE SET email = EXCLUDED.email, nome = EXCLUDED.nome, telefone = EXCLUDED.telefone, data_nascimento = EXCLUDED.data_nascimento
                        RETURNING id
                    """), {
                        "cpf": str(cpf)[:14], "email": c.get("email"), "nome": c.get("name"),
                        "birth": birth, "tel": str(tel)[:20]
                    })
                    pessoa_id = p_res.fetchone()[0]

                    # --- MAPEMANTO LP1 ---
                    if list_id == "141":
                        conn.execute(text("""
                            INSERT INTO form_gc.lp1_respostas (pessoa_id, edicao, estado, cidade, como_ficou_sabendo, codigo_indicacao, data_resposta)
                            VALUES (:p_id, '2026', :est, :cid, :sabendo, :cupom, NOW())
                            ON CONFLICT DO NOTHING
                        """), {
                            "p_id": pessoa_id, "est": f.get("[GC 2026 LP1] Estado") or c.get("state"),
                            "cid": f.get("[GC2026] LP1 Cidades") or c.get("city_name"),
                            "sabendo": f.get("[GC 2026 LP1] Origem") or f.get("[2025] Como ficou sabendo do Geração Caldeira?"),
                            "cupom": f.get("[2025] CUPOM GC 2025")
                        })

                    # --- MAPEMANTO LP2 ---
                    if list_id == "144":
                        g_raw = str(f.get('[GC 2026 LP2] Gênero') or f.get('[2025] GÊNERO') or "").lower()
                        genero = "Masculino" if "homem" in g_raw or "masc" in g_raw else "Feminino" if "mulher" in g_raw or "fem" in g_raw else "Outros"
                        
                        conn.execute(text("""
                            INSERT INTO form_gc.lp2_respostas (pessoa_id, edicao, trilha, escola, genero, etnia, trabalha)
                            VALUES (:p_id, '2026', :trilha, :escola, :genero, :etnia, :trabalha)
                            ON CONFLICT DO NOTHING
                        """), {
                            "p_id": pessoa_id, 
                            "trilha": f.get("[GC 2026 LP2] trilha educacional") or f.get("[2025] TRILHAS 2025"),
                            "escola": f.get("[GC 2026 LP2] qual escola") or f.get("[2025] ESCOLA/FACULDADE"),
                            "genero": genero, 
                            "etnia": f.get("[GC 2026 LP2] etnia") or f.get("[2025] ETNIA"),
                            "trabalha": f.get("[GC 2026 LP2] você trabalha") or f.get("[2025] VOCÊ TRABALHA?")
                        })
                    trans.commit()
                except Exception as e:
                    trans.rollback()
                    print(f"Erro no contato {c.get('id')}: {e}")

if __name__ == "__main__":
    process()