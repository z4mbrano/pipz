import requests
import pandas as pd
import time
import os
from sqlalchemy import create_engine, text

# Captura de variáveis de ambiente
PIPZ_KEY = os.getenv("PIPZ_TOKEN")    # Sua API Key
PIPZ_SECRET = os.getenv("PIPZ_SECRET") # Sua API Secret
DB_URL = os.getenv("DB_URL")

def fetch_pipz(list_id):
    contacts = []
    offset = 0
    limit = 1000
    while True:
        # Passando as credenciais diretamente na Query (Padrão Pipz)
        url = f"https://campuscaldeira.pipz.io/api/v1/contact/"
        params = {
            "list_id": list_id,
            "limit": limit,
            "offset": offset,
            "extra_fields": "1",
            "api_key": PIPZ_KEY,
            "api_secret": PIPZ_SECRET
        }
        
        res = requests.get(url, params=params, headers={"Accept": "application/json"})
        
        if res.status_code == 429:
            print("Pipz pediu para esperar (429)...")
            time.sleep(15)
            continue
        if res.status_code != 200:
            print(f"Erro na API Pipz: {res.status_code} - {res.text}")
            break
            
        data = res.json()
        objs = data.get('objects', [])
        if not objs: break
        contacts.extend(objs)
        offset += limit
        if len(objs) < limit: break
    return contacts

def process():
    if not DB_URL or not PIPZ_KEY or not PIPZ_SECRET:
        print("ERRO: As variáveis PIPZ_KEY, PIPZ_SECRET ou DB_URL não foram configuradas nos Secrets!")
        return

    try:
        engine = create_engine(DB_URL)
        # Teste de conexão imediato
        with engine.connect() as conn:
            print("Conexão com o banco estabelecida com sucesso!")
            
            for list_id in ["141", "144"]:
                contacts = fetch_pipz(list_id)
                print(f"Iniciando carga da lista {list_id} ({len(contacts)} contatos)...")
                
                for c in contacts:
                    # Busca campos customizados
                    f = {}
                    for fs in c.get('fieldsets', []):
                        for field in fs.get('fields', []):
                            f[field['name']] = field.get('value')

                    # Tratamento de CPF para não violar o NOT NULL do seu banco
                    cpf = f.get("gc_2026_lp2_cpf") or f.get("gc_2026_lp1_cpf") or f.get("cpf")
                    if not cpf: cpf = f"ID_{c.get('id')}"

                    # 1. UPSERT Pessoa
                    p_res = conn.execute(text("""
                        INSERT INTO form_gc.pessoas (cpf, email, nome, data_nascimento, telefone)
                        VALUES (:cpf, :email, :nome, :birth, :tel)
                        ON CONFLICT (cpf) DO UPDATE SET email = EXCLUDED.email, nome = EXCLUDED.nome
                        RETURNING id
                    """), {
                        "cpf": str(cpf)[:14],
                        "email": c.get("email") or f"sem_email_{c.get('id')}@caldeira.com",
                        "nome": c.get("name") or "Sem Nome",
                        "birth": c.get("birthday") or f.get("birthdate"),
                        "tel": str(c.get("phone"))[:20]
                    })
                    p_id = p_res.fetchone()[0]

                    # 2. Respostas LP1 (Sincronização)
                    if list_id == "141":
                        conn.execute(text("""
                            INSERT INTO form_gc.lp1_respostas (pessoa_id, edicao, estado, cidade, data_resposta)
                            VALUES (:p_id, '2026', :est, :cid, NOW())
                            ON CONFLICT DO NOTHING
                        """), {"p_id": p_id, "est": c.get("state"), "cid": c.get("city_name")})

                    # 3. Respostas LP2
                    if list_id == "144":
                        g_raw = str(f.get('gc_2026_lp2_genero') or c.get('gender') or "").lower()
                        genero = "Masculino" if g_raw.startswith(('h', 'mas')) else "Feminino" if g_raw.startswith(('mu', 'f')) else "Outros"
                        
                        conn.execute(text("""
                            INSERT INTO form_gc.lp2_respostas (pessoa_id, edicao, trilha, escola, genero, trabalha)
                            VALUES (:p_id, '2026', :trilha, :escola, :genero, :trabalha)
                            ON CONFLICT DO NOTHING
                        """), {
                            "p_id": p_id, "trilha": f.get("gc_2026_lp2_trilha_educacional"),
                            "escola": f.get("gc_2026_lp2_qual_escola"), "genero": genero, "trabalha": "Sim" if f.get("gc_2026_lp2_voce_trabalha") == "Sim" else "Não"
                        })
                conn.commit()
                print(f"Lista {list_id} finalizada.")

    except Exception as e:
        print(f"Ocorreu um erro fatal: {e}")
        exit(1) # Força o GitHub a mostrar que deu erro

if __name__ == "__main__":
    process()