import requests
import pandas as pd
import time
import os
from sqlalchemy import create_engine, text

# Lendo as credenciais dos Secrets do GitHub
PIPZ_TOKEN = os.getenv("PIPZ_TOKEN")
DB_URL = os.getenv("DB_URL") 
engine = create_engine(DB_URL)

def fetch_pipz(list_id):
    contacts = []
    offset = 0
    limit = 1000
    while True:
        url = f"https://campuscaldeira.pipz.io/api/v1/contact/?list_id={list_id}&limit={limit}&offset={offset}&extra_fields=1"
        res = requests.get(url, headers={"Authorization": f"Bearer {PIPZ_TOKEN}", "Accept": "application/json"})
        
        if res.status_code == 429:
            time.sleep(10)
            continue
        
        data = res.json()
        objs = data.get('objects', [])
        if not objs: break
        contacts.extend(objs)
        offset += limit
        if len(objs) < limit: break
    return contacts

def process_and_save():
    print("Iniciando sincronização...")
    # Listas: 141 (LP1) e 144 (LP2)
    for list_id in ["141", "144"]:
        contacts = fetch_pipz(list_id)
        
        with engine.begin() as conn:
            for c in contacts:
                # Extrai campos customizados
                f = {}
                for fs in c.get('fieldsets', []):
                    for field in fs.get('fields', []):
                        f[field['name']] = field.get('value')

                # TRATAMENTO DE CPF (Obrigatório no seu banco)
                cpf_bruto = f.get("gc_2026_lp2_cpf") or f.get("gc_2026_lp1_cpf") or f.get("cpf")
                if not cpf_bruto:
                    # Se não tem CPF, usamos o ID do Pipz para não dar erro de NOT NULL
                    cpf_bruto = f"SEM_CPF_{c.get('id')}"
                
                # 1. UPSERT na tabela PESSOAS (Se o CPF já existe, ele atualiza o nome/email)
                p_id_res = conn.execute(text("""
                    INSERT INTO form_gc.pessoas (cpf, email, nome, data_nascimento, telefone)
                    VALUES (:cpf, :email, :nome, :birth, :tel)
                    ON CONFLICT (cpf) DO UPDATE SET 
                        email = EXCLUDED.email,
                        nome = EXCLUDED.nome
                    RETURNING id
                """), {
                    "cpf": str(cpf_bruto)[:14],
                    "email": c.get("email"),
                    "nome": c.get("name"),
                    "birth": c.get("birthday") or f.get("birthdate"),
                    "tel": c.get("phone")
                })
                pessoa_id = p_id_res.fetchone()[0]

                # 2. SE FOR LP1, SALVA NA TABELA LP1
                if list_id == "141":
                    conn.execute(text("""
                        INSERT INTO form_gc.lp1_respostas (pessoa_id, edicao, estado, cidade, data_resposta)
                        VALUES (:p_id, '2026', :est, :cid, NOW())
                        ON CONFLICT DO NOTHING
                    """), {"p_id": pessoa_id, "est": c.get("state"), "cid": c.get("city_name")})

                # 3. SE FOR LP2, SALVA NA TABELA LP2 (Com padronização)
                if list_id == "144":
                    # Padronização de Gênero
                    gen_raw = str(f.get('gc_2026_lp2_genero') or c.get('gender') or "").lower()
                    genero = "Masculino" if gen_raw.startswith(('h', 'mas')) else "Feminino" if gen_raw.startswith(('mu', 'f')) else "Outros"
                    
                    # Padronização de Etnia
                    etnia_raw = str(f.get('gc_2026_lp2_etnia') or "").lower()
                    etnia = "Branca" if "bran" in etnia_raw else "Parda" if "pard" in etnia_raw else "Preta" if "pret" in etnia_raw else "Outra"

                    # Padronização de Trabalho
                    trab_lp2 = f.get('gc_2026_lp2_voce_trabalha')
                    trab_empresa = str(f.get('_gc_2026_lp2_voc_trabalha_em_alguma_empresa') or "").lower()
                    trabalha = "Sim" if (trab_lp2 == "Sim" or (trab_empresa != "" and not trab_empresa.startswith('n') and trab_empresa != "null")) else "Não"

                    conn.execute(text("""
                        INSERT INTO form_gc.lp2_respostas (pessoa_id, edicao, trilha, escola, genero, etnia, trabalha)
                        VALUES (:p_id, '2026', :trilha, :escola, :genero, :etnia, :trabalha)
                        ON CONFLICT DO NOTHING
                    """), {
                        "p_id": pessoa_id, 
                        "trilha": f.get("gc_2026_lp2_trilha_educacional"),
                        "escola": f.get("gc_2026_lp2_qual_escola"),
                        "genero": genero, "etnia": etnia, "trabalha": trabalha
                    })

if __name__ == "__main__":
    process_and_save()