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
    clean = str(date_str)[:10].replace("/", "-")
    try: return datetime.strptime(clean, "%Y-%m-%d").strftime("%Y-%m-%d")
    except:
        try: return datetime.strptime(clean, "%d-%m-%Y").strftime("%Y-%m-%d")
        except: return None

def format_timestamp(ts_str):
    if not ts_str: return None
    try: return datetime.strptime(ts_str[:19], "%Y-%m-%dT%H:%M:%S")
    except: return None

def normalize_genero(lp2, g2026, root, custom_lp2):
    val = lp2 or g2026 or root or custom_lp2
    if not val: return "Não informado"
    txt = str(val).lower().strip()
    if txt.startswith(('m', 'f', 'mu')) or "mulher" in txt or "fem" in txt: return "Feminino"
    if txt.startswith(('h', 'mas')) or "homem" in txt: return "Masculino"
    return "Outros"

def normalize_etnia(etnia, qual_etnia, custom_etnia, custom_qual):
    texto = (str(etnia or "") + " " + str(qual_etnia or "") + " " + str(custom_etnia or "") + " " + str(custom_qual or "")).lower()
    if "bran" in texto: return "Branca"
    if "pard" in texto: return "Parda"
    if "pret" in texto or "negr" in texto: return "Preta"
    if "amar" in texto: return "Amarela"
    if "indi" in texto: return "Indígena"
    return "Outra" if texto.strip() else None

def extract_fields_logic(contact_full):
    if not contact_full: return {}
    data = {}
    for k, v in contact_full.items():
        if not isinstance(v, (dict, list)): data[k] = v
        
    cf = contact_full.get('custom_fields', {})
    if isinstance(cf, dict):
        for k, v in cf.items(): data[k] = v
            
    fs_raw = contact_full.get('fieldsets', [])
    fs_list = fs_raw.values() if isinstance(fs_raw, dict) else fs_raw
    for fs in fs_list:
        for field in fs.get('fields', []):
            name, label, val = field.get('name'), field.get('label'), field.get('value')
            if name: data[name] = val
            if label: data[label] = val
    return data

def get_contact_detail(contact_id):
    url = f"https://campuscaldeira.pipz.io/api/v1/contact/{contact_id}/"
    params = {"extra_fields": "1", "api_key": PIPZ_KEY, "api_secret": PIPZ_SECRET}
    res = requests.get(url, params=params, headers={"Accept": "application/json"})
    if res.status_code == 429:
        time.sleep(10)
    return res.json() if res.status_code == 200 else None

def process():
    if not DB_URL: return
    engine = create_engine(DB_URL)
    
    with engine.connect() as conn:
        print(f"--- INICIANDO VARREDURA DE RECUPERAÇÃO: {datetime.now().strftime('%H:%M:%S')} ---")
        
        # 2. CACHE EM MEMÓRIA DE PESSOAS VÁLIDAS
        print("Carregando e-mails que já estão prontos no banco...")
        lp1_ok = set(row[0] for row in conn.execute(text("SELECT p.email FROM form_gc.pessoas p JOIN form_gc.lp1_respostas r ON p.id = r.pessoa_id WHERE p.email IS NOT NULL")).fetchall())
        lp2_ok = set(row[0] for row in conn.execute(text("SELECT p.email FROM form_gc.pessoas p JOIN form_gc.lp2_respostas r ON p.id = r.pessoa_id WHERE p.email IS NOT NULL")).fetchall())
        print(f"Cache pronto: {len(lp1_ok)} na LP1 | {len(lp2_ok)} na LP2\n")

        for list_id, handler in [("141", "lp1"), ("144", "lp2")]:
            print(f"--- Sincronizando {handler.upper()} (Lista {list_id}) ---")
            offset = 0
            limit = 100
            
            while True:
                url = "https://campuscaldeira.pipz.io/api/v1/contact/"
                params = {
                    "list_id": list_id, "limit": limit, "offset": offset, 
                    "api_key": PIPZ_KEY, "api_secret": PIPZ_SECRET,
                    "include_fieldsets": "1", "extra_fields": "1"
                }
                
                success = False
                for attempt in range(3):
                    res = requests.get(url, params=params)
                    if res.status_code == 200:
                        success = True
                        break
                    elif res.status_code == 429:
                        print(f"[{handler}] Pipz limitou (429). Esperando 30 segundos...")
                        time.sleep(30)
                    else:
                        time.sleep(5)
                        
                if not success:
                    print(f"[{handler}] ERRO FATAL DO PIPZ. Status: {res.status_code} - Resposta: {res.text[:200]}")
                    break
                
                batch = res.json().get('objects', [])
                if not batch: 
                    print(f"[{handler}] Fim da lista!")
                    break
                
                processados = 0
                ignorados = 0
                pulados = 0

                for summary in batch:
                    email = summary.get('email')
                    
                    if email:
                        if handler == "lp1" and email in lp1_ok:
                            pulados += 1
                            continue
                        if handler == "lp2" and email in lp2_ok:
                            pulados += 1
                            continue

                    f = extract_fields_logic(summary)
                    
                    # Ampliamos a busca do CPF para pegar todos os campos possíveis
                    raw_cpf = f.get("gc_2026_lp1_cpf") or f.get("gc_2026_lp2_cpf") or f.get("CPF") or f.get("cpf") or f.get("contact_custom_gc_2026_lp1_cpf") or f.get("document") or f.get("document_number")
                    nums_cpf = re.sub(r'\D', '', str(raw_cpf)) if raw_cpf else None
                    tri = f.get("gc_2026_lp2_trilha_educacional") or f.get("contact_custom_gc_2026_lp2_trilha_educacional")
                    
                    precisa_detail = False
                    if not nums_cpf or len(nums_cpf) < 11: precisa_detail = True
                    if handler == "lp2" and (not tri or str(tri).strip() == ""): precisa_detail = True
                        
                    if precisa_detail:
                        detail = get_contact_detail(summary['id'])
                        if detail: f.update(extract_fields_logic(detail))

                    raw_cpf = f.get("gc_2026_lp1_cpf") or f.get("gc_2026_lp2_cpf") or f.get("CPF") or f.get("cpf") or f.get("contact_custom_gc_2026_lp1_cpf") or f.get("document") or f.get("document_number")
                    nums_cpf = re.sub(r'\D', '', str(raw_cpf)) if raw_cpf else None
                    
                    # Se mesmo pedindo detalhe ele não tem CPF, a gente aceita como ID_ para não perder a pessoa
                    final_cpf = nums_cpf[:11] if nums_cpf and len(nums_cpf) >= 11 else f"ID_{summary.get('id')}"
                    
                    if handler == "lp2":
                        tri = f.get("gc_2026_lp2_trilha_educacional") or f.get("contact_custom_gc_2026_lp2_trilha_educacional")
                        if not tri or str(tri).strip() == "":
                            ignorados += 1
                            continue # Só barra da LP2 se a pessoa realmente não escolheu trilha
                    
                    if handler == "lp1":
                        est = f.get('state')
                        cid = f.get('city_name')
                        if not est and not cid:
                            ignorados += 1
                            continue # Caiu na lista mas não preencheu estado/cidade
                            
                    processados += 1

                    try:
                        with conn.begin():
                            p_res = conn.execute(text("""
                                INSERT INTO form_gc.pessoas (cpf, email, nome, data_nascimento, telefone)
                                VALUES (:cpf, :email, :nome, :birth, :tel)
                                ON CONFLICT (cpf) DO UPDATE SET 
                                    email = EXCLUDED.email, nome = EXCLUDED.nome,
                                    data_nascimento = COALESCE(EXCLUDED.data_nascimento, form_gc.pessoas.data_nascimento),
                                    telefone = COALESCE(EXCLUDED.telefone, form_gc.pessoas.telefone)
                                RETURNING id
                            """), {
                                "cpf": final_cpf, "email": f.get('email'), "nome": f.get('name'),
                                "birth": format_date_to_db(f.get('birthdate') or f.get('birthday')),
                                "tel": f.get('mobile_phone') or f.get('phone')
                            })
                            pessoa_id = p_res.fetchone()[0]

                            if handler == "lp1":
                                sab = f.get("[2025] Como ficou sabendo do Geração Caldeira?") or f.get("gc_2026_lp1_origem") or f.get("contact_custom_gc_2026_lp1_origem")
                                cod = f.get("gc2026_codigo_alumni") or f.get("gc_2026_codigo_alumni") or f.get("contact_custom_gc2026_codigo_alumni")
                                conn.execute(text("""
                                    INSERT INTO form_gc.lp1_respostas (pessoa_id, edicao, estado, cidade, como_ficou_sabendo, codigo_indicacao, data_cadastro, data_resposta)
                                    VALUES (:p_id, '2026', :est, :cid, :sab, :cod, :dt, NOW())
                                    ON CONFLICT (pessoa_id, edicao) DO UPDATE SET como_ficou_sabendo = EXCLUDED.como_ficou_sabendo, codigo_indicacao = EXCLUDED.codigo_indicacao
                                """), {"p_id": pessoa_id, "est": est, "cid": cid, "sab": sab, "cod": cod, "dt": format_timestamp(f.get('creation_date'))})
                                if f.get('email'): lp1_ok.add(f.get('email'))

                            if handler == "lp2":
                                gen = normalize_genero(f.get("gc_2026_lp2_genero"), f.get("gc_2026_genero"), f.get('gender'), f.get("contact_custom_gc_2026_lp2_genero"))
                                etn = normalize_etnia(f.get("gc_2026_lp2_etnia"), f.get("gc_2026_lp2_qual_etnia"), f.get("contact_custom_gc_2026_lp2_etnia"), f.get("contact_custom_gc_2026_lp2_qual_etnia"))
                                tra_val = f.get("gc_2026_lp2_voce_trabalha") or f.get("contact_custom_gc_2026_lp2_voce_trabalha")
                                
                                conn.execute(text("""
                                    INSERT INTO form_gc.lp2_respostas (
                                        pessoa_id, edicao, trilha, ensino_medio, escola, tipo_escola, 
                                        semestre, turno_escola, genero, etnia, pcd, qual_pcd, 
                                        instituicao_parceira, trabalha, regime, carga_horaria, data_cadastro
                                    )
                                    VALUES (
                                        :p_id, '2026', :tri, :ens_med, :esc, :tip_esc, 
                                        :semestre, :tur_esc, :gen, :etn, :pcd, :pcd_qual, 
                                        :inst, :tra, :regime, :carga, :dt
                                    )
                                    ON CONFLICT (pessoa_id, edicao) DO UPDATE SET 
                                        trilha = EXCLUDED.trilha, ensino_medio = EXCLUDED.ensino_medio,
                                        escola = EXCLUDED.escola, tipo_escola = EXCLUDED.tipo_escola,
                                        semestre = EXCLUDED.semestre, turno_escola = EXCLUDED.turno_escola,
                                        genero = EXCLUDED.genero, etnia = EXCLUDED.etnia,
                                        pcd = EXCLUDED.pcd, qual_pcd = EXCLUDED.qual_pcd,
                                        instituicao_parceira = EXCLUDED.instituicao_parceira,
                                        trabalha = EXCLUDED.trabalha, regime = EXCLUDED.regime,
                                        carga_horaria = EXCLUDED.carga_horaria
                                """), {
                                    "p_id": pessoa_id, "tri": tri,
                                    "ens_med": f.get("contact_custom_gc_2026_lp2_ensino_medio"), "esc": f.get("gc_2026_lp2_qual_escola") or f.get("contact_custom_gc_2026_lp2_qual_escola") or f.get("Nome da escola"),
                                    "tip_esc": f.get("contact_custom_gc_2026_escola_publica_ou_privada"), "semestre": f.get("contact_custom_gc_2026_lp2_qual_semestre_ano"), "tur_esc": f.get("contact_custom_gc_2026_lp2_qual_turno"),
                                    "gen": gen, "etn": etn, "pcd": f.get("contact_custom_gc_2026_lp2_acessibilidade"), "pcd_qual": f.get("contact_custom_gc_2026_lp2_acessibilidade_se_sim"), "inst": f.get("contact_custom_gc_2026_lp2_instituio_parceira"),
                                    "tra": "Sim" if "sim" in str(tra_val or "").lower() else "Não", "regime": f.get("contact_custom_gc_2026_lp2_regime_trabalho"), "carga": f.get("contact_custom_gc_2026_lp2_turno_de_trabalho"), "dt": format_timestamp(f.get('creation_date'))
                                })
                                if f.get('email'): lp2_ok.add(f.get('email'))
                    except Exception as e:
                        print(f"[ERRO BANCO] ID {summary.get('id')}: {str(e)[:100]}")
                
                print(f"[{handler}] Página (Offset {offset}): {pulados} já validados e pulados | {processados} novos | {ignorados} ignorados.")
                offset += limit
                time.sleep(0.5)
                
        print("\n--- VARREDURA FINALIZADA COM SUCESSO ---")

if __name__ == "__main__":
    process()