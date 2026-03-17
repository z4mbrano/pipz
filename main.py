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
    try:
        return datetime.strptime(clean, "%Y-%m-%d").strftime("%Y-%m-%d")
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
    texto = (str(etnia or "") + " " + str(qual_etnia or "") + " " + 
             str(custom_etnia or "") + " " + str(custom_qual or "")).lower()
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
    return res.json() if res.status_code == 200 else None

def process():
    if not DB_URL: return
    engine = create_engine(DB_URL)
    
    with engine.connect() as conn:
        print(f"--- INICIANDO LOTE: {datetime.now().strftime('%H:%M:%S')} ---")
        
        for list_id, handler in [("141", "lp1"), ("144", "lp2")]:
            print(f"Buscando novos contatos da {handler}...")
            url = "https://campuscaldeira.pipz.io/api/v1/contact/"
            params = {"list_id": list_id, "limit": 100, "api_key": PIPZ_KEY, "api_secret": PIPZ_SECRET}
            
            res = requests.get(url, params=params)
            if res.status_code != 200: continue
            batch = res.json().get('objects', [])
            print(f"Sincronizando {len(batch)} contatos...")

            for summary in batch:
                try:
                    detail = get_contact_detail(summary['id'])
                    if not detail: continue
                    f = extract_fields_logic(detail)
                    
                    raw_cpf = f.get("gc_2026_lp1_cpf") or f.get("gc_2026_lp2_cpf") or f.get("CPF") or f.get("cpf")
                    nums_cpf = re.sub(r'\D', '', str(raw_cpf)) if raw_cpf else None
                    final_cpf = nums_cpf if nums_cpf and len(nums_cpf) >= 11 else f"ID_{f.get('id')}"

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

                        # --- LP1 ---
                        if handler == "lp1":
                            sab = f.get("[2025] Como ficou sabendo do Geração Caldeira?") or f.get("gc_2026_lp1_origem")
                            cod = f.get("gc_2026_codigo_alumni") or f.get("[GC2026] codigo alumni") or f.get("contact_custom_gc2026_codigo_alumni") or f.get("gc2026_codigo_alumni")
                            conn.execute(text("""
                                INSERT INTO form_gc.lp1_respostas (pessoa_id, edicao, estado, cidade, como_ficou_sabendo, codigo_indicacao, data_cadastro, data_resposta)
                                VALUES (:p_id, '2026', :est, :cid, :sab, :cod, :dt, NOW())
                                ON CONFLICT (pessoa_id, edicao) DO UPDATE SET 
                                    como_ficou_sabendo = EXCLUDED.como_ficou_sabendo,
                                    codigo_indicacao = EXCLUDED.codigo_indicacao
                            """), {"p_id": pessoa_id, "est": f.get('state'), "cid": f.get('city_name'), "sab": sab, "cod": cod, "dt": format_timestamp(f.get('creation_date'))})

                        # --- LP2 (Mapeamento Completo solicitado) ---
                        if handler == "lp2":
                            # Campos complexos (Gênero e Etnia)
                            gen = normalize_genero(f.get("gc_2026_lp2_genero"), f.get("gc_2026_genero"), f.get('gender'), f.get("contact_custom_gc_2026_lp2_genero"))
                            etn = normalize_etnia(f.get("gc_2026_lp2_etnia"), f.get("gc_2026_lp2_qual_etnia"), f.get("contact_custom_gc_2026_lp2_etnia"), f.get("contact_custom_gc_2026_lp2_qual_etnia"))
                            
                            # Campos simples com fallbacks OR
                            tri = f.get("gc_2026_lp2_trilha_educacional") or f.get("contact_custom_gc_2026_lp2_trilha_educacional")
                            esc = f.get("gc_2026_lp2_qual_escola") or f.get("Nome da escola") or f.get("contact_custom_gc_2026_lp2_qual_escola")
                            tra_val = f.get("gc_2026_lp2_voce_trabalha") or f.get("contact_custom_gc_2026_lp2_voce_trabalha")
                            
                            # Novos campos mapeados da exportação
                            ens_med = f.get("contact_custom_gc_2026_lp2_ensino_medio")
                            tip_esc = f.get("contact_custom_gc_2026_escola_publica_ou_privada")
                            semestre = f.get("contact_custom_gc_2026_lp2_qual_semestre_ano")
                            tur_esc = f.get("contact_custom_gc_2026_lp2_qual_turno")
                            pcd_val = f.get("contact_custom_gc_2026_lp2_acessibilidade")
                            pcd_qual = f.get("contact_custom_gc_2026_lp2_acessibilidade_se_sim")
                            inst_parc = f.get("contact_custom_gc_2026_lp2_instituio_parceira")
                            regime = f.get("contact_custom_gc_2026_lp2_regime_trabalho")
                            carga = f.get("contact_custom_gc_2026_lp2_turno_de_trabalho")

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
                                "p_id": pessoa_id, "tri": tri, "ens_med": ens_med, "esc": esc, "tip_esc": tip_esc,
                                "semestre": semestre, "tur_esc": tur_esc, "gen": gen, "etn": etn, "pcd": pcd_val,
                                "pcd_qual": pcd_qual, "inst": inst_parc, "tra": "Sim" if "sim" in str(tra_val or "").lower() else "Não",
                                "regime": regime, "carga": carga, "dt": format_timestamp(f.get('creation_date'))
                            })
                except Exception as e:
                    print(f"[ERRO] Usuário {summary.get('id')} não processado: {str(e)[:100]}")

        print("--- LOTE FINALIZADO COM SUCESSO ---")

if __name__ == "__main__":
    process()