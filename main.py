import requests
import json
import time
import os
from sqlalchemy import create_engine, text
from datetime import datetime
import re

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

def extract_all_fields(contact):
    """
    Varre fieldsets de forma profunda para achar todos os campos.
    A API Pipz pode retornar fieldsets como dict (keyed) ou como lista.
    Indexa por LABEL e por NAME para máxima compatibilidade.
    """
    data = {}

    # Campos base do objeto raiz
    for k, v in contact.items():
        if not isinstance(v, (dict, list)):
            data[k] = v

    # Fieldsets podem vir como dict OU como lista dependendo da versão da API
    fs_raw = contact.get('fieldsets', {})

    if isinstance(fs_raw, dict):
        fs_list = list(fs_raw.values())
    elif isinstance(fs_raw, list):
        fs_list = fs_raw
    else:
        fs_list = []

    for fs in fs_list:
        if not isinstance(fs, dict):
            continue

        # Cada fieldset tem uma lista de fields
        fields = fs.get('fields', [])
        for field in fields:
            if not isinstance(field, dict):
                continue
            label = (field.get('label') or '').strip()
            name  = (field.get('name')  or '').strip()
            val   = field.get('value')

            # Só sobrescreve se o valor atual for None/vazio
            if label:
                if label not in data or data[label] in (None, ''):
                    data[label] = val
            if name:
                if name not in data or data[name] in (None, ''):
                    data[name] = val

    return data

def normalize_genero(raw: str) -> str:
    r = (raw or '').lower()
    if any(x in r for x in ['homem', 'masc', 'male', 'h ']):
        return 'Masculino'
    if any(x in r for x in ['mulher', 'fem', 'female']):
        return 'Feminino'
    if r:
        return 'Outros'
    return None

def fetch_pipz_page(list_id, offset=0, limit=100):
    """Busca uma página de contatos com paginação e retry para 429."""
    url = "https://campuscaldeira.pipz.io/api/v1/contact/"
    params = {
        "list_id": list_id,
        "limit": "50",
        "offset": offset,
        "extra_fields": "true",
        "include_fieldsets": "true",
        "api_key": PIPZ_KEY,
        "api_secret": PIPZ_SECRET
    }
    for attempt in range(4):
        try:
            res = requests.get(
                url, params=params,
                headers={"Accept": "application/json"},
                timeout=30
            )
            if res.status_code == 200:
                body = res.json()
                # A API pode retornar 'objects' ou 'results'
                return body.get('objects') or body.get('results') or []
            elif res.status_code == 429:
                wait = 20 * (attempt + 1)
                print(f"  [429] Rate limit. Aguardando {wait}s (tentativa {attempt+1}/4)...")
                time.sleep(wait)
            else:
                print(f"  [ERRO {res.status_code}] Lista {list_id} offset {offset}: {res.text[:200]}")
                return []
        except Exception as e:
            print(f"  [EXCEÇÃO] {e} — aguardando 5s...")
            time.sleep(5)
    return []

def fetch_all_contacts(list_id, page_size=100):
    """Busca todos os contatos de uma lista com paginação automática."""
    all_contacts = []
    offset = 0
    while True:
        batch = fetch_pipz_page(list_id, offset=offset, limit=page_size)
        if not batch:
            break
        all_contacts.extend(batch)
        print(f"  Lista {list_id}: {len(all_contacts)} contatos carregados (offset={offset})...")
        if len(batch) < page_size:
            break  # Última página
        offset += page_size
        time.sleep(2)  # Pausa entre páginas para evitar 429
    return all_contacts

def upsert_pessoa(conn, f, contact):
    raw_cpf = (
        f.get("CPF") or
        f.get("[2025] CPF") or
        f.get("gc_2026_lp1_cpf") or
        f.get("gc_2026_lp2_cpf")
    )
    cpf_limpo  = clean_cpf(raw_cpf)
    final_cpf  = cpf_limpo if cpf_limpo else f"ID_{contact.get('id')}"

    birth = format_date_to_db(
        contact.get('birthdate') or
        f.get('Birthdate') or
        f.get('revisar_data_de_nascimento')
    )
    tel = (
        contact.get('mobile_phone') or
        contact.get('phone') or
        f.get('telefone')
    )

    result = conn.execute(text("""
        INSERT INTO form_gc.pessoas (cpf, email, nome, data_nascimento, telefone)
        VALUES (:cpf, :email, :nome, :birth, :tel)
        ON CONFLICT (cpf) DO UPDATE SET
            email            = COALESCE(EXCLUDED.email, form_gc.pessoas.email),
            nome             = COALESCE(EXCLUDED.nome,  form_gc.pessoas.nome),
            telefone         = COALESCE(EXCLUDED.telefone, form_gc.pessoas.telefone),
            data_nascimento  = COALESCE(EXCLUDED.data_nascimento, form_gc.pessoas.data_nascimento)
        RETURNING id
    """), {
        "cpf":   final_cpf,
        "email": contact.get("email"),
        "nome":  contact.get("name"),
        "birth": birth,
        "tel":   str(tel)[:20] if tel else None
    })
    return result.fetchone()[0]

def upsert_lp1(conn, pessoa_id, f, contact):
    # FIX: captura estado e cidade com todos os aliases possíveis
    estado = (
        f.get("[GC 2026 LP1] Estado") or
        contact.get("state") or
        f.get("gc_2026_lp1_estado")
    )
    cidade = (
        f.get("[GC2026] LP1 Cidades") or
        contact.get("city_name") or
        f.get("gc_2026_lp1_cidades")
    )
    sabendo = (
        f.get("[GC 2026 LP1] Origem") or
        f.get("gc_2026_lp1_origem") or
        f.get("[2025] Como ficou sabendo do Geração Caldeira?")
    )
    codigo = (
        f.get("[GC2026] codigo alumni") or
        f.get("[2025] CUPOM GC 2025") or
        f.get("[2024] CUPOM GC 2024")
    )

    conn.execute(text("""
        INSERT INTO form_gc.lp1_respostas
            (pessoa_id, edicao, estado, cidade, como_ficou_sabendo, codigo_indicacao, data_resposta)
        VALUES
            (:p_id, '2026', :est, :cid, :sab, :cod, NOW())
        ON CONFLICT DO NOTHING
    """), {
        "p_id": pessoa_id,
        "est":  estado,
        "cid":  cidade,
        "sab":  sabendo,
        "cod":  codigo
    })

def upsert_lp2(conn, pessoa_id, f, contact):
    # FIX: captura gênero — o campo "[GC 2026 LP2] Gênero" vem null para muitos contatos,
    # mas "[GC 2026] Genero" (sem acento, sem LP2) vem preenchido
    genero_raw = (
        f.get("[GC 2026 LP2] Gênero") or
        f.get("[GC 2026] Genero") or        # ← campo alternativo (sem acento)
        f.get("gc_2026_lp2_genero") or
        f.get("gc_2026_genero") or
        f.get("[2025] GÊNERO") or
        ""
    )
    genero = normalize_genero(genero_raw)

    # FIX: captura todos os campos LP2 com seus aliases completos
    trilha = (
        f.get("[GC 2026 LP2] trilha educacional") or
        f.get("gc_2026_lp2_trilha") or
        f.get("[2025] TRILHAS 2025")
    )
    escola = (
        f.get("[GC 2026 LP2] qual escola") or
        f.get("gc_2026_lp2_escola") or
        f.get("Nome da escola")
    )
    tipo_escola = (
        f.get("[GC 2026] Escola publica ou privada") or
        f.get("gc_2026_tipo_escola") or
        f.get("[2025] TIPO DE ESCOLA")
    )
    ensino_medio = (
        f.get("[GC 2026 LP2] ensino médio") or
        f.get("gc_2026_lp2_ensino_medio")
    )
    semestre = (
        f.get("[GC 2026 LP2] qual semestre/ano") or
        f.get("gc_2026_lp2_semestre") or
        f.get("[2025] SÉRIE DE ESTUDO")
    )
    turno_escola = (
        f.get("[GC 2026 LP2] qual turno") or
        f.get("gc_2026_lp2_turno") or
        f.get("[2025] TURNO DE ESTUDO")
    )
    etnia = (
        f.get("[GC 2026 LP2] qual etnia") or
        f.get("[GC 2026 LP2] etnia") or
        f.get("gc_2026_lp2_etnia") or
        f.get("[2025] ETNIA")
    )
    pcd = (
        f.get("[GC 2026 LP2] Acessibilidade") or
        f.get("gc_2026_lp2_pcd") or
        f.get("[2025] DEFICIÊNCIA")
    )
    qual_pcd = (
        f.get("[GC 2026 LP2] Acessibilidade se sim") or
        f.get("gc_2026_lp2_qual_pcd") or
        f.get("[2025] DESCREVA A DEFICIÊNCIA")
    )
    instituicao = (
        f.get("[GC 2026 LP2] instituição parceira") or
        f.get("[GC 2026] instituição parceira") or
        f.get("gc_2026_lp2_inst") or
        f.get("[2025] INSTITUIÇÃO PARCEIRA")
    )
    # FIX: "[GC 2026 LP2] você trabalha" frequentemente vem null;
    # o campo alternativo livre "[GC 2026 LP2] regime trabalho" diz "Não trabalho"
    trabalha_raw = (
        f.get("[GC 2026 LP2] você trabalha") or
        f.get("gc_2026_lp2_trabalha") or
        f.get("[ GC 2026 LP2] Você trabalha em alguma empresa?") or
        f.get("[2025] VOCÊ TRABALHA?")
    )
    # Infere a partir do regime caso o campo trabalha esteja vazio
    regime = (
        f.get("[GC 2026 LP2] regime trabalho") or
        f.get("gc_2026_lp2_regime") or
        f.get("[2025] REGIME DE TRABALHO")
    )
    if not trabalha_raw and regime:
        trabalha_raw = "Não" if "não trabalho" in (regime or '').lower() else regime

    carga = (
        f.get("[GC 2026 LP2] carga horaria") or
        f.get("gc_2026_lp2_carga") or
        f.get("[2025] CARGA HORÁRIA DE TRABALHO")
    )

    conn.execute(text("""
        INSERT INTO form_gc.lp2_respostas (
            pessoa_id, edicao, trilha, ensino_medio, escola, tipo_escola,
            semestre, turno_escola, genero, etnia, pcd, qual_pcd,
            instituicao_parceira, trabalha, regime, carga_horaria
        )
        VALUES (
            :p_id, '2026', :trilha, :ensino_medio, :escola, :tipo_escola,
            :semestre, :turno_escola, :genero, :etnia, :pcd, :qual_pcd,
            :inst, :trabalha, :regime, :carga
        )
        ON CONFLICT DO NOTHING
    """), {
        "p_id":        pessoa_id,
        "trilha":      trilha,
        "ensino_medio": ensino_medio,
        "escola":      escola,
        "tipo_escola": tipo_escola,
        "semestre":    semestre,
        "turno_escola": turno_escola,
        "genero":      genero,
        "etnia":       etnia,
        "pcd":         pcd,
        "qual_pcd":    qual_pcd,
        "inst":        instituicao,
        "trabalha":    trabalha_raw,
        "regime":      regime,
        "carga":       str(carga) if carga else None,
    })

def process():
    if not DB_URL:
        print("[ERRO] Variável DB_URL não definida.")
        return
    if not PIPZ_KEY or not PIPZ_SECRET:
        print("[ERRO] PIPZ_TOKEN ou PIPZ_SECRET não definidos.")
        return

    engine = create_engine(DB_URL)

    with engine.begin() as conn:
        print("--- CONEXÃO ESTABELECIDA ---")

        for list_id, handler in [("141", "lp1"), ("144", "lp2")]:
            contacts = fetch_all_contacts(list_id)
            print(f"\nLista {list_id} ({handler}): {len(contacts)} contatos no total.")

            ok, erros = 0, 0
            for c in contacts:
                try:
                    f = extract_all_fields(c)
                    pessoa_id = upsert_pessoa(conn, f, c)

                    if handler == "lp1":
                        upsert_lp1(conn, pessoa_id, f, c)
                    else:
                        upsert_lp2(conn, pessoa_id, f, c)
                    ok += 1
                except Exception as e:
                    erros += 1
                    print(f"  [ERRO contato {c.get('id')}]: {e}")

            print(f"  → {ok} inseridos/atualizados, {erros} erros.")
            time.sleep(3)  # Pausa entre listas

        print("\n--- PROCESSO FINALIZADO ---")

if __name__ == "__main__":
    process()