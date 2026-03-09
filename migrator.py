
"""
migrator.py — Migrador de Contatos do Bling para MySQL
"""

import json
from loguru import logger
from core import BlingAPI, Database

class ContatosMigrator:
    """Migrador de Contatos da API Bling V3 para MySQL"""

    def __init__(self, api: BlingAPI, db: Database, on_progress=None):
        self.api = api
        self.db = db
        self.on_progress = on_progress

    def _report_progress(self, current: int, total: int, message: str):
        """Notifica a interface sobre o progresso da migração."""
        if self.on_progress:
            self.on_progress(current, total, message)

    def _fetch_contact_list(self, page: int) -> list[dict]:
        """
        GET {{baseUrl}}/contatos?pagina=1&limite=100
        Retorna lista de contatos resumidos ou lista vazia.
        """
        endpoint = f"/contatos?pagina={page}&limite=100"
        response = self.api.request(endpoint)

        if response and "data" in response: 
            return response["data"]         # Lista de dicts
        return []

    def _fetch_contact_details(self, contact_id: int) -> dict | None:
        """
        GET {{baseUrl}}/contatos/:idContato
        Retorna o JSON completo do contato ou None.
        """
        endpoint = f"/contatos/{contact_id}"
        response = self.api.request(endpoint)

        if response and "data" in response:
            return response["data"]         # Dict com todas as informações
        return None

    def _save_contact(self, data: dict):
        """Grava contato no MySQL (INSERT ON DUPLICATE KEY UPDATE)."""

        # Extrair objetos nested (se não existir, retorna dict vazio)
        endereco = data.get("endereco", {})
        geral = endereco.get("geral", {})
        cobranca = endereco.get("cobranca", {})
        vendedor = data.get("vendedor", {})
        dados_adicionais = data.get("dadosAdicionais", {})
        financeiro = data.get("financeiro", {})
        finan_categoria = financeiro.get("categoria", {})
        pais = data.get("pais", {})                   
        data_nasc = dados_adicionais.get("dataNascimento")

        if data_nasc == "0000-00-00" or not data_nasc:
            data_nasc = None

        self.db.execute("""
            INSERT INTO contatos (
            id, nome, codigo, situacao, numero_documento,
            telefone, celular, fantasia, tipo, indicadorIe, ie, rg,
            inscricaoMunicipal, orgaoEmissor, email, emailNotaFiscal,
            end_geral_endereco, end_geral_cep, end_geral_bairro,
            end_geral_municipio, end_geral_uf, end_geral_numero,
            end_geral_complemento, end_cobranca_endereco, end_cobranca_cep,
            end_cobranca_bairro, end_cobranca_municipio, end_cobranca_uf,
            end_cobranca_numero, end_cobranca_complemento, orgaoPublico, vendedor,
            ad_dataNascimento, ad_sexo, ad_naturalidade, finan_limiteCredito,
            finan_condicaoPagamento, finan_categoria, pais,
            tiposContato, pessoasContato, json_completo
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            nome = VALUES(nome),
            situacao = VALUES(situacao),
            telefone = VALUES(telefone),
            celular = VALUES(celular),
            email= VALUES(email),
            json_completo = VALUES(json_completo),
            atualizado_em = CURRENT_TIMESTAMP

        """, (
            data.get("id"),
            data.get("nome"),
            data.get("codigo"),
            data.get("situacao"),
            data.get("numeroDocumento"),
            data.get("telefone"),
            data.get("celular"),
            data.get("fantasia"),
            data.get("tipo"),
            data.get("indicadorIe"),
            data.get("ie"),
            data.get("rg"),
            data.get("inscricaoMunicipal"),
            data.get("orgaoEmissor"),
            data.get("email"),
            data.get("emailNotaFiscal"),
            # Endereço Geral
            geral.get("endereco"),
            geral.get("cep"),
            geral.get("bairro"),
            geral.get("municipio"),
            geral.get("uf"),
            geral.get("numero"),
            geral.get("complemento"),
            # Endereço Cobrança
            cobranca.get("endereco"),
            cobranca.get("cep"),
            cobranca.get("bairro"),
            cobranca.get("municipio"),
            cobranca.get("uf"),
            cobranca.get("numero"),
            cobranca.get("complemento"),
            data.get("orgaoPublico"),
            vendedor.get("id"),
            # Dados Adicionais
            data_nasc,
            dados_adicionais.get("sexo"),
            dados_adicionais.get("naturalidade"),
            # Financeiro
            financeiro.get("limiteCredito"),
            financeiro.get("condicaoPagamento"),
            finan_categoria.get("id"),
            # Pais
            pais.get("nome"),
            # Arrays como JSON string
            json.dumps(data.get("tiposContato", []), ensure_ascii=False),
            json.dumps(data.get("pessoasContato", []), ensure_ascii=False),
            # JSON completo
            json.dumps(data, ensure_ascii=False),
        ))

    def execute(self) -> int:
        """Executa migração completa: Fase 1 (IDs) + Fase 2 (Detalhes)."""

        # Criar tabelas se não existirem
        self.db.create_contatos_tables()

        # ========== FASE 1: Coletar todod os IDs ==========
        all_ids = []
        page = 1
        self._report_progress(0, 0, "Fase 1: Coletando lista de contatos...")

        while True:
            self._report_progress(len(all_ids), 0, f"Coletando página {page}...")
            contacts = self._fetch_contact_list(page)

            if not contacts:       # Lista vazia = acabaram as páginas
                break

            for c in contacts:
                all_ids.append({"id": c["id"], "nome": c.get("nome", "")})

            page += 1   
        total = len(all_ids)
        self._report_progress(0, total, f"Fase 1 concluída: {total} contatos encontrados")

        # ========== FASE 2: Buscar detalhes e gravar ==========
        processed = 0

        for i, rec in enumerate(all_ids):
            # Verifica se já existe no banco (economiza chamadas na API)
            if self.db.record_exists("contatos", rec["id"]):
                self._report_progress(i + 1, total,
                    f"Pulando ID já migrado: {rec['id']} ({rec['nome']})")
                continue

            self._report_progress(i + 1, total,
                f"Importando {rec['id']} ({rec['nome']}) - {i+1}/{total}...")

            # Busca detalhes completos via API
            details = self._fetch_contact_details(rec["id"])
            if not details:
                continue

            # Grava no banco com transaction
            try:
                self._save_contact(details)
                self.db.commit()
                processed += 1
            except Exception as e:
                self.db.rollback()
                logger.error(f"Erro ao gravar contato {rec['id']}: {e}") 

        self._report_progress(total, total,
            f"Migração concluída! {processed} contatos processados")   
        return processed            
                
         

