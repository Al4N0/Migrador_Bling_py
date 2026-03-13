"""
contas_receber_migrator.py — Migrador de Contas a Receber do Bling para MySQL
"""

import json
from loguru import logger
import traceback
from core import BlingAPI, Database

class ContasReceberMigrator:
    """Migrador de Contas a Receber da API Bling V3 para MySQL"""

    def __init__(self, api: BlingAPI, db: Database, on_progress=None, pause_event=None):
        self.api = api
        self.db = db
        self.on_progress = on_progress
        self.pause_event = pause_event

    def _report_progress(self, current: int, total: int, message: str):
        """Notifica a interface sobre o progresso da migração."""
        if self.on_progress:
            self.on_progress(current, total, message)

    def _fetch_contas_list(self, page: int) -> list[dict]:
        """
        GET {{baseUrl}}/contatos?pagina=1&limite=100
        A API de contas a receber retorna as listar paginadas. &direcao=A para ordem mais antiga primeiro (opcional dependendo de como Contas a Receber se comporta na API, mas recomendável).
        """
        endpoint = f"/contas/receber?pagina={page}&limite=100"
        response = self.api.request(endpoint)

        if response and "data" in response: 
            return response["data"]
        return []

    def _fetch_conta_details(self, conta_id: int) -> dict | None:
        """
        GET {{baseUrl}}/contas/receber/:idConta
        """
        endpoint = f"/contas/receber/{conta_id}"
        response = self.api.request(endpoint)

        if response and "data" in response:
            return response["data"]
        return None

    def _save_conta(self, data: dict):
        """Grava a conta a receber no MySQL na tabela `conta_receber`."""
        
        conta_id = data.get("id")

        # Helper para tratamento de data vazia ("0000-00-00")
        def parse_date(d: str):
            if d == "0000-00-00" or not d:
                return None
            return d

        self.db.execute("""
            INSERT INTO conta_receber (
                id, situacao, vencimento, valor, idTransacao, linkQRCodePix, linkBoleto, dataEmissao,
                contato_id, contato_nome, contato_numeroDocumento, contato_tipo,
                formaPagamento_id, contaContabil_id,
                origem_id, origem_tipoOrigem,
                saldo, vencimentoOriginal, numeroDocumento, competencia, historico,
                numeroBanco, portador_id, categoria_id, vendedor_id, ocorrencia_tipo,
                borderos, json_completo
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s
            )
            ON DUPLICATE KEY UPDATE
                situacao = VALUES(situacao),
                vencimento = VALUES(vencimento),
                valor = VALUES(valor),
                idTransacao = VALUES(idTransacao),
                linkQRCodePix = VALUES(linkQRCodePix),
                linkBoleto = VALUES(linkBoleto),
                dataEmissao = VALUES(dataEmissao),
                contato_id = VALUES(contato_id),
                contato_nome = VALUES(contato_nome),
                contato_numeroDocumento = VALUES(contato_numeroDocumento),
                contato_tipo = VALUES(contato_tipo),
                formaPagamento_id = VALUES(formaPagamento_id),
                contaContabil_id = VALUES(contaContabil_id),
                origem_id = VALUES(origem_id),
                origem_tipoOrigem = VALUES(origem_tipoOrigem),
                saldo = VALUES(saldo),
                vencimentoOriginal = VALUES(vencimentoOriginal),
                numeroDocumento = VALUES(numeroDocumento),
                competencia = VALUES(competencia),
                historico = VALUES(historico),
                numeroBanco = VALUES(numeroBanco),
                portador_id = VALUES(portador_id),
                categoria_id = VALUES(categoria_id),
                vendedor_id = VALUES(vendedor_id),
                ocorrencia_tipo = VALUES(ocorrencia_tipo),
                borderos = VALUES(borderos),
                json_completo = VALUES(json_completo),
                atualizado_em = CURRENT_TIMESTAMP
        """, (
            conta_id,
            data.get("situacao", 0),
            parse_date(data.get("vencimento")),
            data.get("valor", 0),
            data.get("idTransacao", ""),
            data.get("linkQRCodePix", ""),
            data.get("linkBoleto", ""),
            parse_date(data.get("dataEmissao")),
            data.get("contato", {}).get("id") if data.get("contato") else None,
            data.get("contato", {}).get("nome", "") if data.get("contato") else "",
            data.get("contato", {}).get("numeroDocumento", "") if data.get("contato") else "",
            data.get("contato", {}).get("tipo", "") if data.get("contato") else "",
            data.get("formaPagamento", {}).get("id") if data.get("formaPagamento") else None,
            data.get("contaContabil", {}).get("id") if data.get("contaContabil") else None,
            data.get("origem", {}).get("id") if data.get("origem") else None,
            data.get("origem", {}).get("tipoOrigem", "") if data.get("origem") else "",
            data.get("saldo", 0),
            parse_date(data.get("vencimentoOriginal")),
            data.get("numeroDocumento", ""),
            parse_date(data.get("competencia")),
            data.get("historico", ""),
            data.get("numeroBanco", ""),
            data.get("portador", {}).get("id") if data.get("portador") else None,
            data.get("categoria", {}).get("id") if data.get("categoria") else None,
            data.get("vendedor", {}).get("id") if data.get("vendedor") else None,
            data.get("ocorrencia", {}).get("tipo") if data.get("ocorrencia") else None,
            json.dumps(data.get("borderos", []), ensure_ascii=False),
            json.dumps(data, ensure_ascii=False)
        ))

    def execute(self) -> int:
        """Executa migração completa do contas a receber."""

        self.db.create_contas_receber_tables()

        # FASE 1
        all_ids = []
        page = 1
        self._report_progress(0, 0, "Fase 1: Coletando lista de contas a receber...")

        while True:
            if self.pause_event:
                self.pause_event.wait()
                
            self._report_progress(len(all_ids), 0, f"Coletando página {page}...")
            contas = self._fetch_contas_list(page)

            if not contas:
                break

            for c in contas:
                contato = c.get("contato", {})
                nome_contato = contato.get("nome", "Desconhecido")
                all_ids.append({
                    "id": c["id"], 
                    "nome": nome_contato
                })

            page += 1   
            
        total = len(all_ids)
        self._report_progress(0, total, f"Fase 1 concluída: {total} contas encontradas")

        # Inverte pra processar a conta mais antiga primeiro caso a API volte Desc
        all_ids.reverse()

        # FASE 2
        processed = 0

        for i, rec in enumerate(all_ids):
            if self.db.record_exists("conta_receber", rec["id"]):
                self._report_progress(i + 1, total, f"Pulando Conta {rec['id']} ({rec['nome']})")
                continue

            self._report_progress(i + 1, total, f"Importando Conta {rec['id']} ({rec['nome']}) - {i+1}/{total}...")

            details = self._fetch_conta_details(rec["id"])
            if not details:
                continue

            try:
                self._save_conta(details)
                self.db.commit()
                if self.pause_event:
                    self.pause_event.wait()
                processed += 1
            except Exception as e:
                self.db.rollback()
                tb = traceback.format_exc()
                logger.error(f"Erro ao gravar conta a receber {rec['id']}: {e}\n{tb}") 
                self._report_progress(i + 1, total, f"Erro Crítico na Conta {rec['id']}: {e}")
                raise e

        self._report_progress(total, total, f"Migração de Contas a Receber concluída! {processed} migrados.")   
        return processed
