"""
formas_pagamento_migrator.py — Migrador de Formas de Pagamento do Bling para MySQL
"""

import json
from loguru import logger
import traceback
from core import BlingAPI, Database

class FormasPagamentoMigrator:
    """Migrador de Formas de Pagamento da API Bling V3 para MySQL"""

    def __init__(self, api: BlingAPI, db: Database, on_progress=None, pause_event=None):
        self.api = api
        self.db = db
        self.on_progress = on_progress
        self.pause_event = pause_event

    def _report_progress(self, current: int, total: int, message: str):
        """Notifica a interface sobre o progresso da migração."""
        if self.on_progress:
            self.on_progress(current, total, message)

    def _fetch_formas_list(self, page: int) -> list[dict]:
        """
        GET {{baseUrl}}/formas-pagamentos?pagina=1&limite=100
        """
        endpoint = f"/formas-pagamentos?pagina={page}&limite=100"
        response = self.api.request(endpoint)

        if response and "data" in response: 
            return response["data"]
        return []

    def _fetch_forma_details(self, forma_id: int) -> dict | None:
        """
        GET {{baseUrl}}/formas-pagamentos/:idFormaPagamento
        """
        endpoint = f"/formas-pagamentos/{forma_id}"
        response = self.api.request(endpoint)

        if response and "data" in response:
            return response["data"]
        return None

    def _save_forma(self, data: dict):
        """Grava a forma de pagamento no MySQL na tabela `forma_pagamento`."""
        
        forma_id = data.get("id")

        self.db.execute("""
            INSERT INTO forma_pagamento (
                id, descricao, tipoPagamento, situacao, fixa, padrao, finalidade,
                juros, multa, condicao, destino, utilizaDiasUteis,
                taxas_aliquota, taxas_valor, taxas_prazo,
                dadosCartao_bandeira, dadosCartao_tipo, dadosCartao_cnpjCredenciadora, dadosCartao_autoLiquidacao,
                json_completo
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s
            )
            ON DUPLICATE KEY UPDATE
                descricao = VALUES(descricao),
                tipoPagamento = VALUES(tipoPagamento),
                situacao = VALUES(situacao),
                fixa = VALUES(fixa),
                padrao = VALUES(padrao),
                finalidade = VALUES(finalidade),
                juros = VALUES(juros),
                multa = VALUES(multa),
                condicao = VALUES(condicao),
                destino = VALUES(destino),
                utilizaDiasUteis = VALUES(utilizaDiasUteis),
                taxas_aliquota = VALUES(taxas_aliquota),
                taxas_valor = VALUES(taxas_valor),
                taxas_prazo = VALUES(taxas_prazo),
                dadosCartao_bandeira = VALUES(dadosCartao_bandeira),
                dadosCartao_tipo = VALUES(dadosCartao_tipo),
                dadosCartao_cnpjCredenciadora = VALUES(dadosCartao_cnpjCredenciadora),
                dadosCartao_autoLiquidacao = VALUES(dadosCartao_autoLiquidacao),
                json_completo = VALUES(json_completo),
                atualizado_em = CURRENT_TIMESTAMP
        """, (
            forma_id,
            data.get("descricao", ""),
            data.get("tipoPagamento", 0),
            data.get("situacao", 0),
            data.get("fixa", False),
            data.get("padrao", 0),
            data.get("finalidade", 0),
            data.get("juros", 0),
            data.get("multa", 0),
            data.get("condicao", ""),
            data.get("destino", 0),
            data.get("utilizaDiasUteis", False),
            data.get("taxas", {}).get("aliquota", 0) if data.get("taxas") else 0,
            data.get("taxas", {}).get("valor", 0) if data.get("taxas") else 0,
            data.get("taxas", {}).get("prazo", 0) if data.get("taxas") else 0,
            data.get("dadosCartao", {}).get("bandeira", 0) if data.get("dadosCartao") else 0,
            data.get("dadosCartao", {}).get("tipo", 0) if data.get("dadosCartao") else 0,
            data.get("dadosCartao", {}).get("cnpjCredenciadora", "") if data.get("dadosCartao") else "",
            data.get("dadosCartao", {}).get("autoLiquidacao", 0) if data.get("dadosCartao") else 0,
            json.dumps(data, ensure_ascii=False)
        ))

    def execute(self) -> int:
        """Executa migração completa de formas de pagamento."""

        self.db.create_formas_pagamento_tables()

        # FASE 1
        all_ids = []
        page = 1
        self._report_progress(0, 0, "Fase 1: Coletando lista de formas de pagamento...")

        while True:
            if self.pause_event:
                self.pause_event.wait()
                
            self._report_progress(len(all_ids), 0, f"Coletando página {page}...")
            formas = self._fetch_formas_list(page)

            if not formas:
                break

            for f in formas:
                all_ids.append({
                    "id": f["id"], 
                    "descricao": f.get("descricao", "Sem descrição")
                })

            page += 1   
            
        total = len(all_ids)
        self._report_progress(0, total, f"Fase 1 concluída: {total} formas encontradas")

        # FASE 2
        processed = 0

        for i, rec in enumerate(all_ids):
            if self.db.record_exists("forma_pagamento", rec["id"]):
                self._report_progress(i + 1, total, f"Pulando Forma {rec['id']} ({rec['descricao']})")
                continue

            self._report_progress(i + 1, total, f"Importando Forma {rec['id']} ({rec['descricao']}) - {i+1}/{total}...")

            details = self._fetch_forma_details(rec["id"])
            if not details:
                continue

            try:
                self._save_forma(details)
                self.db.commit()
                if self.pause_event:
                    self.pause_event.wait()
                processed += 1
            except Exception as e:
                self.db.rollback()
                tb = traceback.format_exc()
                logger.error(f"Erro ao gravar forma de pagamento {rec['id']}: {e}\n{tb}") 
                self._report_progress(i + 1, total, f"Erro Crítico na Forma {rec['id']}: {e}")
                raise e

        self._report_progress(total, total, f"Migração de Formas de Pagamento concluída! {processed} migrados.")   
        return processed
