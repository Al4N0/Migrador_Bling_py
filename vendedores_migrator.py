"""
vendedores_migrator.py — Migrador de Vendedores do Bling para MySQL
"""

import json
from loguru import logger
import traceback
from core import BlingAPI, Database

class VendedoresMigrator:
    """Migrador de Vendedores da API Bling V3 para MySQL"""

    def __init__(self, api: BlingAPI, db: Database, on_progress=None, pause_event=None):
        self.api = api
        self.db = db
        self.on_progress = on_progress
        self.pause_event = pause_event

    def _report_progress(self, current: int, total: int, message: str):
        """Notifica a interface sobre o progresso da migração."""
        if self.on_progress:
            self.on_progress(current, total, message)

    def _fetch_vendedores_list(self, page: int) -> list[dict]:
        """
        GET {{baseUrl}}/vendedores?pagina=1&limite=100
        Retorna lista de vendedores. Diferente de vendas, usaremos listagem e paginação padrão.
        """
        endpoint = f"/vendedores?pagina={page}&limite=100"
        response = self.api.request(endpoint)

        if response and "data" in response: 
            return response["data"]
        return []

    def _fetch_vendedor_details(self, vendedor_id: int) -> dict | None:
        """
        GET {{baseUrl}}/vendedores/:idVendedor
        Retorna o JSON completo do vendedor.
        """
        endpoint = f"/vendedores/{vendedor_id}"
        response = self.api.request(endpoint)

        if response and "data" in response:
            return response["data"]
        return None

    def _save_vendedor(self, data: dict):
        """Grava vendedor no MySQL (Apenas uma tabela por exigência)."""
        
        vendedor_id = data.get("id")

        self.db.execute("""
            INSERT INTO vendedor (
                id, descontoLimite, loja_id, contato_id, contato_nome, contato_situacao, comissoes, json_completo
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                descontoLimite = VALUES(descontoLimite),
                loja_id = VALUES(loja_id),
                contato_id = VALUES(contato_id),
                contato_nome = VALUES(contato_nome),
                contato_situacao = VALUES(contato_situacao),
                comissoes = VALUES(comissoes),
                json_completo = VALUES(json_completo),
                atualizado_em = CURRENT_TIMESTAMP
        """, (
            vendedor_id,
            data.get("descontoLimite", 0),
            data.get("loja", {}).get("id") if data.get("loja") else None,
            data.get("contato", {}).get("id") if data.get("contato") else None,
            data.get("contato", {}).get("nome", "") if data.get("contato") else "",
            data.get("contato", {}).get("situacao", "") if data.get("contato") else "",
            json.dumps(data.get("comissoes", []), ensure_ascii=False),
            json.dumps(data, ensure_ascii=False)
        ))

    def execute(self) -> int:
        """Executa migração completa de vendedores."""

        # Criar tabelas se não existirem
        self.db.create_vendedores_tables()

        # ========== FASE 1: Coletar todos os IDs ==========
        all_ids = []
        page = 1
        self._report_progress(0, 0, "Fase 1: Coletando lista de vendedores...")

        while True:
            if self.pause_event:
                self.pause_event.wait()
                
            self._report_progress(len(all_ids), 0, f"Coletando página {page}...")
            vendedores = self._fetch_vendedores_list(page)

            if not vendedores:
                break

            for v in vendedores:
                # O endpoint lista os vendedores.
                # A chave de Contato carrega o nome do Vendedor no Bling.
                contato = v.get("contato", {})
                nome_vendedor = contato.get("nome", "Sem Nome")
                
                all_ids.append({
                    "id": v["id"], 
                    "nome": nome_vendedor
                })

            page += 1   
            
        total = len(all_ids)
        self._report_progress(0, total, f"Fase 1 concluída: {total} vendedores encontrados")

        # Diferente das vendas que queríamos inverter a lista de datas,
        # Vendedores não tem tantas particularidades de data-chave que impeçam o ID Asc.

        # ========== FASE 2: Buscar detalhes e gravar ==========
        processed = 0

        for i, rec in enumerate(all_ids):
            # Verifica se já existe
            if self.db.record_exists("vendedor", rec["id"]):
                self._report_progress(i + 1, total,
                    f"Pulando Vendedor ID {rec['id']} ({rec['nome']})")
                continue

            self._report_progress(i + 1, total,
                f"Importando Vendedor {rec['id']} ({rec['nome']}) - {i+1}/{total}...")

            # Busca detalhes completos via API
            details = self._fetch_vendedor_details(rec["id"])
            if not details:
                continue

            # Grava na única tabela
            try:
                self._save_vendedor(details)
                self.db.commit()
                if self.pause_event:
                    self.pause_event.wait()
                processed += 1
            except Exception as e:
                self.db.rollback()
                tb = traceback.format_exc()
                logger.error(f"Erro ao gravar vendedor {rec['id']}: {e}\n{tb}") 
                self._report_progress(i + 1, total, f"Erro Crítico no Vendedor {rec['id']}: {e}")
                raise e # Propaga o erro pro main.py logar

        self._report_progress(total, total, f"Migração de Vendedores concluída! {processed} migrados.")   
        return processed
