"""
pedidos_venda_migrator.py — Migrador de Pedidos de Venda do Bling para MySQL
"""

import json
from loguru import logger
from core import BlingAPI, Database

class PedidosVendaMigrator:
    """Migrador de Pedidos de Venda da API Bling V3 para MySQL"""

    def __init__(self, api: BlingAPI, db: Database, on_progress=None, pause_event=None):
        self.api = api
        self.db = db
        self.on_progress = on_progress
        self.pause_event = pause_event

    def _report_progress(self, current: int, total: int, message: str):
        """Notifica a interface sobre o progresso da migração."""
        if self.on_progress:
            self.on_progress(current, total, message)

    def _fetch_pedidos_list(self, page: int) -> list[dict]:
        """
        GET {{baseUrl}}/pedidos/vendas?pagina=1&limite=100&direcao=A
        Retorna lista de pedidos resumidos do mais antigo para o mais novo.
        """
        endpoint = f"/pedidos/vendas?pagina={page}&limite=100&direcao=A"
        response = self.api.request(endpoint)

        if response and "data" in response: 
            return response["data"]
        return []

    def _fetch_pedido_details(self, pedido_id: int) -> dict | None:
        """
        GET {{baseUrl}}/pedidos/vendas/:idPedido
        Retorna o JSON completo do pedido.
        """
        endpoint = f"/pedidos/vendas/{pedido_id}"
        response = self.api.request(endpoint)

        if response and "data" in response:
            return response["data"]
        return None

    def _save_pedido(self, data: dict):
        """Grava pedido, itens e parcelas no MySQL, substituindo detalhes antigos."""
        
        pedido_id = data.get("id")
        
        # Helper: conversor de data vazia
        def parse_date(d: str):
            if d == "0000-00-00" or not d:
                return None
            return d

        data_pedido = parse_date(data.get("data"))
        data_saida = parse_date(data.get("dataSaida"))
        data_prevista = parse_date(data.get("dataPrevista"))

        # 1. Gravar Cabeçalho (Tabela pedido)
        self.db.execute("""
            INSERT INTO pedido (
                id, numero, numeroLoja, data, dataSaida, dataPrevista, 
                totalProdutos, total, 
                contato_id, contato_nome, contato_tipoPessoa, contato_numeroDocumento, 
                situacao_id, situacao_valor, 
                loja_id, loja_unidadeNegocio_id, 
                numeroPedidoCompra, outrasDespesas, observacoes, observacoesInternas, 
                desconto_valor, desconto_unidade, 
                categoria_id, notaFiscal_id, 
                tributacao_totalICMS, tributacao_totalIPI, 
                transporte, vendedor_id, intermediador, taxas, 
                json_completo
            ) VALUES (
                %s, %s, %s, %s, %s, %s, 
                %s, %s, 
                %s, %s, %s, %s, 
                %s, %s, 
                %s, %s, 
                %s, %s, %s, %s, 
                %s, %s, 
                %s, %s, 
                %s, %s, 
                %s, %s, %s, %s, 
                %s
            )
            ON DUPLICATE KEY UPDATE
                numero = VALUES(numero),
                numeroLoja = VALUES(numeroLoja),
                data = VALUES(data),
                dataSaida = VALUES(dataSaida),
                dataPrevista = VALUES(dataPrevista),
                totalProdutos = VALUES(totalProdutos),
                total = VALUES(total),
                contato_id = VALUES(contato_id),
                contato_nome = VALUES(contato_nome),
                contato_tipoPessoa = VALUES(contato_tipoPessoa),
                contato_numeroDocumento = VALUES(contato_numeroDocumento),
                situacao_id = VALUES(situacao_id),
                situacao_valor = VALUES(situacao_valor),
                loja_id = VALUES(loja_id),
                loja_unidadeNegocio_id = VALUES(loja_unidadeNegocio_id),
                numeroPedidoCompra = VALUES(numeroPedidoCompra),
                outrasDespesas = VALUES(outrasDespesas),
                observacoes = VALUES(observacoes),
                observacoesInternas = VALUES(observacoesInternas),
                desconto_valor = VALUES(desconto_valor),
                desconto_unidade = VALUES(desconto_unidade),
                categoria_id = VALUES(categoria_id),
                notaFiscal_id = VALUES(notaFiscal_id),
                tributacao_totalICMS = VALUES(tributacao_totalICMS),
                tributacao_totalIPI = VALUES(tributacao_totalIPI),
                transporte = VALUES(transporte),
                vendedor_id = VALUES(vendedor_id),
                intermediador = VALUES(intermediador),
                taxas = VALUES(taxas),
                json_completo = VALUES(json_completo),
                atualizado_em = CURRENT_TIMESTAMP
        """, (
            pedido_id,
            data.get("numero"),
            data.get("numeroLoja", ""),
            data_pedido,
            data_saida,
            data_prevista,
            data.get("totalProdutos", 0),
            data.get("total", 0),
            data.get("contato", {}).get("id") if data.get("contato") else None,
            data.get("contato", {}).get("nome", "") if data.get("contato") else "",
            data.get("contato", {}).get("tipoPessoa", "") if data.get("contato") else "",
            data.get("contato", {}).get("numeroDocumento", "") if data.get("contato") else "",
            data.get("situacao", {}).get("id") if data.get("situacao") else None,
            data.get("situacao", {}).get("valor", 0) if data.get("situacao") else 0,
            data.get("loja", {}).get("id") if data.get("loja") else None,
            data.get("loja", {}).get("unidadeNegocio", {}).get("id") if data.get("loja") and "unidadeNegocio" in data["loja"] else None,
            data.get("numeroPedidoCompra", ""),
            data.get("outrasDespesas", 0),
            data.get("observacoes", ""),
            data.get("observacoesInternas", ""),
            data.get("desconto", {}).get("valor", 0) if data.get("desconto") else 0,
            data.get("desconto", {}).get("unidade", "") if data.get("desconto") else "",
            data.get("categoria", {}).get("id") if data.get("categoria") else None,
            data.get("notaFiscal", {}).get("id") if data.get("notaFiscal") else None,
            data.get("tributacao", {}).get("totalICMS", 0) if data.get("tributacao") else 0,
            data.get("tributacao", {}).get("totalIPI", 0) if data.get("tributacao") else 0,
            json.dumps(data.get("transporte", {}), ensure_ascii=False),
            data.get("vendedor", {}).get("id") if data.get("vendedor") else None,
            json.dumps(data.get("intermediador", {}), ensure_ascii=False),
            json.dumps(data.get("taxas", {}), ensure_ascii=False),
            json.dumps(data, ensure_ascii=False)
        ))

        # 2. Gravar Itens (Tabela pedido_item) - Sempre remove os antigos
        self.db.execute("DELETE FROM pedido_item WHERE pedido_id = %s", (pedido_id,))
        itens = data.get("itens", [])
        for item in itens:
            self.db.execute("""
                INSERT INTO pedido_item (
                    id, pedido_id, codigo, unidade, quantidade, desconto, valor, aliquotaIPI, 
                    descricao, descricaoDetalhada, produto_id, 
                    comissao_base, comissao_aliquota, comissao_valor, naturezaOperacao_id, json_completo
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, 
                    %s, %s, %s, %s, %s
                )
            """, (
                item.get("id"),
                pedido_id,
                item.get("codigo", ""),
                item.get("unidade", ""),
                item.get("quantidade", 0),
                item.get("desconto", 0),
                item.get("valor", 0),
                item.get("aliquotaIPI", 0),
                item.get("descricao", ""),
                item.get("descricaoDetalhada", ""),
                item.get("produto", {}).get("id") if item.get("produto") else None,
                item.get("comissao", {}).get("base", 0) if item.get("comissao") else 0,
                item.get("comissao", {}).get("aliquota", 0) if item.get("comissao") else 0,
                item.get("comissao", {}).get("valor", 0) if item.get("comissao") else 0,
                item.get("naturezaOperacao", {}).get("id") if item.get("naturezaOperacao") else None,
                json.dumps(item, ensure_ascii=False)
            ))

        # 3. Gravar Parcelas (Tabela pedido_parcela) - Sempre remove os antigos
        self.db.execute("DELETE FROM pedido_parcela WHERE pedido_id = %s", (pedido_id,))
        parcelas = data.get("parcelas", [])
        for parcela in parcelas:
            self.db.execute("""
                INSERT INTO pedido_parcela (
                    id, pedido_id, dataVencimento, valor, observacoes, caut, formaPagamento_id, json_completo
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
            """, (
                parcela.get("id"),
                pedido_id,
                parse_date(parcela.get("dataVencimento")),
                parcela.get("valor", 0),
                parcela.get("observacoes", ""),
                parcela.get("caut", ""),
                parcela.get("formaPagamento", {}).get("id") if parcela.get("formaPagamento") else None,
                json.dumps(parcela, ensure_ascii=False)
            ))


    def execute(self) -> int:
        """Executa migração completa de pedidos."""

        # Criar tabelas se não existirem
        self.db.create_pedidos_venda_tables()

        # ========== FASE 1: Coletar todos os IDs ==========
        all_ids = []
        page = 1
        self._report_progress(0, 0, "Fase 1: Coletando lista de pedidos de venda...")

        while True:
            if self.pause_event:
                self.pause_event.wait()
                
            self._report_progress(len(all_ids), 0, f"Coletando página {page}...")
            pedidos = self._fetch_pedidos_list(page)

            if not pedidos:
                break

            for p in pedidos:
                all_ids.append({
                    "id": p["id"], 
                    "numero": p.get("numero", "")
                })

            page += 1   
            
        total = len(all_ids)
        self._report_progress(0, total, f"Fase 1 concluída: {total} pedidos encontrados")

        # Inverte a lista para garantir que os mais antigos sejam processados primeiro
        all_ids.reverse()

        # ========== FASE 2: Buscar detalhes e gravar ==========
        processed = 0

        for i, rec in enumerate(all_ids):
            # Verifica se já existe o cabeçalho no banco
            if self.db.record_exists("pedido", rec["id"]):
                self._report_progress(i + 1, total,
                    f"Pulando ID já migrado: {rec['id']} (Nº {rec['numero']})")
                continue

            self._report_progress(i + 1, total,
                f"Importando {rec['id']} (Nº {rec['numero']}) - {i+1}/{total}...")

            # Busca detalhes completos via API
            details = self._fetch_pedido_details(rec["id"])
            if not details:
                continue

            # Grava cabeçalho, itens e parcelas com transaction
            try:
                self._save_pedido(details)
                self.db.commit()
                if self.pause_event:
                    self.pause_event.wait()
                processed += 1
            except Exception as e:
                self.db.rollback()
                logger.error(f"Erro ao gravar pedido {rec['id']}: {e}") 
                self._report_progress(i + 1, total, f"Erro Crítico no pedido {rec['id']}: {e}")
                raise e # Propaga o erro para ser capturado no main.py

        self._report_progress(total, total, f"Migração de Pedidos de Venda concluída! {processed} pedidos processados")   
        return processed
