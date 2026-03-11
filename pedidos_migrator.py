"""
pedidos_migrator.py — Migrador de Pedidos de Venda do Bling para MySQL
"""

import json
from loguru import logger
from core import BlingAPI, Database

class PedidosMigrator:
    """Migrador de Pedidos de Venda da API Bling V3 para MySQL"""

    def __init__(self, api: BlingAPI, db: Database, on_progress=None):
        self.api = api
        self.db = db
        self.on_progress = on_progress

    def _report_progress(self, current: int, total: int, message: str):
        """Notifica a interface sobre o progresso da migração."""
        if self.on_progress:
            self.on_progress(current, total, message)

    def _fetch_pedido_list(self, page: int) -> list[dict]:
        """
        GET {{baseUrl}}/pedidos/vendas?pagina=1&limite=100
        Retorna lista de pedidos de venda resumidos.
        """
        endpoint = f"/pedidos/vendas?pagina={page}&limite=100"
        response = self.api.request(endpoint)

        if response and "data" in response: 
            return response["data"]
        return []

    def _fetch_pedido_details(self, pedido_id: int) -> dict | None:
        """
        GET {{baseUrl}}/pedidos/vendas/:idPedido
        Retorna o JSON completo do pedido de venda.
        """
        endpoint = f"/pedidos/vendas/{pedido_id}"
        response = self.api.request(endpoint)

        if response and "data" in response:
            return response["data"]
        return None

    def _save_pedido(self, data: dict):
        """Grava pedido no MySQL (INSERT ON DUPLICATE KEY UPDATE)."""

        def parse_date(date_str):
            return date_str if date_str and date_str != "0000-00-00" else None

        val_data = parse_date(data.get("data"))
        val_data_saida = parse_date(data.get("dataSaida"))
        val_data_prevista = parse_date(data.get("dataPrevista"))

        contato = data.get("contato", {})
        situacao = data.get("situacao", {})
        loja = data.get("loja", {})

        self.db.execute("""
            INSERT INTO pedidos (
                id, numero, numeroLoja, data, dataSaida, dataPrevista, 
                totalProdutos, total, contato_id, contato_nome, contato_tipoPessoa, contato_numeroDocumento,
                situacao_id, situacao_valor, loja_id, unidadeNegocio_id,
                numeroPedidoCompra, outrasDespesas, observacoes, observacoesInternas,
                categoria_id, notaFiscal_id, vendedor_id,
                desconto, tributacao, parcelas, transporte, intermediador, taxas,
                json_completo
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
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
                unidadeNegocio_id = VALUES(unidadeNegocio_id),
                numeroPedidoCompra = VALUES(numeroPedidoCompra),
                outrasDespesas = VALUES(outrasDespesas),
                observacoes = VALUES(observacoes),
                observacoesInternas = VALUES(observacoesInternas),
                categoria_id = VALUES(categoria_id),
                notaFiscal_id = VALUES(notaFiscal_id),
                vendedor_id = VALUES(vendedor_id),
                desconto = VALUES(desconto),
                tributacao = VALUES(tributacao),
                parcelas = VALUES(parcelas),
                transporte = VALUES(transporte),
                intermediador = VALUES(intermediador),
                taxas = VALUES(taxas),
                json_completo = VALUES(json_completo),
                atualizado_em = CURRENT_TIMESTAMP
        """, (
            data.get("id"),
            data.get("numero"),
            data.get("numeroLoja"),
            val_data,
            val_data_saida,
            val_data_prevista,
            data.get("totalProdutos", 0),
            data.get("total", 0),
            
            contato.get("id"),
            contato.get("nome"),
            contato.get("tipoPessoa"),
            contato.get("numeroDocumento"),
            
            situacao.get("id"),
            situacao.get("valor"),
            
            loja.get("id"),
            loja.get("unidadeNegocio", {}).get("id"),
            
            data.get("numeroPedidoCompra"),
            data.get("outrasDespesas", 0),
            data.get("observacoes"),
            data.get("observacoesInternas"),
            
            data.get("categoria", {}).get("id"),
            data.get("notaFiscal", {}).get("id"),
            data.get("vendedor", {}).get("id"),
            
            # JSONs
            json.dumps(data.get("desconto", {}), ensure_ascii=False) if "desconto" in data else None,
            json.dumps(data.get("tributacao", {}), ensure_ascii=False) if "tributacao" in data else None,
            json.dumps(data.get("parcelas", []), ensure_ascii=False) if "parcelas" in data else None,
            json.dumps(data.get("transporte", {}), ensure_ascii=False) if "transporte" in data else None,
            json.dumps(data.get("intermediador", {}), ensure_ascii=False) if "intermediador" in data else None,
            json.dumps(data.get("taxas", {}), ensure_ascii=False) if "taxas" in data else None,
            
            json.dumps(data, ensure_ascii=False)
        ))

        # ========== Gravar Itens do Pedido ==========
        # Deletar itens antigos caso seja uma atualização (evita itens órfãos se a venda for alterada)
        pedido_id = data.get("id")
        self.db.execute("DELETE FROM pedidos_itens WHERE pedido_id = %s", (pedido_id,))

        itens = data.get("itens", [])
        for item in itens:
            comissao = item.get("comissao", {})
            self.db.execute("""
                INSERT INTO pedidos_itens (
                    id, pedido_id, codigo, unidade, quantidade, desconto, valor, 
                    aliquotaIPI, descricao, descricaoDetalhada, produto_id,
                    comissao_base, comissao_aliquota, comissao_valor, naturezaOperacao_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
            """, (
                item.get("id"),
                pedido_id,
                item.get("codigo"),
                item.get("unidade"),
                item.get("quantidade", 0),
                item.get("desconto", 0),
                item.get("valor", 0),
                item.get("aliquotaIPI", 0),
                item.get("descricao"),
                item.get("descricaoDetalhada"),
                item.get("produto", {}).get("id"),
                comissao.get("base", 0),
                comissao.get("aliquota", 0),
                comissao.get("valor", 0),
                item.get("naturezaOperacao", {}).get("id")
            ))

    def execute(self) -> int:
        """Executa migração completa: Fase 1 (IDs) + Fase 2 (Detalhes)."""

        # Criar tabela se não existir
        self.db.create_pedidos_tables()

        # ========== FASE 1: Coletar todos os IDs ==========
        all_ids = []
        page = 1
        self._report_progress(0, 0, "Fase 1: Coletando lista de pedidos...")

        while True:
            self._report_progress(len(all_ids), 0, f"Coletando página {page}...")
            pedidos = self._fetch_pedido_list(page)

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

        # ========== FASE 2: Buscar detalhes e gravar ==========
        processed = 0

        for i, rec in enumerate(all_ids):
            # Verifica se já existe no banco (economiza chamadas na API)
            if self.db.record_exists("pedidos", rec["id"]):
                self._report_progress(i + 1, total,
                    f"Pulando ID já migrado: {rec['id']} (Nº {rec['numero']})")
                continue

            self._report_progress(i + 1, total,
                f"Importando {rec['id']} (Nº {rec['numero']}) - {i+1}/{total}...")

            # Busca detalhes completos via API
            details = self._fetch_pedido_details(rec["id"])
            if not details:
                continue

            # Grava no banco com transaction
            try:
                self._save_pedido(details)
                self.db.commit()
                processed += 1
            except Exception as e:
                self.db.rollback()
                logger.error(f"Erro ao gravar pedido {rec['id']}: {e}") 

        self._report_progress(total, total, f"Migração de Pedidos concluída! {processed} pedidos processados")   
        return processed
