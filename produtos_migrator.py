"""
produtos_migrator.py — Migrador de Produtos do Bling para MySQL
"""

import json
from loguru import logger
from core import BlingAPI, Database

class ProdutosMigrator:
    """Migrador de Produtos da API Bling V3 para MySQL"""

    def __init__(self, api: BlingAPI, db: Database, on_progress=None):
        self.api = api
        self.db = db
        self.on_progress = on_progress

    def _report_progress(self, current: int, total: int, message: str):
        """Notifica a interface sobre o progresso da migração."""
        if self.on_progress:
            self.on_progress(current, total, message)

    def _fetch_produto_list(self, page: int) -> list[dict]:
        """
        GET {{baseUrl}}/produtos?pagina=1&limite=100
        Retorna lista de produtos resumidos.
        """
        endpoint = f"/produtos?pagina={page}&limite=100"
        response = self.api.request(endpoint)

        if response and "data" in response: 
            return response["data"]
        return []

    def _fetch_produto_details(self, produto_id: int) -> dict | None:
        """
        GET {{baseUrl}}/produtos/:idProduto
        Retorna o JSON completo do produto.
        """
        endpoint = f"/produtos/{produto_id}"
        response = self.api.request(endpoint)

        if response and "data" in response:
            return response["data"]
        return None

    def _save_produto(self, data: dict, id_produto_pai_fase1: int = None):
        """Grava produto no MySQL (INSERT ON DUPLICATE KEY UPDATE)."""

        val_data_validade = data.get("dataValidade")
        if val_data_validade == "0000-00-00" or not val_data_validade:
            val_data_validade = None

        cat_id = data.get("categoria", {}).get("id")
        forn_id = data.get("fornecedor", {}).get("id")
        
        id_produto_pai = id_produto_pai_fase1
        if not id_produto_pai:
            id_produto_pai = data.get("variacao", {}).get("produtoPai", {}).get("id")

        self.db.execute("""
            INSERT INTO produtos (
                id, idProdutoPai, nome, codigo, preco, precoCusto, tipo, situacao, formato,
                descricaoCurta, dataValidade, unidade, pesoLiquido, pesoBruto,
                volumes, itensPorCaixa, gtin, gtinEmbalagem, tipoProducao,
                condicao, freteGratis, marca, descricaoComplementar, linkExterno,
                observacoes, descricaoEmbalagemDiscreta, categoria_id, fornecedor_id,
                categoria, fornecedor,
                actionEstoque, artigoPerigoso,
                dimensoes, tributacao, midia, linhaProduto, estrutura,
                camposCustomizados, variacoes, variacao, estoque, json_completo
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                idProdutoPai = VALUES(idProdutoPai),
                nome = VALUES(nome),
                codigo = VALUES(codigo),
                preco = VALUES(preco),
                precoCusto = VALUES(precoCusto),
                tipo = VALUES(tipo),
                situacao = VALUES(situacao),
                formato = VALUES(formato),
                descricaoCurta = VALUES(descricaoCurta),
                dataValidade = VALUES(dataValidade),
                unidade = VALUES(unidade),
                pesoLiquido = VALUES(pesoLiquido),
                pesoBruto = VALUES(pesoBruto),
                volumes = VALUES(volumes),
                itensPorCaixa = VALUES(itensPorCaixa),
                gtin = VALUES(gtin),
                gtinEmbalagem = VALUES(gtinEmbalagem),
                tipoProducao = VALUES(tipoProducao),
                condicao = VALUES(condicao),
                freteGratis = VALUES(freteGratis),
                marca = VALUES(marca),
                descricaoComplementar = VALUES(descricaoComplementar),
                linkExterno = VALUES(linkExterno),
                observacoes = VALUES(observacoes),
                descricaoEmbalagemDiscreta = VALUES(descricaoEmbalagemDiscreta),
                categoria_id = VALUES(categoria_id),
                fornecedor_id = VALUES(fornecedor_id),
                categoria = VALUES(categoria),
                fornecedor = VALUES(fornecedor),
                actionEstoque = VALUES(actionEstoque),
                artigoPerigoso = VALUES(artigoPerigoso),
                dimensoes = VALUES(dimensoes),
                tributacao = VALUES(tributacao),
                midia = VALUES(midia),
                linhaProduto = VALUES(linhaProduto),
                estrutura = VALUES(estrutura),
                camposCustomizados = VALUES(camposCustomizados),
                variacoes = VALUES(variacoes),
                variacao = VALUES(variacao),
                estoque = VALUES(estoque),
                json_completo = VALUES(json_completo),
                atualizado_em = CURRENT_TIMESTAMP
        """, (
            data.get("id"),
            id_produto_pai,
            data.get("nome"),
            data.get("codigo"),
            data.get("preco", 0),
            data.get("precoCusto", 0),
            data.get("tipo"),
            data.get("situacao"),
            data.get("formato"),
            data.get("descricaoCurta"),
            val_data_validade,
            data.get("unidade"),
            data.get("pesoLiquido", 0),
            data.get("pesoBruto", 0),
            data.get("volumes", 0),
            data.get("itensPorCaixa", 0),
            data.get("gtin"),
            data.get("gtinEmbalagem"),
            data.get("tipoProducao"),
            data.get("condicao", 0),
            data.get("freteGratis", False),
            data.get("marca"),
            data.get("descricaoComplementar"),
            data.get("linkExterno"),
            data.get("observacoes"),
            data.get("descricaoEmbalagemDiscreta"),
            cat_id if cat_id else None,
            forn_id if forn_id else None,
            json.dumps(data.get("categoria", {}), ensure_ascii=False) if "categoria" in data else None,
            json.dumps(data.get("fornecedor", {}), ensure_ascii=False) if "fornecedor" in data else None,
            data.get("actionEstoque"),
            data.get("artigoPerigoso", False),
            # JSONs
            json.dumps(data.get("dimensoes", {}), ensure_ascii=False) if "dimensoes" in data else None,
            json.dumps(data.get("tributacao", {}), ensure_ascii=False) if "tributacao" in data else None,
            json.dumps(data.get("midia", {}), ensure_ascii=False) if "midia" in data else None,
            json.dumps(data.get("linhaProduto", {}), ensure_ascii=False) if "linhaProduto" in data else None,
            json.dumps(data.get("estrutura", {}), ensure_ascii=False) if "estrutura" in data else None,
            json.dumps(data.get("camposCustomizados", []), ensure_ascii=False) if "camposCustomizados" in data else None,
            json.dumps(data.get("variacoes", []), ensure_ascii=False) if "variacoes" in data else None,
            json.dumps(data.get("variacao", {}), ensure_ascii=False) if "variacao" in data else None,
            json.dumps(data.get("estoque", {}), ensure_ascii=False) if "estoque" in data else None,
            json.dumps(data, ensure_ascii=False)
        ))

    def execute(self) -> int:
        """Executa migração completa: Fase 1 (IDs) + Fase 2 (Detalhes)."""

        # Criar tabela se não existir
        self.db.create_produtos_tables()

        # ========== FASE 1: Coletar todos os IDs ==========
        all_ids = []
        page = 1
        self._report_progress(0, 0, "Fase 1: Coletando lista de produtos...")

        while True:
            self._report_progress(len(all_ids), 0, f"Coletando página {page}...")
            produtos = self._fetch_produto_list(page)

            if not produtos:
                break

            for p in produtos:
                all_ids.append({
                    "id": p["id"], 
                    "nome": p.get("nome", ""),
                    "idProdutoPai": p.get("idProdutoPai")
                })

            page += 1   
            
        total = len(all_ids)
        self._report_progress(0, total, f"Fase 1 concluída: {total} produtos encontrados")

        # ========== FASE 2: Buscar detalhes e gravar ==========
        processed = 0

        for i, rec in enumerate(all_ids):
            # Verifica se já existe no banco (economiza chamadas na API)
            if self.db.record_exists("produtos", rec["id"]):
                # Opcional: Se quiser que ele atualize SEMPRE, comente a linha continue abaixo.
                # Como a migração pode ser interrompida e continuada, é prudente manter assim,
                # ou usar um método force_update para sobrepor isso.
                self._report_progress(i + 1, total,
                    f"Pulando ID já migrado: {rec['id']} ({rec['nome']})")
                continue

            self._report_progress(i + 1, total,
                f"Importando {rec['id']} ({rec['nome']}) - {i+1}/{total}...")

            # Busca detalhes completos via API
            details = self._fetch_produto_details(rec["id"])
            if not details:
                continue

            # Grava no banco com transaction
            try:
                self._save_produto(details, id_produto_pai_fase1=rec.get("idProdutoPai"))
                self.db.commit()
                processed += 1
            except Exception as e:
                self.db.rollback()
                logger.error(f"Erro ao gravar produto {rec['id']}: {e}") 

        self._report_progress(total, total, f"Migração de Produtos concluída! {processed} produtos processados")   
        return processed
