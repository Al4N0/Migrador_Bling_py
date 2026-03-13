"""
notas_fiscais_migrator.py — Migrador de Notas Fiscais do Bling para MySQL
"""

import json
from loguru import logger
import traceback
from core import BlingAPI, Database

class NotasFiscaisMigrator:
    """Migrador de Notas Fiscais da API Bling V3 para MySQL, separando cabeçalho, itens e parcelas."""

    def __init__(self, api: BlingAPI, db: Database, on_progress=None, pause_event=None):
        self.api = api
        self.db = db
        self.on_progress = on_progress
        self.pause_event = pause_event

    def _report_progress(self, current: int, total: int, message: str):
        if self.on_progress:
            self.on_progress(current, total, message)

    def _fetch_nf_list(self, page: int) -> list[dict]:
        """GET {{baseUrl}}/nfe?pagina=1&limite=100"""
        endpoint = f"/nfe?pagina={page}&limite=100"
        response = self.api.request(endpoint)

        if response and "data" in response:
            return response["data"]
        return []

    def _fetch_nf_details(self, nf_id: int) -> dict | None:
        """GET {{baseUrl}}/nfe/:idNotaFiscal"""
        endpoint = f"/nfe/{nf_id}"
        response = self.api.request(endpoint)

        if response and "data" in response:
            return response["data"]
        return None

    def _safe_date(self, date_str: str) -> str | None:
        if not date_str or date_str.startswith("0000-00-00"):
            return None
        return date_str

    def _save_nf(self, nf: dict):
        """Salva a Nota Fiscal (Cabeçalho), exclui dependentes se existirem, e salva Itens e Parcelas."""
        
        nf_id = nf.get("id")
        data_emissao = self._safe_date(nf.get("dataEmissao"))
        data_operacao = self._safe_date(nf.get("dataOperacao"))

        # CABEÇALHO
        query_header = """
            INSERT INTO nota_fiscal (
                id, tipo, situacao, numero, dataEmissao, dataOperacao, chaveAcesso,
                contato_id, contato_nome, contato_numeroDocumento, contato_ie, contato_rg, contato_telefone, contato_email,
                contato_endereco_endereco, contato_endereco_numero, contato_endereco_complemento, contato_endereco_bairro, contato_endereco_cep, contato_endereco_municipio, contato_endereco_uf,
                naturezaOperacao_id, loja_id, serie, valorNota, valorFrete, xml, linkDanfe, linkPDF,
                optanteSimplesNacional, numeroPedidoLoja, vendedor_id,
                transporte_fretePorConta, transporte_transportador_nome, transporte_transportador_numeroDocumento, transporte_volumes,
                transporte_etiqueta_nome, transporte_etiqueta_endereco, transporte_etiqueta_numero, transporte_etiqueta_complemento, transporte_etiqueta_municipio, transporte_etiqueta_uf, transporte_etiqueta_cep, transporte_etiqueta_bairro,
                json_completo
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s
            )
            ON DUPLICATE KEY UPDATE
                tipo = VALUES(tipo),
                situacao = VALUES(situacao),
                numero = VALUES(numero),
                dataEmissao = VALUES(dataEmissao),
                dataOperacao = VALUES(dataOperacao),
                chaveAcesso = VALUES(chaveAcesso),
                contato_id = VALUES(contato_id),
                contato_nome = VALUES(contato_nome),
                contato_numeroDocumento = VALUES(contato_numeroDocumento),
                contato_ie = VALUES(contato_ie),
                contato_rg = VALUES(contato_rg),
                contato_telefone = VALUES(contato_telefone),
                contato_email = VALUES(contato_email),
                contato_endereco_endereco = VALUES(contato_endereco_endereco),
                contato_endereco_numero = VALUES(contato_endereco_numero),
                contato_endereco_complemento = VALUES(contato_endereco_complemento),
                contato_endereco_bairro = VALUES(contato_endereco_bairro),
                contato_endereco_cep = VALUES(contato_endereco_cep),
                contato_endereco_municipio = VALUES(contato_endereco_municipio),
                contato_endereco_uf = VALUES(contato_endereco_uf),
                naturezaOperacao_id = VALUES(naturezaOperacao_id),
                loja_id = VALUES(loja_id),
                serie = VALUES(serie),
                valorNota = VALUES(valorNota),
                valorFrete = VALUES(valorFrete),
                xml = VALUES(xml),
                linkDanfe = VALUES(linkDanfe),
                linkPDF = VALUES(linkPDF),
                optanteSimplesNacional = VALUES(optanteSimplesNacional),
                numeroPedidoLoja = VALUES(numeroPedidoLoja),
                vendedor_id = VALUES(vendedor_id),
                transporte_fretePorConta = VALUES(transporte_fretePorConta),
                transporte_transportador_nome = VALUES(transporte_transportador_nome),
                transporte_transportador_numeroDocumento = VALUES(transporte_transportador_numeroDocumento),
                transporte_volumes = VALUES(transporte_volumes),
                transporte_etiqueta_nome = VALUES(transporte_etiqueta_nome),
                transporte_etiqueta_endereco = VALUES(transporte_etiqueta_endereco),
                transporte_etiqueta_numero = VALUES(transporte_etiqueta_numero),
                transporte_etiqueta_complemento = VALUES(transporte_etiqueta_complemento),
                transporte_etiqueta_municipio = VALUES(transporte_etiqueta_municipio),
                transporte_etiqueta_uf = VALUES(transporte_etiqueta_uf),
                transporte_etiqueta_cep = VALUES(transporte_etiqueta_cep),
                transporte_etiqueta_bairro = VALUES(transporte_etiqueta_bairro),
                json_completo = VALUES(json_completo),
                atualizado_em = CURRENT_TIMESTAMP
        """

        contato = nf.get("contato", {})
        endereco = contato.get("endereco", {})
        transporte = nf.get("transporte", {})
        transportador = transporte.get("transportador", {})
        etiqueta = transporte.get("etiqueta", {})

        params_header = (
            nf_id, nf.get("tipo", 0), nf.get("situacao", 0), nf.get("numero", ""),
            data_emissao, data_operacao, nf.get("chaveAcesso", ""),
            contato.get("id"), contato.get("nome", ""), contato.get("numeroDocumento", ""),
            contato.get("ie", ""), contato.get("rg", ""), contato.get("telefone", ""), contato.get("email", ""),
            endereco.get("endereco", ""), endereco.get("numero", ""), endereco.get("complemento", ""),
            endereco.get("bairro", ""), endereco.get("cep", ""), endereco.get("municipio", ""), endereco.get("uf", ""),
            nf.get("naturezaOperacao", {}).get("id") if nf.get("naturezaOperacao") else None,
            nf.get("loja", {}).get("id") if nf.get("loja") else None,
            nf.get("serie", 0), nf.get("valorNota", 0), nf.get("valorFrete", 0),
            nf.get("xml", ""), nf.get("linkDanfe", ""), nf.get("linkPDF", ""),
            nf.get("optanteSimplesNacional", False), nf.get("numeroPedidoLoja", ""),
            nf.get("vendedor", {}).get("id") if nf.get("vendedor") else None,
            transporte.get("fretePorConta", 0),
            transportador.get("nome", ""), transportador.get("numeroDocumento", ""),
            json.dumps(transporte.get("volumes", []), ensure_ascii=False),
            etiqueta.get("nome", ""), etiqueta.get("endereco", ""), etiqueta.get("numero", ""),
            etiqueta.get("complemento", ""), etiqueta.get("municipio", ""), etiqueta.get("uf", ""),
            etiqueta.get("cep", ""), etiqueta.get("bairro", ""),
            json.dumps(nf, ensure_ascii=False)
        )

        self.db.execute(query_header, params_header)

        # Deletar itens/parcelas antigos (ON DELETE CASCADE é bom mas faremos force purge manual)
        self.db.execute("DELETE FROM nota_fiscal_item WHERE nota_fiscal_id = %s", (nf_id,))
        self.db.execute("DELETE FROM nota_fiscal_parcela WHERE nota_fiscal_id = %s", (nf_id,))

        # ITENS
        itens = nf.get("itens", [])
        if itens:
            query_item = """
                INSERT INTO nota_fiscal_item (
                    nota_fiscal_id, codigo, descricao, unidade, quantidade, valor, valorTotal,
                    tipo, pesoBruto, pesoLiquido, numeroPedidoCompra, classificacaoFiscal,
                    cest, codigoServico, origem, informacoesAdicionais, gtin, cfop,
                    impostos_valorAproximadoTotalTributos, impostos_icms_st, impostos_icms_origem,
                    impostos_icms_modalidade, impostos_icms_aliquota, impostos_icms_valor
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s
                )
            """
            for i in itens:
                impostos = i.get("impostos", {})
                icms = impostos.get("icms", {})
                
                params_item = (
                    nf_id, i.get("codigo", ""), i.get("descricao", ""), i.get("unidade", ""),
                    i.get("quantidade", 0), i.get("valor", 0), i.get("valorTotal", 0),
                    i.get("tipo", ""), i.get("pesoBruto", 0), i.get("pesoLiquido", 0),
                    i.get("numeroPedidoCompra", ""), i.get("classificacaoFiscal", ""),
                    i.get("cest", ""), i.get("codigoServico", ""), i.get("origem", 0),
                    i.get("informacoesAdicionais", ""), i.get("gtin", ""), i.get("cfop", ""),
                    impostos.get("valorAproximadoTotalTributos", 0),
                    icms.get("st", 0), icms.get("origem", 0), icms.get("modalidade", 0),
                    icms.get("aliquota", 0), icms.get("valor", 0)
                )
                self.db.execute(query_item, params_item)

        # PARCELAS
        parcelas = nf.get("parcelas", [])
        if parcelas:
            query_parcela = """
                INSERT INTO nota_fiscal_parcela (
                    nota_fiscal_id, data, valor, observacoes, caut, formaPagamento_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s
                )
            """
            for p in parcelas:
                dt_vencimento = self._safe_date(p.get("data"))
                params_parcela = (
                    nf_id, dt_vencimento, p.get("valor", 0), p.get("observacoes", ""),
                    p.get("caut", ""), p.get("formaPagamento", {}).get("id") if p.get("formaPagamento") else None
                )
                self.db.execute(query_parcela, params_parcela)

    def execute(self) -> int:
        """Executa migração completa de notas fiscais."""

        self.db.create_notas_fiscais_tables()

        # FASE 1: Listar todas as NF
        all_ids = []
        page = 1
        self._report_progress(0, 0, "Fase 1: Coletando lista de notas fiscais...")

        while True:
            if self.pause_event:
                self.pause_event.wait()
                
            self._report_progress(len(all_ids), 0, f"Coletando página {page}...")
            nfs = self._fetch_nf_list(page)

            if not nfs:
                break

            for n in nfs:
                all_ids.append({
                    "id": n["id"], 
                    "numero": n.get("numero", "S/N")
                })

            page += 1   
            
        total = len(all_ids)
        self._report_progress(0, total, f"Fase 1 concluída: {total} notas fiscais encontradas")

        # Inverte pois queremos as antigas primeiro
        all_ids.reverse()

        # FASE 2: Buscar detalhes
        processed = 0

        for i, rec in enumerate(all_ids):
            if self.db.record_exists("nota_fiscal", rec["id"]):
                self._report_progress(i + 1, total, f"Pulando NF {rec['id']} (Nº {rec['numero']})")
                continue

            self._report_progress(i + 1, total, f"Importando NF {rec['id']} (Nº {rec['numero']}) - {i+1}/{total}...")

            details = self._fetch_nf_details(rec["id"])
            if not details:
                continue

            try:
                self._save_nf(details)
                self.db.commit()
                if self.pause_event:
                    self.pause_event.wait()
                processed += 1
            except Exception as e:
                self.db.rollback()
                tb = traceback.format_exc()
                logger.error(f"Erro ao gravar nota fiscal {rec['id']}: {e}\n{tb}") 
                self._report_progress(i + 1, total, f"Erro Crítico na NF {rec['id']}: {e}")
                raise e

        self._report_progress(total, total, f"Migração de Notas Fiscais concluída! {processed} migrados.")   
        return processed
