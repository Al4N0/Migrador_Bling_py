"""
core.py — Infraestrutura do Migrador Bling → MySQL
Contém: BlingAPI (cliente HTTP) e Database (conexão MySQL)
"""

import os
import json
import time
import base64
from pathlib import Path

import requests
from dotenv import load_dotenv, set_key
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_result
import mysql.connector
from mysql.connector import pooling


# Carrega variáveis do .env
ENV_PATH = Path(__file__).parent / ".env"
load_dotenv(ENV_PATH)


# ============================================================================
# BlingAPI — Cliente HTTP para Bling API v3
# ============================================================================

class BlingAPI:
    """
    Cliente HTTP para a API Bling v3.
    Gerencia autenticação OAuth 2.0, throttling e retry automático (429).
    """

    BASE_URL = "https://api.bling.com.br/Api/v3"
    THROTTLE_DELAY = 0.35  # 350ms entre requisições (limite: 3 req/s)

    def __init__(self):
        self.client_id = os.getenv("BLING_CLIENT_ID", "")
        self.client_secret = os.getenv("BLING_CLIENT_SECRET", "")
        self.access_token = os.getenv("BLING_ACCESS_TOKEN", "")
        self.refresh_token_value = os.getenv("BLING_REFRESH_TOKEN", "")

        if not self.client_id or not self.client_secret:
            raise ValueError("BLING_CLIENT_ID e BLING_CLIENT_SECRET são obrigatórios no .env")
        if not self.refresh_token_value:
            raise ValueError("BLING_REFRESH_TOKEN é obrigatório no .env")

        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
        })

    def _update_auth_header(self):
        """Atualiza o header Authorization com o token atual."""
        self.session.headers["Authorization"] = f"Bearer {self.access_token}"

    @retry(
        stop=stop_after_attempt(8),
        wait=wait_exponential(multiplier=1.5, min=1.5, max=12),
        retry=retry_if_result(lambda r: r is not None and r.status_code == 429),
        before_sleep=lambda retry_state: logger.warning(
            f"Rate limit (429). Tentativa {retry_state.attempt_number}/8. "
            f"Aguardando {retry_state.next_action.sleep:.1f}s..."
        ),
    )
    def _do_request(self, endpoint: str) -> requests.Response:
        """Executa GET com retry automático para 429."""
        self._update_auth_header()
        url = f"{self.BASE_URL}{endpoint}"
        response = self.session.get(url, timeout=120)
        return response

    def request(self, endpoint: str) -> dict | None:
        """
        Faz requisição GET autenticada à API Bling.
        Retorna o JSON parseado ou None se 404.
        Aplica throttling de 350ms entre chamadas.
        """
        response = self._do_request(endpoint)

        if response.status_code in (200, 201):
            # Throttling: respeita limite de 3 req/s
            time.sleep(self.THROTTLE_DELAY)
            if response.text:
                return response.json()
            return None

        elif response.status_code == 404:
            logger.debug(f"Recurso não encontrado: {endpoint}")
            time.sleep(self.THROTTLE_DELAY)
            return None

        elif response.status_code == 401:
            # Token expirado — tenta renovar automaticamente
            logger.warning("Token expirado (401). Tentando renovar...")
            try:
                self.do_refresh_token()
                # Tenta a requisição novamente com o novo token
                response = self._do_request(endpoint)
                if response.status_code in (200, 201):
                    time.sleep(self.THROTTLE_DELAY)
                    if response.text:
                        return response.json()
                    return None
            except Exception as e:
                logger.error(f"Falha ao renovar token: {e}")

            raise PermissionError(
                "Token de acesso inválido (HTTP 401). "
                "Não foi possível renovar automaticamente. "
                "Verifique BLING_REFRESH_TOKEN no .env."
            )

        else:
            raise ConnectionError(
                f"Erro na API Bling: HTTP {response.status_code}\n"
                f"Resposta: {response.text}"
            )

    def do_refresh_token(self):
        """
        Renova o access_token usando o refresh_token via OAuth 2.0.
        Salva os novos tokens no .env automaticamente.
        """
        logger.info("Renovando Access Token via refresh_token...")

        # Basic Auth: Base64(client_id:client_secret)
        credentials = f"{self.client_id}:{self.client_secret}"
        basic_auth = base64.b64encode(credentials.encode()).decode()

        response = requests.post(
            "https://api.bling.com.br/Api/v3/oauth/token",
            headers={
                "Authorization": f"Basic {basic_auth}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token_value,
            },
            timeout=30,
        )

        if response.status_code != 200:
            raise ConnectionError(
                f"Erro ao renovar token: HTTP {response.status_code}\n"
                f"Detalhe: {response.text}"
            )

        data = response.json()
        self.access_token = data["access_token"]
        self.refresh_token_value = data.get("refresh_token", self.refresh_token_value)

        # Salva novos tokens no .env
        env_path = str(ENV_PATH)
        set_key(env_path, "BLING_ACCESS_TOKEN", self.access_token)
        set_key(env_path, "BLING_REFRESH_TOKEN", self.refresh_token_value)

        logger.success("Access Token renovado e salvo no .env")


# ============================================================================
# Database — Conexão MySQL via mysql-connector-python
# ============================================================================

class Database:
    """
    Gerenciador de conexão MySQL.
    Cria o banco automaticamente se não existir.
    """

    def __init__(self, database_name: str):
        self.host = os.getenv("MYSQL_HOST", "localhost")
        self.user = os.getenv("MYSQL_USER", "root")
        self.password = os.getenv("MYSQL_PASSWORD", "")
        self.database = database_name
        self.connection = None

    def connect(self):
        """Conecta ao MySQL. Cria o banco se não existir."""
        # Primeiro, tenta conectar sem especificar o banco para criá-lo
        try:
            temp_conn = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                charset="utf8mb4",
                collation="utf8mb4_unicode_ci",
            )
            cursor = temp_conn.cursor()
            cursor.execute(
                f"CREATE DATABASE IF NOT EXISTS `{self.database}` "
                f"DEFAULT CHARACTER SET utf8mb4 DEFAULT COLLATE utf8mb4_unicode_ci"
            )
            cursor.close()
            temp_conn.close()
            logger.info(f"Banco '{self.database}' verificado/criado")
        except mysql.connector.Error as e:
            raise ConnectionError(f"Erro ao criar banco: {e}")

        # Agora conecta ao banco especificado
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                charset="utf8mb4",
                collation="utf8mb4_unicode_ci",
                autocommit=False,
            )
            logger.success(f"Conectado ao MySQL → {self.database}")
        except mysql.connector.Error as e:
            raise ConnectionError(f"Erro ao conectar ao MySQL: {e}")

    def disconnect(self):
        """Fecha a conexão."""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("Desconectado do MySQL")

    def execute(self, sql: str, params: tuple = None) -> int:
        """Executa SQL (INSERT/UPDATE/CREATE). Retorna rowcount."""
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql, params)
            return cursor.rowcount
        finally:
            cursor.close()

    def query(self, sql: str, params: tuple = None) -> list[dict]:
        """Executa SELECT e retorna lista de dicts."""
        cursor = self.connection.cursor(dictionary=True)
        try:
            cursor.execute(sql, params)
            return cursor.fetchall()
        finally:
            cursor.close()

    def record_exists(self, table: str, id_value) -> bool:
        """Verifica se um registro existe pela PK."""
        result = self.query(f"SELECT COUNT(*) as cnt FROM `{table}` WHERE id = %s", (id_value,))
        return result[0]["cnt"] > 0

    def commit(self):
        """Confirma a transação."""
        self.connection.commit()

    def rollback(self):
        """Desfaz a transação."""
        self.connection.rollback()

    # ========================================================================
    # Criação de Tabelas — Espelha a estrutura JSON do Bling
    # ========================================================================

    def create_contatos_tables(self):
        """Cria as tabelas de contatos que espelham o JSON do Bling."""

        # Tabela Tipos de Contato (Lista Mestre)
        self.execute("""
            CREATE TABLE IF NOT EXISTS tipos_contato (
                id BIGINT PRIMARY KEY,
                descricao VARCHAR(255)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        # Tabela principal: contatos
        self.execute("""
            CREATE TABLE IF NOT EXISTS contatos (
                id BIGINT PRIMARY KEY,
                nome VARCHAR(255),
                codigo VARCHAR(50),
                situacao VARCHAR(1),
                numero_documento VARCHAR(14),
                telefone VARCHAR(30),
                celular VARCHAR(30),
                fantasia VARCHAR(255),
                tipo VARCHAR(1),
                indicadorIe INT,
                ie VARCHAR(30),
                rg VARCHAR(30),
                inscricaoMunicipal VARCHAR(30),
                orgaoEmissor VARCHAR(30),
                email VARCHAR(255),
                emailNotaFiscal VARCHAR(255),
                -- Endereço Geral
                end_geral_endereco VARCHAR(255),
                end_geral_cep VARCHAR(10),
                end_geral_bairro VARCHAR(100),                
                end_geral_municipio VARCHAR(100),
                end_geral_uf VARCHAR(2),
                end_geral_numero VARCHAR(20),
                end_geral_complemento VARCHAR(100),        
                -- Endereço Cobrança
                end_cobranca_endereco VARCHAR(255),
                end_cobranca_cep VARCHAR(10),
                end_cobranca_bairro VARCHAR(100),                
                end_cobranca_municipio VARCHAR(100),
                end_cobranca_uf VARCHAR(2),
                end_cobranca_numero VARCHAR(20),
                end_cobranca_complemento VARCHAR(100),   
                orgaoPublico VARCHAR(10),
                vendedor BIGINT,
                ad_dataNascimento DATE NULL,
                ad_sexo VARCHAR(10),
                ad_naturalidade VARCHAR(255),
                finan_limiteCredito DECIMAL(15,2),  
                finan_condicaoPagamento VARCHAR(255), 
                finan_categoria VARCHAR(255),
                pais VARCHAR(255),
                tiposContato JSON,
                pessoasContato JSON,        
                -- JSON backup + timestamps
                json_completo JSON,
                migrado_em DATETIME DEFAULT CURRENT_TIMESTAMP,
                atualizado_em DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_nome (nome),
                INDEX idx_documento (numero_documento)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        self.commit()
        logger.success("Tabelas de contatos criadas/verificadas")

    def create_produtos_tables(self):
        """Cria a tabela de produtos que espelha o JSON do Bling."""
        self.execute("""
            CREATE TABLE IF NOT EXISTS produtos (
                id BIGINT PRIMARY KEY,
                idProdutoPai BIGINT,
                nome VARCHAR(255),
                codigo VARCHAR(100),
                preco DECIMAL(15,2),
                precoCusto DECIMAL(15,2),
                tipo VARCHAR(1),
                situacao VARCHAR(1),
                formato VARCHAR(1),
                descricaoCurta TEXT,
                dataValidade DATE NULL,
                unidade VARCHAR(20),
                pesoLiquido DECIMAL(15,4),
                pesoBruto DECIMAL(15,4),
                volumes INT,
                itensPorCaixa INT,
                gtin VARCHAR(50),
                gtinEmbalagem VARCHAR(50),
                tipoProducao VARCHAR(1),
                condicao INT,
                freteGratis BOOLEAN,
                marca VARCHAR(255),
                descricaoComplementar TEXT,
                linkExterno VARCHAR(255),
                observacoes TEXT,
                descricaoEmbalagemDiscreta VARCHAR(255),
                categoria_id BIGINT,
                fornecedor_id BIGINT,
                categoria JSON,
                fornecedor JSON,
                actionEstoque VARCHAR(50),
                artigoPerigoso BOOLEAN,
                -- Objetos Complexos como JSON
                dimensoes JSON,
                tributacao JSON,
                midia JSON,
                linhaProduto JSON,
                estrutura JSON,
                camposCustomizados JSON,
                variacoes JSON,
                variacao JSON,
                estoque JSON,
                -- JSON completo
                json_completo JSON,
                migrado_em DATETIME DEFAULT CURRENT_TIMESTAMP,
                atualizado_em DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_nome (nome),
                INDEX idx_codigo (codigo),
                INDEX idx_situacao (situacao),
                INDEX idx_idProdutoPai (idProdutoPai)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        self.commit()
        logger.success("Tabela de produtos criada/verificada")

    def create_pedidos_tables(self):
        """Cria a tabela de pedidos de venda que espelha o JSON do Bling."""
        self.execute("""
            CREATE TABLE IF NOT EXISTS pedidos (
                id BIGINT PRIMARY KEY,
                numero INT,
                numeroLoja VARCHAR(100),
                data DATE,
                dataSaida DATE,
                dataPrevista DATE NULL,
                totalProdutos DECIMAL(15,2),
                total DECIMAL(15,2),
                
                contato_id BIGINT,
                contato_nome VARCHAR(255),
                contato_tipoPessoa VARCHAR(1),
                contato_numeroDocumento VARCHAR(50),
                
                situacao_id INT,
                situacao_valor INT,
                
                loja_id BIGINT,
                unidadeNegocio_id BIGINT,
                
                numeroPedidoCompra VARCHAR(100),
                outrasDespesas DECIMAL(15,2),
                observacoes TEXT,
                observacoesInternas TEXT,
                
                categoria_id BIGINT,
                notaFiscal_id BIGINT,
                vendedor_id BIGINT,
                
                -- Arrays e Objetos em JSON nativos
                desconto JSON,
                tributacao JSON,
                parcelas JSON,
                transporte JSON,
                intermediador JSON,
                taxas JSON,
                
                json_completo JSON,
                migrado_em DATETIME DEFAULT CURRENT_TIMESTAMP,
                atualizado_em DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                
                INDEX idx_numero (numero),
                INDEX idx_data (data),
                INDEX idx_contato_id (contato_id),
                INDEX idx_situacao_id (situacao_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # Tabela para os Itens do Pedido (Filha)
        self.execute("""
            CREATE TABLE IF NOT EXISTS pedidos_itens (
                id BIGINT PRIMARY KEY,
                pedido_id BIGINT NOT NULL,
                codigo VARCHAR(100),
                unidade VARCHAR(20),
                quantidade DECIMAL(15,4),
                desconto DECIMAL(15,2),
                valor DECIMAL(15,2),
                aliquotaIPI DECIMAL(5,2),
                descricao TEXT,
                descricaoDetalhada TEXT,
                produto_id BIGINT,
                comissao_base DECIMAL(15,2),
                comissao_aliquota DECIMAL(5,2),
                comissao_valor DECIMAL(15,2),
                naturezaOperacao_id BIGINT,
                
                INDEX idx_pedido_id (pedido_id),
                INDEX idx_produto_id (produto_id),
                CONSTRAINT fk_pedido_item FOREIGN KEY (pedido_id) 
                    REFERENCES pedidos(id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        self.commit()
        logger.success("Tabela de pedidos criada/verificada")


