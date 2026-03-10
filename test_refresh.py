import os
from dotenv import load_dotenv
from loguru import logger
from core import BlingAPI

def testar_refresh():
    """
    Testador isolado para o Refresh Token do Bling API V3.
    Use este script para confirmar se o REFRESH_TOKEN do seu .env
    ainda está válido e conseguindo gerar novos Access Tokens.
    """
    logger.info("🧪 Iniciando teste de Refresh Token...")
    
    # Recarrega variáveis de ambiente do .env para garantir os dados mais recentes
    load_dotenv(override=True)
    
    try:
        # A instância de BlingAPI já carrega as credenciais do .env automaticamente
        api = BlingAPI()
        
        token_antigo = api.access_token
        refresh_antigo = api.refresh_token_value
        
        logger.info(f"O Access Token atual no .env é: {str(token_antigo)[:15]}... (ocultado por segurança)")
        logger.info(f"O Refresh Token atual no .env é: {str(refresh_antigo)[:15]}... (ocultado por segurança)")
        
        logger.warning("Forçando renovação (do_refresh_token)...")
        # Força o refresh, contatando o Bling
        api.do_refresh_token()
        
        token_novo = api.access_token
        refresh_novo = api.refresh_token_value
        
        logger.success("✅ REFRESH EXECUTADO COM SUCESSO!")
        logger.success(f"NOVO Access Token foi salvo:  {str(token_novo)[:15]}...")
        logger.success(f"NOVO Refresh Token foi salvo: {str(refresh_novo)[:15]}...")
        logger.success("Abra o seu arquivo .env, ele deve ter sido atualizado automaticamente pelo sistema.")
        
    except Exception as e:
        logger.error(f"❌ Erro ao testar o refresh: {e}")
        logger.error("Se você recebeu HTTP 401 ou 400 aqui no Refresh:")
        logger.error("Isso significa que o REFRESH_TOKEN configurado no .env também está expirado ou é inválido.")
        logger.error("SOLUÇÃO: Siga o fluxo OAuth2 padrão do Bling no Postman ou Navegador para gerar novos tokens do zero e atualizar seu .env.")

if __name__ == "__main__":
    testar_refresh()
