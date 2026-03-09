# Migrador Bling V3 para MySQL (Python Edition)

Migrador de dados projetado para espelhar a estrutura da **API V3 do Bling** em bancos de dados relacionais **MySQL**. 
Atualmente, extrai os Contatos em duas fases (listagem paginada e requisição de detalhes), mapeando dados como JSONs salvos nativamente ou de forma relacional para máxima fidelidade dos dados obtidos.

O projeto foi portado a partir da antiga versão em **Delphi VCL (OO)**, aplicando as melhores práticas de **Python Moderno**: tipagem de dados, arquitetura limpa e UI com **CustomTkinter** (Dark Mode out of the box).

---

## 🚀 Funcionalidades Principais
*   **Fluxo em Duas Etapas**: Extracão `GET /contatos` paginado + extração `GET /contatos/:id` para capturar todos os dados não fornecidos na listagem básica.
*   **OAuth 2.0 Nativo**: Auto-Renovação configurada de Access Token através de um Refresh Token caso a chamada caia via HTTP 401. 
*   **Controle Inteligente de API**: Biblioteca `tenacity` implementando Exponential Backoff para lidar com o `HTTP 429` e pausas controladas de 350ms em requests manuais. 
*   **Gravação Idempotente no Banco de Dados**: A aplicação lida automaticamente com `INSERT ... ON DUPLICATE KEY UPDATE` nos cadastros. Você pode rodar milhões de vezes que o dado será apenas atualizado.
*   **Múltiplas Threads Assíncronas (`threading`)**: Não congela a janela (CustomTkinter) enquanto o processo de banco é finalizado, contando com progresso em tempo real do Console para o form de interface gráfica.

---

## 🛠️ Tecnologias
- **[Python](https://www.python.org/)** — Núcleo / Engine principal.
- **[CustomTkinter](https://github.com/TomSchimansky/CustomTkinter)** — GUI Moderna e Dark Mode.
- **[Requests](https://pypi.org/project/requests/)** — Consumo e Request REST na documentação oficial Bling V3.
- **[MySQL Connector/Python](https://dev.mysql.com/doc/connector-python/en/)** — Camada de persistência MySQL nativa.
- **[Loguru](https://pypi.org/project/loguru/)** — Substituição inteligente ao *print()*. Log e stdout com cores e timestamp.
- **[Python-dotenv](https://pypi.org/project/python-dotenv/)** — Loader das chaves do arquivo `.env` para segurança do projeto.

---

## ⚙️ Pré-requisitos & Instalação

1. Clone o repositório na sua máquina local:
```cmd
git clone https://github.com/Al4N0/Migrador_Bling_py.git
cd Migrador_Bling_py
```
2. Crie um Ambiente Virtual isolado (VENV) usando Python:
```cmd
python -m venv venv
```
3. Ative o VENV e instale todos os pacotes:
**Windows**
```cmd
.\venv\Scripts\activate
pip install customtkinter requests mysql-connector-python loguru python-dotenv tenacity
```
**Linux / macOS**
```bash
source venv/bin/activate
pip install customtkinter requests mysql-connector-python loguru python-dotenv tenacity
```

4. Variáveis de Ambiente e Autenticação
Copie o arquivo *Modelo* de Configuração e informe os valores do seu banco de dados e as Keys Geradas pelo Aplicativo no Bling Dev.
```cmd
copy .env.example .env
```

---

## 💡 Como Usar
Execute `main.py` diretamente com o Python em sua venv.
```cmd
python main.py
```
*   A interface Gráfica aparecerá. Preencha o nome desejado do Schema MySQL e pressione **Conectar DB**. Caso o banco não exista, o Migrador criará magicamente e conectará.  
*   Ao estabelecer a comunicação, as Tabelas necessárias serão validadas e/ou criadas dentro do banco, replicando a árvore JSON da v3 Oficial.
*   Clique em **Migrar Contatos**  

> **Obs: Manutenção de Tokens de Acesso** \
> Certifique-se de configurar e usar os Tokens Gerados de Access / Refresh via POST na Autenticação OAuth. \
> O Refresh só deve acontecer se ainda não houver passado o prazo de vida do provedor (Bling) — Acesso em base diária. 

---

## 🔄 Fluxo de Desenvolvimento Atual
- [x] Correção de credenciais de Configuração `.env`.
- [x] Modelagem do `core.py` (Camada Infraestrutura e Persistência O.O).
- [x] Modelagem do `migrator.py` para Processos Massivos Lógicos.
- [x] Módulo `Contatos` completo.
- [x] UI CustomTkinter Modernizada.
- [x] Git Versionado e Configurado.
- [ ] Módulo Futuro: **Produtos**
- [ ] Módulo Futuro: **Pedidos**

---
*Projetado como refatoramento de Delphi VCL Engine para Python Modern Ecosystem*
