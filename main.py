import customtkinter as ctk
import threading
from core import BlingAPI, Database
from migrator import ContatosMigrator
from produtos_migrator import ProdutosMigrator

# Classe da janela principal
class App(ctk.CTk):
    def __init__(self):
        super().__init__() 

        # Configuração da janela
        self.title("Migrador ETL Bling -> MySQL")     
        self.geometry("700x500")     
        ctk.set_appearance_mode("dark")  # Dark Mode!
        ctk.set_default_color_theme("blue")
        

        # Variáveis de estado
        self.api = None       # BlingAPI (Criado ao conectar)  
        self.db = None        # Database (Criado ao conectar)

        # 1. BARRA LATERAL (Esquerda)
        self.sidebar_frame = ctk.CTkFrame(self, width=200, corner_radius=0)
        self.sidebar_frame.pack(side="left", fill="y")
        self.sidebar_frame.pack_propagate(False) # Impede que a barra encolha
        
        # 2. ÁREA PRINCIPAL (Direita)
        self.main_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.main_frame.pack(side="left", fill="both", expand=True, padx=10, pady=10)
        
        # ----- ITENS DA BARRA LATERAL (Esquerda) -----
        self.lbl_titulo = ctk.CTkLabel(self.sidebar_frame, text="MENU", font=("", 18, "bold"))
        self.lbl_titulo.pack(pady=(20, 20)) # Margem superior/inferior
        # Campo do Banco
        self.lbl_banco = ctk.CTkLabel(self.sidebar_frame, text="Banco MySQL:", anchor="w")
        self.lbl_banco.pack(fill="x", padx=20, pady=(10, 0))
        self.entry_banco = ctk.CTkEntry(self.sidebar_frame, placeholder_text="Nome do banco")
        self.entry_banco.pack(fill="x", padx=20, pady=(0, 20))
        self.entry_banco.insert(0, "bling_staging")
        # Botões
        self.btn_connect = ctk.CTkButton(self.sidebar_frame, text="🔌 Conectar DB", command=self.on_connect)
        self.btn_connect.pack(fill="x", padx=20, pady=10)
        self.btn_contatos = ctk.CTkButton(self.sidebar_frame, text="👥 Migrar Contatos", command=self.on_migrate_contatos, state="disabled")
        self.btn_contatos.pack(fill="x", padx=20, pady=10)
        
        self.btn_produtos = ctk.CTkButton(self.sidebar_frame, text="📦 Migrar Produtos", command=self.on_migrate_produtos, state="disabled")
        self.btn_produtos.pack(fill="x", padx=20, pady=10)
        
        # Futuros botões de Pedidos entrarão aqui!
        # ----- ITENS DA ÁREA PRINCIPAL (Direita) -----
        # Status e Progresso
        self.lbl_status = ctk.CTkLabel(self.main_frame, text="Aguardando conexão...", font=("", 14, "bold"))
        self.lbl_status.pack(anchor="w", pady=(0, 5)) 
        self.progress = ctk.CTkProgressBar(self.main_frame)
        self.progress.pack(fill="x", pady=(0, 15))
        self.progress.set(0)
        # Log de texto grande
        self.txt_log = ctk.CTkTextbox(self.main_frame, font=("Consolas", 12))
        self.txt_log.pack(fill="both", expand=True)

    def log(self, message: str):
        """Adiciona mensagem ao log com timestamp."""
        from datetime import datetime           
        timestamp = datetime.now().strftime("%H:%M:%S")         
        self.txt_log.insert("end", f"[{timestamp}] {message}\n")
        self.txt_log.see("end")       # Auto-scroll para o final

    def update_status(self, text:str):
        """Atualiza o label de status."""
        self.lbl_status.configure(text=text)

    def update_progress(self, current: int, total: int, message: str):
        """Callback chamado pelo migrador."""
        self.log(message)
        self.update_status(message)
        if total > 0:
            self.progress.set(current / total)     # Valor entre 0.0 e 1.0                                                                                        

    def on_connect(self):
        """Conecta ao MySQL e inicializa a API Bling."""
        db_name = self.entry_banco.get().strip()     # Pega o texto do Edit
        if not db_name:
            self.log("❌ Informe o nome do banco de destino!")
            return

        try:
            # Inicializa a API Bling
            self.api = BlingAPI()
            self.log("API Bling inicializada ✅")

            # Conecta ao MySQL
            self.db = Database(db_name)
            self.db.connect()
            self.log(f"MySQL Conectado → {db_name} ✅")

            # Habilita os botões de migração
            self.btn_contatos.configure(state="normal")
            self.btn_produtos.configure(state="normal")
            self.update_status("Conectado! Pronto para migrar.")

        except Exception as e:
            self.log(f"❌ Erro ao conectar: {e}")
            self.update_status("Erro na conexão")

    def on_migrate_contatos(self):
        """Inicia a migração de contatos em thread separada."""
        # Desabilita os botões durante a migração
        self.btn_connect.configure(state="disabled")
        self.btn_contatos.configure(state="disabled")
        self.btn_produtos.configure(state="disabled")
        self.progress.set(0)

        # Roda em thread separada para não travar a interface
        thread =threading.Thread(target=self._run_contatos_migration, daemon=True)
        thread.start()

    def _run_contatos_migration(self):
        """Execute a migração (roda na thread). Não interferir na UI diretamente aqui!"""
        try:
            migrator = ContatosMigrator(
                api=self.api,
                db=self.db,
                on_progress=self._safe_progress,      # Callback thread-safe
            )               
            total = migrator.execute()
            self.after(0, lambda: self.log(f"🎉 Migração finalizada! {total} contatos"))
        except Exception as e:
            self.after(0, lambda: self.log(f"❌ Erro na migração: {e}"))
        finally:
            # Salva o log após finalizar a migração
            self.after(0, lambda: self._save_log_to_file("contatos.log"))
            # Reabilita botões (via after para ser thread-safe)
            self.after(0, lambda: self.btn_connect.configure(state="normal"))
            self.after(0, lambda: self.btn_contatos.configure(state="normal"))
            self.after(0, lambda: self.btn_produtos.configure(state="normal"))

    def on_migrate_produtos(self):
        """Inicia a migração de produtos em thread separada."""
        self.btn_connect.configure(state="disabled")
        self.btn_contatos.configure(state="disabled")
        self.btn_produtos.configure(state="disabled")
        self.progress.set(0)

        thread = threading.Thread(target=self._run_produtos_migration, daemon=True)
        thread.start()

    def _run_produtos_migration(self):
        """Execute a migração de produtos (roda na thread)."""
        try:
            migrator = ProdutosMigrator(
                api=self.api,
                db=self.db,
                on_progress=self._safe_progress,
            )               
            total = migrator.execute()
            self.after(0, lambda: self.log(f"🎉 Migração de Produtos finalizada! {total} processados"))
        except Exception as e:
            self.after(0, lambda: self.log(f"❌ Erro na migração de produtos: {e}"))
        finally:
            self.after(0, lambda: self._save_log_to_file("produtos.log"))
            self.after(0, lambda: self.btn_connect.configure(state="normal"))
            self.after(0, lambda: self.btn_contatos.configure(state="normal"))
            self.after(0, lambda: self.btn_produtos.configure(state="normal"))

    def _save_log_to_file(self, filename: str):
        """Salva o conteúdo da caixa de texto do log na pasta logs."""
        import os
        try:
            log_content = self.txt_log.get("1.0", "end-1c")
            
            # Cria a pasta 'logs' na raiz se não existir
            logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
            os.makedirs(logs_dir, exist_ok=True)
            
            # Caminho completo do arquivo
            filepath = os.path.join(logs_dir, filename)
            
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(log_content)
            self.log(f"📄 Arquivo de log salvo com sucesso em: logs/{filename}")
        except Exception as e:
            self.log(f"❌ Falha ao salvar arquivo de log: {e}")

    def _safe_progress(self, current: int, total: int, message: str):
        """Chama update_progress de forma thread-safe."""
        self.after(0, lambda: self.update_progress(current, total, message))     

if __name__ == "__main__":
    app = App()
    app.mainloop()                   