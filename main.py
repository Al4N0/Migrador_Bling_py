import customtkinter as ctk
import threading
import traceback
from core import BlingAPI, Database
from migrator import ContatosMigrator
from produtos_migrator import ProdutosMigrator
from pedidos_venda_migrator import PedidosVendaMigrator
from vendedores_migrator import VendedoresMigrator
from contas_receber_migrator import ContasReceberMigrator
from formas_pagamento_migrator import FormasPagamentoMigrator
from notas_fiscais_migrator import NotasFiscaisMigrator

# Classe da janela principal
class App(ctk.CTk):
    def __init__(self):
        super().__init__() 

        # Configuração da janela
        self.title("Migrador ETL Bling -> MySQL")
        
        # Dobro do tamanho
        window_width = 1400
        window_height = 1000
        
        # Obter resolução da tela
        screen_width = self.winfo_screenwidth()
        screen_height = self.winfo_screenheight()
        
        # Centralizar a janela
        x = (screen_width // 2) - (window_width // 2)
        y = (screen_height // 2) - (window_height // 2)
        
        self.geometry(f"{window_width}x{window_height}+{x}+{y}")
        ctk.set_appearance_mode("dark")  # Dark Mode!
        ctk.set_default_color_theme("blue")
        

        # Variáveis de estado
        self.api = None       # BlingAPI (Criado ao conectar)  
        self.db = None        # Database (Criado ao conectar)
        self.pause_event = threading.Event()
        self.pause_event.set() # Inicialmente liberado (verde)

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
        
        self.btn_pedidos = ctk.CTkButton(self.sidebar_frame, text="🛒 Migrar Pedidos Venda", command=self.on_migrate_pedidos, state="disabled")
        self.btn_pedidos.pack(fill="x", padx=20, pady=10)
        
        self.btn_vendedores = ctk.CTkButton(self.sidebar_frame, text="💼 Migrar Vendedores", command=self.on_migrate_vendedores, state="disabled")
        self.btn_vendedores.pack(fill="x", padx=20, pady=10)
        
        self.btn_contas_receber = ctk.CTkButton(self.sidebar_frame, text="💰 Migrar Contas a Receber", command=self.on_migrate_contas_receber, state="disabled")
        self.btn_contas_receber.pack(fill="x", padx=20, pady=10)
        
        self.btn_formas_pagamento = ctk.CTkButton(self.sidebar_frame, text="💳 Migrar Formas de Pagamento", command=self.on_migrate_formas_pagamento, state="disabled")
        self.btn_formas_pagamento.pack(fill="x", padx=20, pady=10)
        
        self.btn_notas_fiscais = ctk.CTkButton(self.sidebar_frame, text="🧾 Migrar Notas Fiscais", command=self.on_migrate_notas_fiscais, state="disabled")
        self.btn_notas_fiscais.pack(fill="x", padx=20, pady=10)
        
        self.btn_pause = ctk.CTkButton(self.sidebar_frame, text="⏸ Pausar Migração", command=self.on_toggle_pause, state="disabled", fg_color="orange", text_color="black")
        self.btn_pause.pack(fill="x", padx=20, pady=10)
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

    def on_toggle_pause(self):
        """Alterna o estado de pausa da migração."""
        if self.pause_event.is_set():
            # Está rodando -> Vamos pausar
            self.pause_event.clear()
            self.btn_pause.configure(text="▶ Retomar Migração", fg_color="green", text_color="white")
            self.log("⏸ Migração Pausada pelo usuário.")
            self.update_status("Pausado")
        else:
            # Está pausado -> Vamos retomar
            self.pause_event.set()
            self.btn_pause.configure(text="⏸ Pausar Migração", fg_color="orange", text_color="black")
            self.log("▶ Migração Retomada.")
            self.update_status("Retomando...")

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
            self.btn_pedidos.configure(state="normal")
            self.btn_vendedores.configure(state="normal")
            self.btn_contas_receber.configure(state="normal")
            self.btn_formas_pagamento.configure(state="normal")
            self.btn_notas_fiscais.configure(state="normal")
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
        self.btn_pedidos.configure(state="disabled")
        self.btn_vendedores.configure(state="disabled")
        self.btn_contas_receber.configure(state="disabled")
        self.btn_pause.configure(state="normal", text="⏸ Pausar Migração", fg_color="orange", text_color="black")
        self.pause_event.set()
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
                pause_event=self.pause_event,
            )               
            total = migrator.execute()
            self.after(0, lambda: self.log(f"🎉 Migração finalizada! {total} contatos"))
        except Exception as e:
            tb = traceback.format_exc()
            self.after(0, lambda e=e, tb=tb: self.log(f"❌ Erro na migração de contatos: {e}\n{tb}"))
        finally:
            # Salva o log após finalizar a migração
            self.after(0, lambda: self._save_log_to_file("contatos.log"))
            # Reabilita botões (via after para ser thread-safe)
            self.after(0, lambda: self.btn_pause.configure(state="disabled"))
            self.after(0, lambda: self.btn_connect.configure(state="normal"))
            self.after(0, lambda: self.btn_contatos.configure(state="normal"))
            self.after(0, lambda: self.btn_produtos.configure(state="normal"))
            self.after(0, lambda: self.btn_pedidos.configure(state="normal"))
            self.after(0, lambda: self.btn_vendedores.configure(state="normal"))
            self.after(0, lambda: self.btn_contas_receber.configure(state="normal"))
            self.after(0, lambda: self.btn_formas_pagamento.configure(state="normal"))
            self.after(0, lambda: self.btn_notas_fiscais.configure(state="normal"))

    def on_migrate_produtos(self):
        """Inicia a migração de produtos em thread separada."""
        self.btn_connect.configure(state="disabled")
        self.btn_contatos.configure(state="disabled")
        self.btn_produtos.configure(state="disabled")
        self.btn_pedidos.configure(state="disabled")
        self.btn_vendedores.configure(state="disabled")
        self.btn_contas_receber.configure(state="disabled")
        self.btn_formas_pagamento.configure(state="disabled")
        self.btn_notas_fiscais.configure(state="disabled")
        self.btn_pause.configure(state="normal", text="⏸ Pausar Migração", fg_color="orange", text_color="black")
        self.pause_event.set()
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
                pause_event=self.pause_event,
            )               
            total = migrator.execute()
            self.after(0, lambda: self.log(f"🎉 Migração de Produtos finalizada! {total} processados"))
        except Exception as e:
            tb = traceback.format_exc()
            self.after(0, lambda e=e, tb=tb: self.log(f"❌ Erro na migração de produtos: {e}\n{tb}"))
        finally:
            self.after(0, lambda: self._save_log_to_file("produtos.log"))
            self.after(0, lambda: self.btn_pause.configure(state="disabled"))
            self.after(0, lambda: self.btn_connect.configure(state="normal"))
            self.after(0, lambda: self.btn_contatos.configure(state="normal"))
            self.after(0, lambda: self.btn_produtos.configure(state="normal"))
            self.after(0, lambda: self.btn_pedidos.configure(state="normal"))
            self.after(0, lambda: self.btn_vendedores.configure(state="normal"))
            self.after(0, lambda: self.btn_contas_receber.configure(state="normal"))
            self.after(0, lambda: self.btn_formas_pagamento.configure(state="normal"))
            self.after(0, lambda: self.btn_notas_fiscais.configure(state="normal"))

    def on_migrate_pedidos(self):
        """Inicia a migração de pedidos de venda em thread separada."""
        self.btn_connect.configure(state="disabled")
        self.btn_contatos.configure(state="disabled")
        self.btn_produtos.configure(state="disabled")
        self.btn_pedidos.configure(state="disabled")
        self.btn_vendedores.configure(state="disabled")
        self.btn_contas_receber.configure(state="disabled")
        self.btn_formas_pagamento.configure(state="disabled")
        self.btn_notas_fiscais.configure(state="disabled")
        self.btn_pause.configure(state="normal", text="⏸ Pausar Migração", fg_color="orange", text_color="black")
        self.pause_event.set()
        self.progress.set(0)

        thread = threading.Thread(target=self._run_pedidos_venda_migration, daemon=True)
        thread.start()

    def _run_pedidos_venda_migration(self):
        """Execute a migração de pedidos de venda (roda na thread)."""
        try:
            migrator = PedidosVendaMigrator(
                api=self.api,
                db=self.db,
                on_progress=self._safe_progress,
                pause_event=self.pause_event,
            )               
            total = migrator.execute()
            self.after(0, lambda: self.log(f"🎉 Migração de Pedidos finalizada! {total} processados"))
        except Exception as e:
            tb = traceback.format_exc()
            self.after(0, lambda e=e, tb=tb: self.log(f"❌ Erro na migração de pedidos: {e}\n{tb}"))
        finally:
            self.after(0, lambda: self._save_log_to_file("pedidos_venda.log"))
            self.after(0, lambda: self.btn_pause.configure(state="disabled"))
            self.after(0, lambda: self.btn_connect.configure(state="normal"))
            self.after(0, lambda: self.btn_contatos.configure(state="normal"))
            self.after(0, lambda: self.btn_produtos.configure(state="normal"))
            self.after(0, lambda: self.btn_pedidos.configure(state="normal"))
            self.after(0, lambda: self.btn_vendedores.configure(state="normal"))
            self.after(0, lambda: self.btn_contas_receber.configure(state="normal"))
            self.after(0, lambda: self.btn_formas_pagamento.configure(state="normal"))
            self.after(0, lambda: self.btn_notas_fiscais.configure(state="normal"))

    def on_migrate_vendedores(self):
        """Inicia a migração de vendedores em thread separada."""
        self.btn_connect.configure(state="disabled")
        self.btn_contatos.configure(state="disabled")
        self.btn_produtos.configure(state="disabled")
        self.btn_pedidos.configure(state="disabled")
        self.btn_vendedores.configure(state="disabled")
        self.btn_contas_receber.configure(state="disabled")
        self.btn_formas_pagamento.configure(state="disabled")
        self.btn_notas_fiscais.configure(state="disabled")
        self.btn_pause.configure(state="normal", text="⏸ Pausar Migração", fg_color="orange", text_color="black")
        self.pause_event.set()
        self.progress.set(0)

        thread = threading.Thread(target=self._run_vendedores_migration, daemon=True)
        thread.start()

    def _run_vendedores_migration(self):
        """Execute a migração de vendedores (roda na thread)."""
        try:
            migrator = VendedoresMigrator(
                api=self.api,
                db=self.db,
                on_progress=self._safe_progress,
                pause_event=self.pause_event,
            )               
            total = migrator.execute()
            self.after(0, lambda: self.log(f"🎉 Migração de Vendedores finalizada! {total} processados"))
        except Exception as e:
            tb = traceback.format_exc()
            self.after(0, lambda e=e, tb=tb: self.log(f"❌ Erro na migração de vendedores: {e}\n{tb}"))
        finally:
            self.after(0, lambda: self._save_log_to_file("vendedores.log"))
            self.after(0, lambda: self.btn_pause.configure(state="disabled"))
            self.after(0, lambda: self.btn_connect.configure(state="normal"))
            self.after(0, lambda: self.btn_contatos.configure(state="normal"))
            self.after(0, lambda: self.btn_produtos.configure(state="normal"))
            self.after(0, lambda: self.btn_pedidos.configure(state="normal"))
            self.after(0, lambda: self.btn_vendedores.configure(state="normal"))
            self.after(0, lambda: self.btn_contas_receber.configure(state="normal"))
            self.after(0, lambda: self.btn_formas_pagamento.configure(state="normal"))
            self.after(0, lambda: self.btn_notas_fiscais.configure(state="normal"))

    def on_migrate_contas_receber(self):
        """Inicia a migração de contas a receber em thread separada."""
        self.btn_connect.configure(state="disabled")
        self.btn_contatos.configure(state="disabled")
        self.btn_produtos.configure(state="disabled")
        self.btn_pedidos.configure(state="disabled")
        self.btn_vendedores.configure(state="disabled")
        self.btn_contas_receber.configure(state="disabled")
        self.btn_formas_pagamento.configure(state="disabled")
        self.btn_notas_fiscais.configure(state="disabled")
        self.btn_pause.configure(state="normal", text="⏸ Pausar Migração", fg_color="orange", text_color="black")
        self.pause_event.set()
        self.progress.set(0)

        thread = threading.Thread(target=self._run_contas_receber_migration, daemon=True)
        thread.start()

    def _run_contas_receber_migration(self):
        """Execute a migração de contas a receber (roda na thread)."""
        try:
            migrator = ContasReceberMigrator(
                api=self.api,
                db=self.db,
                on_progress=self._safe_progress,
                pause_event=self.pause_event,
            )               
            total = migrator.execute()
            self.after(0, lambda: self.log(f"🎉 Migração de Contas a Receber finalizada! {total} processados"))
        except Exception as e:
            tb = traceback.format_exc()
            self.after(0, lambda e=e, tb=tb: self.log(f"❌ Erro na migração de contas a receber: {e}\n{tb}"))
        finally:
            self.after(0, lambda: self._save_log_to_file("contas_receber.log"))
            self.after(0, lambda: self.btn_pause.configure(state="disabled"))
            self.after(0, lambda: self.btn_connect.configure(state="normal"))
            self.after(0, lambda: self.btn_contatos.configure(state="normal"))
            self.after(0, lambda: self.btn_produtos.configure(state="normal"))
            self.after(0, lambda: self.btn_pedidos.configure(state="normal"))
            self.after(0, lambda: self.btn_vendedores.configure(state="normal"))
            self.after(0, lambda: self.btn_contas_receber.configure(state="normal"))
            self.after(0, lambda: self.btn_formas_pagamento.configure(state="normal"))
            self.after(0, lambda: self.btn_notas_fiscais.configure(state="normal"))

    def on_migrate_formas_pagamento(self):
        """Inicia a migração de formas de pagamento em thread separada."""
        self.btn_connect.configure(state="disabled")
        self.btn_contatos.configure(state="disabled")
        self.btn_produtos.configure(state="disabled")
        self.btn_pedidos.configure(state="disabled")
        self.btn_vendedores.configure(state="disabled")
        self.btn_contas_receber.configure(state="disabled")
        self.btn_formas_pagamento.configure(state="disabled")
        self.btn_notas_fiscais.configure(state="disabled")
        self.btn_pause.configure(state="normal", text="⏸ Pausar Migração", fg_color="orange", text_color="black")
        self.pause_event.set()
        self.progress.set(0)

        thread = threading.Thread(target=self._run_formas_pagamento_migration, daemon=True)
        thread.start()

    def _run_formas_pagamento_migration(self):
        """Execute a migração de formas de pagamento (roda na thread)."""
        try:
            migrator = FormasPagamentoMigrator(
                api=self.api,
                db=self.db,
                on_progress=self._safe_progress,
                pause_event=self.pause_event,
            )               
            total = migrator.execute()
            self.after(0, lambda: self.log(f"🎉 Migração de Formas de Pagamento finalizada! {total} processadas"))
        except Exception as e:
            tb = traceback.format_exc()
            self.after(0, lambda e=e, tb=tb: self.log(f"❌ Erro na migração de formas de pagamento: {e}\n{tb}"))
        finally:
            self.after(0, lambda: self._save_log_to_file("formas_pagamento.log"))
            self.after(0, lambda: self.btn_pause.configure(state="disabled"))
            self.after(0, lambda: self.btn_connect.configure(state="normal"))
            self.after(0, lambda: self.btn_contatos.configure(state="normal"))
            self.after(0, lambda: self.btn_produtos.configure(state="normal"))
            self.after(0, lambda: self.btn_pedidos.configure(state="normal"))
            self.after(0, lambda: self.btn_vendedores.configure(state="normal"))
            self.after(0, lambda: self.btn_contas_receber.configure(state="normal"))
            self.after(0, lambda: self.btn_formas_pagamento.configure(state="normal"))
            self.after(0, lambda: self.btn_notas_fiscais.configure(state="normal"))

    def on_migrate_notas_fiscais(self):
        """Inicia a migração de notas fiscais em thread separada."""
        self.btn_connect.configure(state="disabled")
        self.btn_contatos.configure(state="disabled")
        self.btn_produtos.configure(state="disabled")
        self.btn_pedidos.configure(state="disabled")
        self.btn_vendedores.configure(state="disabled")
        self.btn_contas_receber.configure(state="disabled")
        self.btn_formas_pagamento.configure(state="disabled")
        self.btn_notas_fiscais.configure(state="disabled")
        self.btn_pause.configure(state="normal", text="⏸ Pausar Migração", fg_color="orange", text_color="black")
        self.pause_event.set()
        self.progress.set(0)

        thread = threading.Thread(target=self._run_notas_fiscais_migration, daemon=True)
        thread.start()

    def _run_notas_fiscais_migration(self):
        """Execute a migração de notas fiscais (roda na thread)."""
        try:
            migrator = NotasFiscaisMigrator(
                api=self.api,
                db=self.db,
                on_progress=self._safe_progress,
                pause_event=self.pause_event,
            )               
            total = migrator.execute()
            self.after(0, lambda: self.log(f"🎉 Migração de Notas Fiscais finalizada! {total} processadas"))
        except Exception as e:
            tb = traceback.format_exc()
            self.after(0, lambda e=e, tb=tb: self.log(f"❌ Erro na migração de notas fiscais: {e}\n{tb}"))
        finally:
            self.after(0, lambda: self._save_log_to_file("notas_fiscais.log"))
            self.after(0, lambda: self.btn_pause.configure(state="disabled"))
            self.after(0, lambda: self.btn_connect.configure(state="normal"))
            self.after(0, lambda: self.btn_contatos.configure(state="normal"))
            self.after(0, lambda: self.btn_produtos.configure(state="normal"))
            self.after(0, lambda: self.btn_pedidos.configure(state="normal"))
            self.after(0, lambda: self.btn_vendedores.configure(state="normal"))
            self.after(0, lambda: self.btn_contas_receber.configure(state="normal"))
            self.after(0, lambda: self.btn_formas_pagamento.configure(state="normal"))
            self.after(0, lambda: self.btn_notas_fiscais.configure(state="normal"))

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