import subprocess
import time
import webbrowser
import platform
import os

# Caminho opcional para o Google Chrome (dependendo do sistema)
CHROME_PATH = {
    "Windows": "C:/Program Files/Google/Chrome/Application/chrome.exe %s",
    "Darwin": "open -a 'Google Chrome' %s",
    "Linux": "/usr/bin/google-chrome %s",
}


def abrir_navegador(url):
    sistema = platform.system()
    chrome_cmd = CHROME_PATH.get(sistema)

    try:
        if chrome_cmd:
            webbrowser.get(chrome_cmd).open_new_tab(url)
        else:
            # fallback caso o navegador não esteja configurado
            webbrowser.open_new_tab(url)
    except Exception as e:
        print(f"❌ Não foi possível abrir o navegador: {e}")


def parar_containers():
    try:
        print("🔴 Parando containers existentes...")
        subprocess.run(["docker-compose", "down"], check=True)
        print("✅ Containers parados com sucesso!")
    except subprocess.CalledProcessError:
        print("❌ Erro ao parar os containers.")


def verificar_docker():
    """Verifica se o Docker e Docker Compose estão instalados"""
    try:
        docker_version = subprocess.run(["docker", "--version"], capture_output=True, text=True)
        compose_version = subprocess.run(["docker-compose", "--version"], capture_output=True, text=True)
        
        if docker_version.returncode == 0 and compose_version.returncode == 0:
            print("✅ Docker e Docker Compose encontrados.")
            return True
        else:
            print("❌ Docker ou Docker Compose não encontrados. Instale-os antes de continuar.")
            return False
    except FileNotFoundError:
        print("❌ Docker ou Docker Compose não encontrados. Instale-os antes de continuar.")
        return False


def verificar_e_criar_diretorios():
    """Verifica e cria os diretórios necessários para o projeto"""
    print("🔍 Verificando estrutura de diretórios...")
    
    diretorios = [
        "s1/app", 
        "s2/app", 
        "s3/app", 
        "dashboard/html"
    ]
    
    for diretorio in diretorios:
        if not os.path.exists(diretorio):
            print(f"📁 Criando diretório: {diretorio}")
            os.makedirs(diretorio, exist_ok=True)
    
    print("✅ Estrutura de diretórios verificada/criada com sucesso!")


def subir_containers():
    try:
        print("🔧 Subindo containers com Docker Compose...")
        # Modificado para não usar o modo detached (-d), assim os logs ficarão visíveis
        subprocess.Popen(["docker-compose", "up"])
        print("✅ Iniciando containers...")
        return True
    except subprocess.CalledProcessError:
        print("❌ Erro ao subir os containers.")
        return False


def abrir_interfaces_web():
    print("🌐 Abrindo dashboard...")
    abrir_navegador("http://localhost:8089")


if __name__ == "__main__":
    if not verificar_docker():
        exit(1)
    
    verificar_e_criar_diretorios()
    parar_containers()  # Parar containers antes de subir novos
    success = subir_containers()

    if success:
        print("⏳ Aguardando containers subirem...")
        time.sleep(15)  # Tempo maior para garantir que todos os serviços estão prontos
        abrir_interfaces_web()
        print("✅ Interfaces web abertas com sucesso!")
        print("🖥️ Os logs dos containers estão sendo exibidos abaixo.")
        print("ℹ️ Pressione Ctrl+C para encerrar os containers quando terminar.")

        # O script continuará em execução enquanto os containers estiverem rodando
        # Os logs serão exibidos no terminal
        try:
            # Este loop mantém o script em execução até que o usuário pressione Ctrl+C
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n🛑 Encerrando os containers...")
            parar_containers()
            print("👋 Obrigado por usar o streaming app!")