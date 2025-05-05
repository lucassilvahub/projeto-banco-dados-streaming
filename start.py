import subprocess
import time
import webbrowser
import platform

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
    parar_containers()  # Parar containers antes de subir novos
    success = subir_containers()

    if success:
        print("⏳ Aguardando containers subirem...")
        time.sleep(8)
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
