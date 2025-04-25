import subprocess
import time
import webbrowser
import platform

# Caminho opcional para o Google Chrome (dependendo do sistema)
CHROME_PATH = {
    "Windows": "C:/Program Files/Google/Chrome/Application/chrome.exe %s",
    "Darwin": "open -a 'Google Chrome' %s",
    "Linux": "/usr/bin/google-chrome %s"
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
        subprocess.run(["docker-compose", "up", "-d"], check=True)
        print("✅ Containers iniciados com sucesso!")
    except subprocess.CalledProcessError:
        print("❌ Erro ao subir os containers.")
        exit(1)

def abrir_interfaces_web():
    print("🌐 Abrindo Adminer e Kafka UI...")
    abrir_navegador("http://localhost:8081/?pgsql=postgres&server=postgres&username=user&db=streaming_db&password=password")  # Adminer
    abrir_navegador("http://localhost:8080")  # Kafka UI

if __name__ == "__main__":
    parar_containers()  # Parar containers antes de subir novos
    subir_containers()
    print("⏳ Aguardando containers subirem...")
    time.sleep(15)  # Aumentando o tempo de espera para garantir que os containers inicializem completamente
    abrir_interfaces_web()
    print("✅ Interfaces web abertas com sucesso!")