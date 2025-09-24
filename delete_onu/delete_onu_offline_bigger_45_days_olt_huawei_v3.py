import os
import time
import re
from datetime import datetime, date
import pandas as pd
from dotenv import load_dotenv
from connection_ssh import ssh
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue
from threading import Lock

# Paths de arquivos
path_01_base = "service_port_all"  # Será usado como prefixo para cada thread
path_02 = "log_hw.txt"
path_03 = "onus_offline.txt"
qtd_dias = 45

# Variavel que controla o total de onus deletadas
total_onus_deletadas = 0
# Nova variável para controlar ONUs com last down time = "-"
total_onus_sem_last_down = 0

# Configurações de threading
MAX_THREADS = 50  # Número máximo de threads simultâneas
THREAD_DELAY = 4  # Delay entre inicialização de threads (segundos)


# Lock para escrita no arquivo de log (thread-safe)
log_lock = Lock()

# -------------------------
# Funções para controlar contador global
# -------------------------

def adicionar_onus_deletadas(quantidade):
    """
    Adiciona ONUs ao contador global usando o log_lock existente
    """
    global total_onus_deletadas
    with log_lock:
        total_onus_deletadas += quantidade
        
def adicionar_onus_sem_last_down(quantidade):
    """
    Adiciona ONUs sem last down time ao contador global
    """
    global total_onus_sem_last_down
    with log_lock:
        total_onus_sem_last_down += quantidade

def obter_total_onus_deletadas():
    """
    Retorna o total geral de ONUs deletadas
    """
    with log_lock:
        return total_onus_deletadas

def obter_total_onus_sem_last_down():
    """
    Retorna o total geral de ONUs sem last down time
    """
    with log_lock:
        return total_onus_sem_last_down

def salvar_total_no_log():
    """
    Salva o total final de ONUs deletadas e sem last down time no log
    """
    total_deletadas = obter_total_onus_deletadas()
    total_sem_last_down = obter_total_onus_sem_last_down()
    
    write_log("\n" + "="*50)
    write_log(f"TOTAL GERAL DE ONUs DELETADAS: {total_deletadas}")
    write_log(f"TOTAL GERAL DE ONUs SEM LAST DOWN TIME (-): {total_sem_last_down}")
    write_log("="*50)

# -------------------------
# Funções thread-safe
# -------------------------

def write_log(message, include_print=True):
    """
    Escreve log de forma thread-safe
    """
    if include_print:
        print(message)
    
    with log_lock:
        with open(path_02, "a", encoding="utf-8") as log_file:
            log_file.write(f"{message}\n")

def olt_date(shell):
    shell.send('enable\n')
    shell.send('config\n')
    shell.send('mmi-mode original-output\n')
    time.sleep(1)
    shell.send('display time\n\n')
    time.sleep(5)
    output = read_output(shell).splitlines()
    
    # procura a linha que comece com YYYY-MM-DD
    date_line = next((l.strip() for l in output if re.match(r"^\d{4}-\d{2}-\d{2}", l.strip())), None)
    
    if not date_line:
        raise ValueError("Não foi possível encontrar a data no display time")

    # separa apenas a parte da data (YYYY-MM-DD)
    date_str = date_line.split()[0]
    
    return datetime.strptime(date_str, '%Y-%m-%d').date()

# Função para ler toda a saída do shell sem cortar
def read_output(shell, buffer_size=65535, wait=1):
    output = ""
    time.sleep(wait)  # espera a saída ser gerada
    while shell.recv_ready():
        data = shell.recv(buffer_size).decode("utf-8", errors="ignore")
        output += data
        time.sleep(0.2)  # dá tempo para vir mais dados
    return output

def get_service_port(shell, thread_id):
    """
    Thread-safe version - cada thread usa seu próprio arquivo
    """
    path_01 = f"{path_01_base}_{thread_id}.txt"
    
    time.sleep(3)
    shell.send('enable\n')
    time.sleep(0.5)
    shell.send('config\n')
    time.sleep(0.5)
    shell.send('mmi-mode original-output\n')
    time.sleep(0.5)
    shell.send('display service-port all | include down\n\n')
    time.sleep(15)
    result = read_output(shell).splitlines()
    
    with open(path_01, 'w') as data:
        data.write("\n".join(result))
        
    print(f"[INFO] Thread-{thread_id}: ONU service-port salvo em {path_01}")
    return path_01

def contar_onus_down(path_01):
    """
    Thread-safe version
    """
    count_down = 0
    
    try:
        with open(path_01, 'r') as file:
            for line in file:
                if '    down' in line:
                    count_down += 1
        return count_down
    except FileNotFoundError:
        return 0

def get_statistics_from_service_port(path_01, thread_id):
    """
    Thread-safe version
    """
    try:
        with open(path_01, 'r') as file:
            content = file.read()
            
        # Procura pela linha de estatísticas
        match = re.search(r'Total\s*:\s*(\d+)\s*\(Up/Down\s*:\s*(\d+)/(\d+)\)', content)
        
        if match:
            total = int(match.group(1))
            up = int(match.group(2))  
            down = int(match.group(3))
            
            print(f"[INFO] Thread-{thread_id}: Estatísticas da OLT:")
            print(f"  Total de Service-Ports: {total}")
            print(f"  Online (Up): {up}")
            print(f"  Offline (Down): {down}")
            print(f"\n[INFO] Thread-{thread_id}: Encontradas {down} ONUs offline. Verificando histórico...\n")
            
            return down
        else:
            # Fallback: conta manualmente
            return contar_onus_down(path_01)
            
    except Exception as e:
        print(f"[WARN] Thread-{thread_id}: Erro ao extrair estatísticas: {e}")
        return contar_onus_down(path_01)

def get_onus_offlines(shell, host, thread_id):
    """
    Thread-safe version
    """
    print(f"[INFO] Thread-{thread_id}: Obtendo ONUs offline da OLT {host}...")
    path_01 = get_service_port(shell, thread_id)
    date_olt_now = olt_date(shell)
    print(f"[INFO] Thread-{thread_id}: Data atual da OLT: {date_olt_now}\n")
    
    # Usa a função que extrai das estatísticas 
    get_statistics_from_service_port(path_01, thread_id)
    
    list_onus_deletadas = []
    contador_sem_last_down = 0  # Contador local por OLT

    with open(path_01, 'r') as file:
        data = file.readlines()

    for line in data:
        if '    down' in line:
            
            result = line.split()
            
            #print(f"[DEBUG] Thread-{thread_id}: Linha service-port down encontrada: {result}")
            
            service_port_id = result[0]
            
             # Verifica se o próximo campo após 'gpon' contém '/' (formato completo)
            gpon_index = result.index('gpon')
            gpon_interface = result[gpon_index + 1]
            
            if gpon_interface.count('/') == 2:  # Formato: 0/15/6
                # Interface completa em um campo
                chassi_slot_pon = gpon_interface.split('/')
                chassi_id = chassi_slot_pon[0]
                slot_id = chassi_slot_pon[1] 
                pon_id = chassi_slot_pon[2]
                onu_id = result[gpon_index + 2]
                
            else:  # Formato: 0/1 /4 (chassi/slot separado do pon)
                # Interface dividida em dois campos
                chassi_slot = gpon_interface.split('/')
                chassi_id = chassi_slot[0]
                slot_id = chassi_slot[1]
                pon_id = result[gpon_index + 2].replace('/', '')  # Remove a '/' do pon
                onu_id = result[gpon_index + 3]
            
            print(f"[INFO] Thread-{thread_id}: Verificando SERVICE-PORT:{service_port_id} ONU {chassi_id}/{slot_id}/{pon_id}:{onu_id}...")

            shell.send(f"display ont info {chassi_id} {slot_id} {pon_id} {onu_id}\n\n")
            time.sleep(3)
            output = read_output(shell).splitlines()
            
            #print(f"[DEBUG] Thread-{thread_id}: Saída do comando display ont info:\n" + "\n".join(output))

            result_sn = None
            for l in output:
                if 'SN' in l and 'SN-auth' not in l:
                    #print(f"[DEBUG] Thread-{thread_id}: Linha SN encontrada: {l.strip()}")
                    result_sn = l.split()[2]
                elif 'Last down time' in l:
                    #print(f"[DEBUG] Thread-{thread_id}: Last down time linha: {l.strip()}")
                    if l.split()[4] == '-':
                        contador_sem_last_down += 1
                        list_onus_deletadas.append((result_sn, service_port_id, chassi_id, slot_id, pon_id, onu_id))
                        print(f"[INFO] Thread-{thread_id}: SERVICE-PORT:{service_port_id} ONU {chassi_id}/{slot_id}/{pon_id}:{onu_id} SEM LAST DOWN TIME (-)")
                        continue
                    else:
                        last_down_time = datetime.strptime(l.split()[4], '%Y-%m-%d').date()
                        
                        diff = (date_olt_now - last_down_time).days
                        if diff >= qtd_dias:
                            list_onus_deletadas.append((result_sn, service_port_id, chassi_id, slot_id, pon_id, onu_id))
                        print(f"[INFO] Thread-{thread_id}: SERVICE-PORT:{service_port_id} ONU {chassi_id}/{slot_id}/{pon_id}:{onu_id} ESTA A {diff} DIA(S) OFFLINE (último last_down_time {last_down_time})\n")
                    break
     # Adiciona ao contador global
    adicionar_onus_sem_last_down(contador_sem_last_down)
    
    # Log do total por OLT
    write_log(f"[INFO] Thread-{thread_id}: OLT {host} - {contador_sem_last_down} ONUs sem Last Down Time (-)")
    
    # Remove arquivo temporário
    try:
        os.remove(path_01)
    except:
        pass
        
    return list_onus_deletadas

# Função para registrar o inicio da rotina
def registrar_inicio_rotina():
    inicio = datetime.now()
    message = f'ROTINA MULTITHREADED HUAWEI INICIADA...\nInício: {inicio.strftime("%Y/%m/%d %H:%M:%S")}\nMax Threads: {MAX_THREADS}\n'
    write_log(message)
    return inicio

# Função para registrar o fim da rotina
def rotina_finalizada(start_time, total_hosts):
    fim = datetime.now()
    duracao = fim - start_time
    duracao_str = str(duracao).split('.')[0]
    
    message = f"\nTérmino: {fim.strftime('%Y/%m/%d %H:%M:%S')}\nDuração: {duracao_str}\nTotal de OLTs processadas: {total_hosts}\nROTINA FINALIZADA\n\n"
    write_log(message)

def save_olt(shell, host, thread_id):
    """
    Thread-safe version
    """
    try:
        print(f"[INFO] Thread-{thread_id}: Salvando configuração na OLT {host}...")
        # Limpa buffer antes
        while shell.recv_ready():
            shell.recv(1024)
            
        # Envia comando save
        shell.send('save\n\n')
        
        # Coleta resposta com timeout inteligente
        full_response = ""
        max_wait = 60
        waited = 0
        
        while waited < max_wait:
            time.sleep(1)
            waited += 1
        
            if shell.recv_ready():
                data = shell.recv(4096).decode("utf-8", errors="ignore") 
                full_response += data
                
                # Auto-confirma se necessário
                if re.search(r'\[y/n\]|\(y/n\)|y/n', full_response.lower()):
                    shell.send('y\n')
                    print(f"[DEBUG] Thread-{thread_id}: Auto-confirmação enviada")
                    continue
            
            # Verifica se terminou com sucesso
            if re.search(r'success|complete|saved.*successfully', full_response.lower()):
                print(f"[SUCCESS] Thread-{thread_id}: Configuração salva na OLT {host}")
                time.sleep(0.1)
                shell.send('quit\n')
                time.sleep(0.1)
                shell.send('quit\n')
                time.sleep(0.1)
                shell.send('y\n')
                break
            
            # Verifica erro explícito
            if re.search(r'error|failed|cannot.*save', full_response.lower()):
                raise Exception(f"Falha reportada pela OLT: {full_response.strip()}")
            
        else:
            # Timeout - analisa o que temos
            response_clean = full_response.strip()
            if response_clean:
                print(f"[WARN] Thread-{thread_id}: Timeout, mas OLT respondeu: {response_clean[:100]}...")
                # Se não tem erro, assume sucesso
                if not re.search(r'error|fail', response_clean.lower()):
                    print(f"[INFO] Thread-{thread_id}: Assumindo sucesso (sem erro detectado)")
                    time.sleep(0.1)
                    shell.send('quit\n')
                    time.sleep(0.1)
                    shell.send('quit\n')
                    time.sleep(0.1)
                    shell.send('y\n')
                else:
                    raise Exception("Timeout com possível erro")
            else:
                raise Exception("Timeout sem resposta da OLT")
    except Exception as e:
        time.sleep(0.1)
        shell.send('quit\n')
        time.sleep(0.1)
        shell.send('quit\n')
        time.sleep(0.1)
        shell.send('y\n')
        write_log(f"[ERROR] Thread-{thread_id}: Erro ao salvar configuração na OLT {host}: {e}")

def delete_onu(shell, host, thread_id):
    """
    Thread-safe version
    """
    list_remove_onus = get_onus_offlines(shell, host, thread_id)
    #print(f"DEBUG: \n{list_remove_onus}\n")
    total_deletadas = len(list_remove_onus)

    if total_deletadas == 0:
        log = f"[INFO] Thread-{thread_id}: Nenhuma ONU a ser deletada na OLT {host}."
        write_log(log)
        return

    now = datetime.now()
    date_time = now.strftime("%Y/%m/%d, %H:%M:%S")
    
    print(f"[INFO] Thread-{thread_id}: Deletando {total_deletadas} ONUs offline a {qtd_dias} dia(s) da OLT {host}...\n")

    for onu in list_remove_onus:
        result_sn, service_port_id, chassi_id, slot_id, pon_id, onu_id = onu
        
        
        shell.send(f"undo service-port {service_port_id}\n")
        time.sleep(0.5)
        shell.send(f"interface gpon {chassi_id}/{slot_id}\n")
        time.sleep(0.5)
        shell.send(f"ont delete {pon_id} {onu_id}\n")
        time.sleep(0.5)
        shell.send("quit\n")
        
        log_msg = f"[INFO] Thread-{thread_id}: CHASSI {chassi_id} SLOT {slot_id} PON {pon_id} ONU {onu_id} SERIAL {result_sn} SERVICE-PORT {service_port_id} DELETADO EM {date_time}."
        print(log_msg)
        
    # Adiciona ao contador global
    adicionar_onus_deletadas(total_deletadas)
    
    # Total de ONUs deletadas
    write_log(f"[INFO] Thread-{thread_id}: TOTAL DE {total_deletadas} ONUs DELETADAS NA OLT {host}")
    
    save_olt(shell, host, thread_id)

# Função principal para processar uma OLT (executada em thread)
def processar_olt(host, thread_id):
    """
    Função principal que processa uma OLT específica
    Executada em thread separada
    """
    try:
        print(f"[INFO] Thread-{thread_id}: Iniciando processamento da OLT {host}")
        
        # Estabelece conexão
        conn, shell = ssh(host)
        
        try:
            delete_onu(shell, host, thread_id)
            
        except Exception as e:
            write_log(f"[ERRO] Thread-{thread_id}: Falha ao processar OLT {host}: {e}")
        finally:
            conn.close()
            print(f"[INFO] Thread-{thread_id}: Conexão fechada com {host}")
            
        return f"Thread-{thread_id}: OLT {host} processada com sucesso"
        
    except Exception as e:
        write_log(f"[ERRO] Thread-{thread_id}: Falha ao conectar OLT {host}: {e}")
        return f"Thread-{thread_id}: Erro ao processar OLT {host}: {e}"

# -------------------------
# Script principal com multithreading
# -------------------------
if __name__ == "__main__":
    inicio_global = registrar_inicio_rotina()
    
    # Lê lista de equipamentos do CSV se necessário
    try:
        # Lista de OLTs
        #equipamentos = ['10.144.0.10']  # LAB
        
        equipamentos = ['10.146.61.3'] # Adicione mais IPs aqui 
        #df_hosts = pd.read_csv("olts_huawei.csv")
        # Descomente a linha abaixo se quiser usar o CSV
        #equipamentos = df_hosts["host"].tolist()
    except:
        write_log("[WARN] Não foi possível carregar CSV, usando lista hardcoded")
    
    print(f"[INFO] Processando {len(equipamentos)} OLTs com máximo de {MAX_THREADS} threads\n")
    
    # Executa processamento multithread
    resultados = []
    
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        # Submete todas as tarefas
        future_to_host = {}
        
        for i, host in enumerate(equipamentos):
            # Adiciona delay entre submissions para evitar sobrecarga
            if i > 0:
                time.sleep(THREAD_DELAY)
            
            future = executor.submit(processar_olt, host, i+1)
            future_to_host[future] = host
        
        # Coleta resultados conforme completam
        for future in as_completed(future_to_host):
            host = future_to_host[future]
            try:
                resultado = future.result()
                resultados.append(resultado)
                print(f"[SUCCESS] {resultado}")
            except Exception as e:
                write_log(f"[ERRO] Falha na thread para OLT {host}: {e}")
    
    # Limpa arquivos temporários
    try:
        for i in range(len(equipamentos)):
            temp_file = f'{path_01_base}_{i+1}.txt'
            if os.path.exists(temp_file):
                os.remove(temp_file)
    except:
        pass
    
    write_log(f"[INFO] Resumo: {len(resultados)} OLTs processadas")
    for resultado in resultados:
        write_log(f"[INFO] {resultado}")
    
    salvar_total_no_log()
    
    # Final global
    rotina_finalizada(inicio_global, len(equipamentos))