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

# Configurações
path_01_base = "onus_state"  # Será usado como prefixo para cada thread
path_02 = "log_zte.txt"
qtd_dias = 31

# Variáveis que controlam os contadores globais
total_onus_deletadas = 0
total_onus_nunca_online = 0

# Configurações de threading
MAX_THREADS = 90  # Número máximo de threads simultâneas
THREAD_DELAY = 5  # Delay entre inicialização de threads (segundos)

# Lista de OLTs
#equipamentos = ['']  # lab
#equipamentos = ['']  # Adicione todas as OLTs que precisa processar

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
        
def adicionar_onus_nunca_online(quantidade):
    """
    Adiciona ONUs nunca online ao contador global
    """
    global total_onus_nunca_online
    with log_lock:
        total_onus_nunca_online += quantidade

def obter_total_onus_deletadas():
    """
    Retorna o total geral de ONUs deletadas
    """
    with log_lock:
        return total_onus_deletadas

def obter_total_onus_nunca_online():
    """
    Retorna o total geral de ONUs nunca online
    """
    with log_lock:
        return total_onus_nunca_online

def salvar_total_no_log():
    """
    Salva o total final de ONUs deletadas e nunca online no log
    """
    total_deletadas = obter_total_onus_deletadas()
    total_nunca_online = obter_total_onus_nunca_online()
    
    write_log("\n" + "="*50)
    write_log(f"TOTAL GERAL DE ONUs DELETADAS: {total_deletadas}")
    write_log(f"SENDO {total_nunca_online} ONUs NUNCA ONLINE")
    write_log("="*50)

# Função thread-safe para escrita de logs
def write_log(message, include_print=True):
    """
    Escreve log de forma thread-safe
    """
    if include_print:
        print(message)
    
    with log_lock:
        with open(path_02, "a", encoding="utf-8") as log_file:
            log_file.write(f"{message}\n")

# Coletar hora Atual da OLT
def olt_date(shell):
    shell.send('show clock\n')
    time.sleep(1)
    result = read_output(shell)

    lines = result.splitlines()
    for line in lines:
        if "BRT" in line and len(line.split()) >= 6:
            # exemplo de line: "10:14:27 BRT Tue Aug 19 2025"
            date_str = " ".join(line.split()[-3:])  # "Aug 19 2025"
            olt_date = datetime.strptime(date_str, "%b %d %Y").date()
            print(f"[INFO] Data atual da OLT:{olt_date}\n")
            return olt_date

    raise ValueError("Não foi possível extrair a data do show clock")

# Função para ler toda a saída do shell sem cortar
def read_output(shell, buffer_size=65535, wait=1):
    output = ""
    time.sleep(wait)  # espera a saída ser gerada
    while shell.recv_ready():
        data = shell.recv(buffer_size).decode("utf-8", errors="ignore")
        output += data
        time.sleep(0.2)  # dá tempo para vir mais dados
    return output

# Função para coletar status das ONUs (thread-safe)
def get_onus_state(shell, thread_id):
    shell.send('terminal length 0\n')
    shell.send('show gpon onu state\n')
    time.sleep(15)
    result = read_output(shell)

    # Cada thread usa seu próprio arquivo
    path_01 = f'{path_01_base}_{thread_id}.txt'
    
    with open(path_01, 'w', encoding="utf-8") as data:
        data.write(result)

    print(f"[INFO] Thread-{thread_id}: ONU state salvo em {path_01}")
    return result, path_01

# Função para obter ONUs offline (thread-safe)
def get_onus_offlines(shell, host, thread_id):
    # coleta o estado das ONUs já existente
    result, path_01 = get_onus_state(shell, thread_id)

    # pega a data atual da OLT
    data_olt = olt_date(shell)

    list_onus_offlines = []
    list_onus_delete = []
    
    contador_nunca_online = 0  # Contador local por OLT

    # lê diretamente do arquivo salvo pelo get_onus_state
    try:
        with open(path_01, 'r', encoding="utf-8") as file_onu_state:
            for line in file_onu_state:
                # Pega ONU que não está working e está enable
                if ('working' not in line) and ('enable' in line):
                    onu_index = f"gpon_onu-{line.split()[0]}"
                    list_onus_offlines.append(onu_index)

        # Remove arquivo temporário
        try:
            os.remove(path_01)
        except:
            pass
            
    except FileNotFoundError:
        write_log(f"[ERRO] Thread-{thread_id}: Arquivo {path_01} não encontrado")
        return []

    if len(list_onus_offlines) >= 1:
        print(f'[INFO] Thread-{thread_id}: Encontradas {len(list_onus_offlines)} ONUs offline. Verificando histórico...\n')

    for index in list_onus_offlines:
        shell.send(f'show gpon onu detail-info {index}\n')
        time.sleep(5)
        result = read_output(shell)

        # --- Extrair serial ---
        serial_number = None
        m_sn = re.search(r'^\s*Serial number:\s*(\S+)', result, re.MULTILINE)
        if m_sn:
            serial_number = m_sn.group(1)

        # --- Detectar ONUs que nunca subiram ou extrair histórico ---
        # Checa flag textual "onu never online"
        if re.search(r'\bonu never online\b', result, re.IGNORECASE):
            contador_nunca_online += 1
            print(f"[INFO] Thread-{thread_id}: ONU {index[9:]} nunca online (flag no detail-info). Incluindo na lista de deleção.\n")
            list_onus_delete.append((index, serial_number))
            continue

        # Regex que captura: índice, AuthPass date/time, Offline date/time, causa (se houver)
        hist_re = re.compile(
            r'^\s*\d+\s+'                      # índice
            r'(\d{4}-\d{2}-\d{2})\s+'          # auth date
            r'(\d{2}:\d{2}:\d{2})\s+'          # auth time
            r'(\d{4}-\d{2}-\d{2})\s+'          # offline date
            r'(\d{2}:\d{2}:\d{2})\s*'          # offline time
            r'(.*\S)?\s*$',                    # cause (opcional)
            re.MULTILINE
        )

        entries = hist_re.findall(result)  # lista de tuplas (auth_date, auth_time, off_date, off_time, cause)


        # Se TODAS as AuthPass dates forem 0000-00-00 => nunca subiu
        if all(entry[0] == "0000-00-00" for entry in entries):
            contador_nunca_online += 1
            print(f"[INFO] Thread-{thread_id}: ONU {index[9:]} possui apenas AuthPass 0000-00-00. Incluindo na lista de deleção.\n")
            list_onus_delete.append((index, serial_number))
            continue

        # Procura a última entrada com OfflineDate válido != 0000-00-00
        offline_dt = None
        offline_cause = None
        for auth_date, auth_time, off_date, off_time, cause in reversed(entries):
            if off_date and off_date != "0000-00-00":
                try:
                    offline_dt = datetime.strptime(f"{off_date} {off_time}", "%Y-%m-%d %H:%M:%S")
                    offline_cause = (cause or '').strip()
                    break
                except Exception:
                    continue

        # Se não achou OfflineTime válido -> pula
        if not offline_dt:
            print(f"[INFO] Thread-{thread_id}: ONU {index[9:]} sem OfflineTime válido. Ignorando para deleção automática.\n")
            continue

        # Calcula dias OFF e decide inclusão na lista de deleção
        try:
            days_off = (data_olt - offline_dt.date()).days
            if days_off >= qtd_dias:
                list_onus_delete.append((index, serial_number))
            print(f"[INFO] Thread-{thread_id}: ONU {index[9:]} ESTA A {days_off} DIA(S) OFFLINE "
                  f"(OfflineTime {offline_dt.date()}, Cause: {offline_cause or 'N/A'})\n")
        except Exception as e:
            write_log(f"[WARN] Thread-{thread_id}: Não foi possível processar {index}: {e}")

    # Adiciona ao contador global
    adicionar_onus_nunca_online(contador_nunca_online)
    
    # Log do total por OLT
    write_log(f"[INFO] Thread-{thread_id}: OLT {host} - {contador_nunca_online} ONUs nunca online")
    
    
    return list_onus_delete


# Função para registrar o inicio da rotina
def registrar_inicio_rotina():
    inicio = datetime.now()
    message = f'ROTINA ZTE MULTITHREADED INICIADA...\nInício: {inicio.strftime("%Y/%m/%d %H:%M:%S")}\nMax Threads: {MAX_THREADS}\n'
    write_log(message)
    return inicio

# Função para registrar o fim da rotina
def rotina_finalizada(start_time, total_hosts):
    fim = datetime.now()
    duracao = fim - start_time
    duracao_str = str(duracao).split('.')[0]
    
    message = f"\nTérmino: {fim.strftime('%Y/%m/%d %H:%M:%S')}\nDuração: {duracao_str}\nTotal de OLTs processadas: {total_hosts}\nROTINA ZTE FINALIZADA\n"
    write_log(message)

# Função para salvar configuração da OLT (thread-safe)
def save_olt(shell, host, thread_id):
    try:
        print(f"[INFO] Thread-{thread_id}: Salvando configuração na OLT {host}...")
        # Limpa buffer antes
        while shell.recv_ready():
            shell.recv(1024)
            
        # Envia comando save
        shell.send('exit\n')
        time.sleep(0.1)
        shell.send('write\n')
        
        # Coleta resposta com timeout inteligente
        full_response = ""
        max_wait = 90
        waited = 0
        
        while waited < max_wait:
            time.sleep(1)
            waited += 1
        
            if shell.recv_ready():
                data = shell.recv(4096).decode("utf-8", errors="ignore") 
                full_response += data
                
            # Verifica sucesso ZTE (procura por [OK] case-insensitive)
            if re.search(r'\[OK\]', full_response, re.IGNORECASE):
                print(f"[SUCCESS] Thread-{thread_id}: Configuração salva na OLT {host}")
                #write_log(f"[SUCCESS] Thread-{thread_id}: Configuração salva na OLT {host}")
                break
            
            # Verifica erro ZTE
            if re.search(r'error|fail|invalid|denied', full_response, re.IGNORECASE):
                shell.send('exit\n')
                raise Exception(f"Erro da ZTE: {full_response.strip()}")
            
        else:
            # Timeout - verifica se tem [OK] na resposta
            if re.search(r'\[OK\]', full_response, re.IGNORECASE):
                print(f"[SUCCESS] Thread-{thread_id}: Configuração salva (detectado [OK])")
                #write_log(f"[SUCCESS] Thread-{thread_id}: Configuração salva na OLT {host}")
            elif full_response.strip() and not re.search(r'error|fail', full_response, re.IGNORECASE):
                print(f"[INFO] Thread-{thread_id}: Assumindo sucesso - resposta: {full_response.strip()}")
            else:
                raise Exception("Timeout sem confirmação de salvamento")
                
    except Exception as e:
        write_log(f"[ERROR] Thread-{thread_id}: Erro ao salvar configuração na OLT {host}: {e}")

# Função para deletar ONUs offline (thread-safe)
def delete_onu(shell, host, thread_id):
    print(f"[INFO] Thread-{thread_id}: Obtendo ONUs offline da OLT {host}...")
    onu_delete = get_onus_offlines(shell, host, thread_id)

    if not onu_delete:
        log = f"[INFO] Thread-{thread_id}: Nenhuma ONU a ser deletada na OLT {host}."
        #write_log(log)
        print(log)
        return

    total_deletadas = len(onu_delete)
    
    print(f"[INFO] Thread-{thread_id}: Deletando {total_deletadas } ONUs offline a {qtd_dias} dia(s) da OLT {host}...\n")
    conf_t = f'configure terminal\n'
    shell.send(conf_t)
    time.sleep(0.5)
    

    for index, serial_number in onu_delete:
        try:
            result = index.replace("gpon_onu-", "").replace(":", " ").split()
            chassi_slot_pon = result[0].split("/")
            chassi_id = chassi_slot_pon[0]
            slot_id = chassi_slot_pon[1]
            pon_id = chassi_slot_pon[2]
            onu_id = result[1]

            interface_gpon = f'interface gpon_olt-{chassi_id}/{slot_id}/{pon_id}\n'
            remove_onu = f'no onu {onu_id}\n'
            comand_exit = f'exit\n'

            shell.send(interface_gpon)
            time.sleep(0.5)
            shell.send(remove_onu)
            time.sleep(0.5)
            shell.send(comand_exit)
            time.sleep(0.5)

            now = datetime.now()
            date_time = now.strftime("%Y/%m/%d, %H:%M:%S")
            log = f"[INFO] Thread-{thread_id}: CHASSI {chassi_id} SLOT {slot_id} PON {pon_id} ONU {onu_id} SERIAL {serial_number} DELETADO EM {date_time}."
            #write_log(log)
            print(log)
            
            # Incrementa o contador
            total_deletadas += 1
            
        except Exception as e:
            log = f"[ERRO] Thread-{thread_id}: Falha ao deletar ONU {index} na OLT {host}: {e}"
            write_log(log)
            print(log)
            
    # Adiciona ao contador global
    adicionar_onus_deletadas(total_deletadas)
    
    # Log final: total de ONUs deletadas
    write_log(f"[INFO] Thread-{thread_id}: TOTAL DE {total_deletadas} ONUs DELETADAS NA OLT {host}")

    save_olt(shell, host, thread_id)

# Função principal para processar uma OLT (executada em thread)
def processar_olt(host, thread_id):
    """
    Função principal que processa uma OLT específica
    Executada em thread separada
    """
    try:
        print(f"[INFO] Thread-{thread_id}: Iniciando processamento da OLT {host}\n")
        
        # Estabelece conexão
        conn, shell = ssh(host)
        
        try:
            # Processa deleção de ONUs
            delete_onu(shell, host, thread_id)
            
        except Exception as e:
            write_log(f"[ERRO] Thread-{thread_id}: Falha ao processar OLT {host}: {e}")
            return f"Thread-{thread_id}: Erro ao processar OLT {host}: {e}"
        finally:
            conn.close()
            print(f"[INFO] Thread-{thread_id}: Conexão fechada com {host}\n")
            
        return f"Thread-{thread_id}: OLT {host} processada com sucesso"
        
    except Exception as e:
        write_log(f"[ERRO] Thread-{thread_id}: Falha ao conectar OLT {host}: {e}")
        return f"Thread-{thread_id}: Erro ao conectar OLT {host}: {e}"

# -------------------------
# Script principal com multithreading
# -------------------------
if __name__ == "__main__":
    inicio_global = registrar_inicio_rotina()
    
    # Lê lista de equipamentos do CSV se necessário
    try:
        df_hosts = pd.read_csv("olts_zte.csv")
        # Descomente a linha abaixo se quiser usar o CSV
        equipamentos = df_hosts["host"].tolist()
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
        
    # Salva totais finais no log
    salvar_total_no_log()
    
    # Final global
    rotina_finalizada(inicio_global, len(equipamentos))