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
path_02 = 'log_fh.txt'
path_03 = 'onu_last_on_and_off_time.txt'
path_04_base = 'slots_ativos'  # Será usado como prefixo para cada thread
qtd_dias = 45

# Variavel que controla o total de onus deletadas
total_onus_deletadas = 0
# Nova variável para controlar ONUs com last_off_time == '0000-00-00' (implementação do Huawei)
total_onus_sem_last_off_time = 0

# Configurações de threading
MAX_THREADS = 50  # Número máximo de threads simultâneas
THREAD_DELAY = 4  # Delay entre inicialização de threads (segundos)

# Lista de OLTs para validação ou uso unico
equipamentos = ['']  # Adicione mais IPs aqui

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
        
def adicionar_onus_sem_last_off_time(quantidade):
    """
    Adiciona ONUs sem last off time ao contador global
    """
    global total_onus_sem_last_off_time
    with log_lock:
        total_onus_sem_last_off_time += quantidade

def obter_total_onus_deletadas():
    """
    Retorna o total geral de ONUs deletadas
    """
    with log_lock:
        return total_onus_deletadas

def obter_total_onus_sem_last_off_time():
    """
    Retorna o total geral de ONUs sem last off time
    """
    with log_lock:
        return total_onus_sem_last_off_time

def salvar_total_no_log():
    """
    Salva o total final de ONUs deletadas e sem last off time no log
    """
    total_deletadas = obter_total_onus_deletadas()
    total_onus_sem_last_off_time = obter_total_onus_sem_last_off_time()
    
    write_log("\n" + "="*50)
    write_log(f"TOTAL GERAL DE ONUs DELETADAS: {total_deletadas}")
    write_log(f"SENDO {total_onus_sem_last_off_time} ONUs SEM LAST OFF TIME (0000-00-00)")
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

# Função para ler toda a saída do shell sem cortar
def read_output(shell, buffer_size=65535, wait=1):
    output = ""
    time.sleep(wait)
    while shell.recv_ready():
        data = shell.recv(buffer_size).decode("utf-8", errors="ignore")
        output += data
        time.sleep(0.2)
    return output

# Função para registrar o inicio da rotina
def registrar_inicio_rotina():
    inicio = datetime.now()
    message = f'ROTINA FIBERHOME MULTITHREADED INICIADA...\nInício: {inicio.strftime("%Y/%m/%d %H:%M:%S")}\nMax Threads: {MAX_THREADS}\n'
    write_log(message)
    return inicio

# Função para registrar o fim da rotina
def rotina_finalizada(start_time, total_hosts):
    fim = datetime.now()
    duracao = fim - start_time
    duracao_str = str(duracao).split('.')[0]
    
    message = f"\nTérmino: {fim.strftime('%Y/%m/%d %H:%M:%S')}\nDuração: {duracao_str}\nTotal de OLTs processadas: {total_hosts}\nROTINA FIBERHOME FINALIZADA\n"
    write_log(message)

# Função para obter versão da OLT
def get_version_olt(shell):
    shell.send("cd service\n")
    time.sleep(0.5)
    shell.send("terminal length 0\n")
    time.sleep(0.5)
    shell.send("cd ..\n")
    time.sleep(0.5)
    shell.send("show version\n")
    time.sleep(5)
    result = read_output(shell)
    
    versao = "DESCONHECIDO"
    
    for line in result.splitlines():
        if "HSWA" in line and "RP1000" in line:
            versao = "RP1000"
        elif "HSUA" in line and "RP1000" in line:
            versao = "RP1000"
        elif "HSUB" in line and "RP1000" in line:
            versao = "RP1000"
        elif "HSUC" in line and "RP1000" in line:
            versao = "RP1000"
        elif "HSUA" in line and "RP1200" in line:
            versao = "RP1200"
        elif "HSWA" in line and "RP1200" in line:
            versao = "RP1200"
        elif "HSUB" in line and "RP1200" in line:
            versao = "RP1200"
        elif "HSUC" in line and "RP1200" in line:
            versao = "RP1200"

    return versao

# Função para coletar hora da OLT
def olt_date(shell):
    shell.send('show time\n')
    time.sleep(5)
    result = read_output(shell)
    
    for line in result.splitlines():
        # Extrai a data usando regex
        # Procura por padrão YYYY-MM-DD no formato da saída
        if "Current Date" in line:
            date_match = re.search(r'(\d{4}-\d{2}-\d{2})', line)
            if date_match:
                date_str = date_match.group(1)
                # Converte string para objeto datetime.date
                return datetime.strptime(date_str, "%Y-%m-%d").date()
            else:
                raise ValueError(f"Formato de data não reconhecido na linha: {line}")

# Função para obter slots ativos da OLT (thread-safe)
def get_slot(shell, thread_id):
    """
    Coleta informações dos slots da OLT usando comando 'show'
    Thread-safe version com arquivo específico por thread
    """
    shell.send('show\n')
    time.sleep(10)
    result = read_output(shell)
    
    # Cada thread usa seu próprio arquivo
    path_04 = f'{path_04_base}_{thread_id}.txt'
    
    with open(path_04, 'w', encoding='utf-8') as file:
        file.write(result)
    print(f"[INFO] Dados dos slots salvos em {path_04}\n")
    
    return path_04

# Função para extrair slots habilitados e quantidade de PONs
def get_slot_enable(shell, thread_id):
    """
    Versão thread-safe da função get_slot_enable
    """
    path_04 = get_slot(shell, thread_id)
    
    list_slot_enables = []
    list_pon_enable = []
    
    try:
        with open(path_04, 'r', encoding='utf-8') as file:
            data = file.readlines()
            
        for line in data:
            line = line.strip()
            
            if '---' in line or not line:
                continue
                
            elif ('GCOB' in line) and ('MATCH' in line):
                parts = line.split()
                if parts:
                    slot = parts[0]
                    pon = 16
                    list_slot_enables.append(slot)
                    list_pon_enable.append(pon)
       
                    
            elif ('GC8B' in line) and ('MATCH' in line):
                parts = line.split()
                if parts:
                    slot = parts[0]
                    pon = 8
                    list_slot_enables.append(slot)
                    list_pon_enable.append(pon)
  
        
        # Remove arquivo temporário
        try:
            os.remove(path_04)
        except:
            pass
                    
    except FileNotFoundError:
        write_log(f"[ERRO] Arquivo {path_04} não encontrado ao processar slots")
    except Exception as e:
        write_log(f"[ERRO] Erro ao processar slots: {e}")

    return list_slot_enables, list_pon_enable

# Função para processar slots de uma OLT específica
def processar_slots_olt(shell, host, thread_id):
    """
    Thread-safe version
    """
    try:
        #write_log(f"[INFO] Thread-{thread_id}: Processando slots da OLT {host}...")
        print(f"[INFO] Thread-{thread_id}: Processando slots da OLT {host}...")
        
        slots_habilitados, pons_por_slot = get_slot_enable(shell, thread_id)
        
        #write_log(f"[INFO] Thread-{thread_id}: OLT {host} - {len(slots_habilitados)} slot(s) habilitado(s)")
        print(f"[INFO] Thread-{thread_id}: OLT {host} - {len(slots_habilitados)} slot(s) habilitado(s)\n")
        
        return slots_habilitados, pons_por_slot
        
    except Exception as e:
        write_log(f"[ERRO] Thread-{thread_id}: Falha ao processar slots da OLT {host}: {e}")
        return [], []

# Função para coletar ONUs autorizadas que estão DOWN
def get_onus_down(shell, slots_habilitados, pons_por_slot, host, thread_id):
    """
    Thread-safe version
    """
    onus_down = []
    
    try:
        shell.send('cd onu\n')
        time.sleep(1)
        
        #write_log(f"[INFO] Thread-{thread_id}: Coletando ONUs DOWN de {len(slots_habilitados)} slot(s) da OLT {host}...")
        print(f"[INFO] Thread-{thread_id}: Coletando ONUs DOWN de {len(slots_habilitados)} slot(s) da OLT {host}...\n")
        
        for i, slot in enumerate(slots_habilitados):
            max_pons = pons_por_slot[i]
            
            for pon in range(1, max_pons + 1):
                print(f"[INFO] Thread-{thread_id}: Verificando slot {slot}, PON {pon}...\n")
                command = f'show authorization slot {slot} pon {pon}\n'
                shell.send(command)
                time.sleep(5)
                result = read_output(shell)
                
                onus_down.extend(parse_authorization_output(result, slot, pon))
        
        time.sleep(3)
        
        if len(onus_down) >= 1:
            #write_log(f"[INFO] Thread-{thread_id}: OLT {host} - Encontradas {len(onus_down)} ONUs offline")
            print(f"[INFO] Thread-{thread_id}: OLT {host} - Encontradas {len(onus_down)} ONUs offline\n")
            
        return onus_down
        
    except Exception as e:
        write_log(f"[ERRO] Thread-{thread_id}: Erro ao coletar ONUs DOWN da OLT {host}: {e}")
        return []

def parse_authorization_output(output, slot, pon):
    """
    Analisa o output do comando show authorization e extrai ONUs DOWN
    """
    onus_down = []
    
    lines = output.splitlines()
    for line in lines:
        if 'dn' in line and len(line.split()) >= 6:
            parts = line.split()
            try:
                if (parts[0] == str(slot) and 
                    parts[1] == str(pon) and 
                    parts[6] == 'dn'):
                    
                    onu_info = {
                        'slot': parts[0],
                        'pon': parts[1], 
                        'onu': parts[2],
                        'onu_type': parts[3],
                        'phy_id': parts[7] if len(parts) > 7 else '',
                    }
                    onus_down.append(onu_info)
                    
            except (IndexError, ValueError):
                continue
                
    return onus_down

def check_onu_offline_time(shell, onu_info, data_atual_olt,thread_id, contador_sem_last_off):
    """
    Verifica há quantos dias uma ONU específica está offline
    """
    try:
        slot = onu_info['slot']
        pon = onu_info['pon'] 
        onu = onu_info['onu']
        
        command = f'show onu_last_on_and_off_time slot {slot} pon {pon} onu {onu}\n'
        shell.send(command)
        time.sleep(3)
        result = read_output(shell)
        
        for line in result.splitlines():
            if 'Last Off Time' in line and 'Last On Time' in line:
                off_match = re.search(r'Last Off Time = (\d{4}-\d{2}-\d{2})', line)
                on_match = re.search(r'Last On Time = (\d{4}-\d{2}-\d{2})', line)
                
                if off_match:
                    last_off_time = off_match.group(1)
                    
                    if last_off_time == '0000-00-00':
                        contador_sem_last_off[0] += 1
                        print(f"[INFO] Thread-{thread_id}: ONU {slot}/{pon}:{onu} SEM LAST OFF TIME (0000-00-00)")
                        
                        if on_match and on_match.group(1) == '0000-00-00':
                            onu_info['dias_offline'] = 1000
                            onu_info['last_off_time'] = last_off_time
                            onu_info['status_offline'] = 'fantasma'
                            return onu_info
                            
                        else:
                            return None
                    
                    try:
                        data_off = datetime.strptime(last_off_time, '%Y-%m-%d').date()
                        dias_offline = (data_atual_olt - data_off).days
                        
                        onu_info['dias_offline'] = dias_offline
                        onu_info['last_off_time'] = last_off_time
                        onu_info['status_offline'] = 'normal'
                        return onu_info
                        
                    except ValueError:
                        return None
        
        return None
        
    except Exception as e:
        return None

def get_onus_for_deletion(shell, slots_habilitados, pons_por_slot, dias_limite, host, thread_id):
    """
    Thread-safe version
    """
    try:
        data_atual = olt_date(shell)
        #write_log(f"[INFO] Thread-{thread_id}: OLT {host} - Data atual: {data_atual}")
        print(f"[INFO] Thread-{thread_id}: OLT {host} - Data atual: {data_atual}\n")
        
        onus_down = get_onus_down(shell, slots_habilitados, pons_por_slot, host, thread_id)
        #print(f"[DEBUG]:\n{onus_down}.\n")
        
        if not onus_down:
            write_log(f"[INFO] Nenhuma ONU Offline na OLT {host}.\n")
            #print(f"[INFO] Nenhuma ONU Offline na OLT {host}.\n")
            return []
        
    
        
        onus_para_deletar = []
        contador_sem_last_off_time= [0]  # Contador local por OLT
        
    
        
        for onu_info in onus_down:
            onu_com_tempo = check_onu_offline_time(shell, onu_info, data_atual, thread_id,contador_sem_last_off_time)
            
            if onu_com_tempo:
                dias_offline = onu_com_tempo['dias_offline']
                
                if dias_offline >= dias_limite:
                    onus_para_deletar.append(onu_com_tempo)  
                #write_log(f"[INFO] Thread-{thread_id}: OLT {host} - ONU {onu_com_tempo['slot']}/{onu_com_tempo['pon']}:{onu_com_tempo['onu']} está há {dias_offline} dia(s) offline")
                print(f"[INFO] Thread-{thread_id}: OLT {host} - ONU {onu_com_tempo['slot']}/{onu_com_tempo['pon']}:{onu_com_tempo['onu']} está há {dias_offline} dia(s) offline (último last_off_time {onu_com_tempo['last_off_time']})\n")
        
        # Adiciona ao contador global
        adicionar_onus_sem_last_off_time(contador_sem_last_off_time[0])
        
        write_log(f"[INFO] Thread-{thread_id}: OLT {host} - {contador_sem_last_off_time[0]} ONUs sem Last Off Time (0000-00-00)")
        
        #print(f"[DEBUG]:\n{onus_para_deletar}.\n")
        return onus_para_deletar
        
    except Exception as e:
        write_log(f"[ERRO] Thread-{thread_id}: Erro na identificação de ONUs para deleção da OLT {host}: {e}")
        return []

def save_olt(shell, host, thread_id):
    """
    Thread-safe version
    """
    try:
        #write_log(f"[INFO] Thread-{thread_id}: Salvando configuração na OLT {host}...")
        print(f"[INFO] Thread-{thread_id}: Salvando configuração na OLT {host}...\n")
        
        while shell.recv_ready():
            shell.recv(1024)
            
        shell.send('save\n')
        
        full_response = ""
        max_wait = 90
        waited = 0
        
        while waited < max_wait:
            time.sleep(1)
            waited += 1
        
            if shell.recv_ready():
                data = shell.recv(4096).decode("utf-8", errors="ignore") 
                full_response += data
                
            if re.search(r'success|complete|saved.*successfully', full_response.lower()):
                #write_log(f"[SUCCESS] Thread-{thread_id}: Configuração salva na OLT {host}")
                print(f"[SUCCESS] Thread-{thread_id}: Configuração salva na OLT {host}\n")
                break
            
            if re.search(r'error|failed|cannot.*save', full_response.lower()):
                raise Exception(f"Falha reportada pela OLT: {full_response.strip()}")
            
        else:
            response_clean = full_response.strip()
            if response_clean:
                if not re.search(r'error|fail', response_clean.lower()):
                    #write_log(f"[INFO] Thread-{thread_id}: Assumindo sucesso (sem erro detectado) - OLT {host}")
                    print(f"[INFO] Thread-{thread_id}: Assumindo sucesso (sem erro detectado) - OLT {host}\n")
                else:
                    raise Exception("Timeout com possível erro")
            else:
                raise Exception("Timeout sem resposta da OLT")
                
    except Exception as e:
        write_log(f"[ERROR] Thread-{thread_id}: Erro ao salvar configuração na OLT {host}: {e}")

def delete_onus_from_whitelist(shell, onus_para_deletar, host, thread_id):
    """
    Thread-safe version
    """
    try:
        
        total_deletadas = len(onus_para_deletar)
        
        if total_deletadas == 0:
            log = f"[INFO] Thread-{thread_id}: Nenhuma ONU a ser deletada na OLT {host}."
            write_log(log)
            return
        
        
        
        
        for onu in onus_para_deletar:
            try:
                slot = onu['slot']
                pon = onu['pon']
                onu_id = onu['onu']
                phy_id = onu['phy_id']
                dias_offline = onu['dias_offline']
                
                command = f'set whitelist phy_addr address {phy_id} password null action delete\n'
                shell.send(command)
                time.sleep(1)
                
                now = datetime.now()
                log_msg = f"[INFO] Thread-{thread_id}: OLT {host} - SLOT {slot} PON {pon} ONU {onu_id} SERIAL {phy_id} DELETADO EM {now.strftime('%Y/%m/%d %H:%M:%S')}\n"
                #write_log(log_msg)
                print(log_msg)
                
                
                
            except Exception as e:
                write_log(f"[ERRO] Thread-{thread_id}: Erro ao deletar ONU {onu} da OLT {host}: {e}")
                continue
        
        shell.send('cd ..\n')
        time.sleep(1)
        
        #  Adiciona ao contador
        adicionar_onus_deletadas(total_deletadas)
        
        write_log(f"[INFO] Thread-{thread_id}: TOTAL DE {total_deletadas} ONUs DELETADAS NA OLT {host}")
    
        
        save_olt(shell, host, thread_id)
        
    except Exception as e:
        write_log(f"[ERRO] Thread-{thread_id}: Erro no processo de deleção da OLT {host}: {e}")

# Função principal para processar uma OLT (executada em thread)
def processar_olt(host, thread_id):
    """
    Função principal que processa uma OLT específica
    Executada em thread separada
    """
    try:
        #write_log(f"[INFO] Thread-{thread_id}: Iniciando processamento da OLT {host}")
        print(f"[INFO] Thread-{thread_id}: Iniciando processamento da OLT {host}\n")
        
        # Estabelece conexão
        conn, shell = ssh(host)
        
        try:
            # Obtém versão
            version = get_version_olt(shell)
            #write_log(f"[INFO] Thread-{thread_id}: OLT {host} - Versão: {version}")
            print(f"[INFO] Thread-{thread_id}: OLT {host} - Versão: {version}\n")
            
            # Processa slots
            slots_habilitados, pons_por_slot = processar_slots_olt(shell, host, thread_id)
            
            if slots_habilitados:
                # Identifica ONUs para deleção
                onus_para_deletar = get_onus_for_deletion(shell, slots_habilitados, pons_por_slot,qtd_dias, host, thread_id)
        
                # Executa deleções
                delete_onus_from_whitelist(shell, onus_para_deletar, host, thread_id)
                
            else:
                write_log(f"[WARN] Thread-{thread_id}: Nenhum slot ativo encontrado na OLT {host}")
        
        finally:
            conn.close()
            #write_log(f"[INFO] Thread-{thread_id}: Conexão fechada com {host}")
            print(f"[INFO] Thread-{thread_id}: Conexão fechada com {host}\n")
            
        return f"Thread-{thread_id}: OLT {host} processada com sucesso"
        
    except Exception as e:
        write_log(f"[ERRO] Thread-{thread_id}: Falha ao processar OLT {host}: {e}")
        return f"Thread-{thread_id}: Erro ao processar OLT {host}: {e}"

# -------------------------
# Script principal com multithreading
# -------------------------
if __name__ == "__main__":
    inicio_global = registrar_inicio_rotina()
    
    # Lê lista de equipamentos do CSV se necessário
    try:
        df_hosts = pd.read_csv("olts_fiberhome.csv")
        # Descomente a linha abaixo se quiser usar o CSV
        #equipamentos = df_hosts["host"].tolist()
    except:
        write_log("[WARN] Não foi possível carregar CSV, usando lista hardcoded")
    
    #write_log(f"[INFO] Processando {len(equipamentos)} OLTs com máximo de {MAX_THREADS} threads")
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
                write_log(f"[SUCCESS] {resultado}")
            except Exception as e:
                write_log(f"[ERRO] Falha na thread para OLT {host}: {e}")
    
    # Limpa arquivos temporários
    try:
        for i in range(len(equipamentos)):
            temp_file = f'{path_04_base}_{i+1}.txt'
            if os.path.exists(temp_file):
                os.remove(temp_file)
    except:
        pass
    
    write_log(f"[INFO] Resumo: {len(resultados)} OLTs processadas")
    for resultado in resultados:
        write_log(f"[INFO] {resultado}")
        
    # Salva totais finais
    salvar_total_no_log()
    
    # Final global
    rotina_finalizada(inicio_global, len(equipamentos))
    
    