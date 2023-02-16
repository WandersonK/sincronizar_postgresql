import csv
import psycopg2 as pg

tabelas_db = ['atribuicao', 'grupo', 'pessoa', 'pessoa_grupo', 'grupo_atribuicao']  # As tabelas do banco
dir_raiz = 'CAMINHO PARA SALVAR O CSV'

def ler_arquivo(nome_arquivo):
    arq = open(f'{dir_raiz}{nome_arquivo}.csv', 'r')
    leitura = csv.DictReader(arq)
    return leitura

def conecta_db_principal():
    conn_principal = pg.connect(host='HOST OU IP', 
                            port='5432', 
                            dbname='NOME DO BANCO', 
                            user='USUARIO', 
                            password='SENHA')
    return conn_principal

def conecta_db_secundario():
    conn_secundario = pg.connect(host='HOST OU IP', 
                            port='5432', 
                            dbname='NOME DO BANCO', 
                            user='USUARIO', 
                            password='SENHA')
    return conn_secundario


conn_primario = conecta_db_principal()
cursor_primario = conn_primario.cursor()

for tabela in tabelas_db:
    
    sql = f"COPY (SELECT * FROM public.{tabela}) TO STDOUT WITH CSV HEADER ENCODING 'UTF8' DELIMITER ','"
    
    with open(f'{dir_raiz}{tabela}.csv', 'w') as arq:
        cursor_primario.copy_expert(sql, arq)

conn_primario.close()



conn_secundario = conecta_db_secundario()
cursor_secundario = conn_secundario.cursor()

for tabela in tabelas_db:
    
    leitura = ler_arquivo(tabela)
    
    cursor_secundario.execute(f"SELECT id FROM public.{tabela};")
    result_selec_tabela = cursor_secundario.fetchall()
    
    for linha in leitura:
        dados = []
        tags = []
        
        for label in leitura.fieldnames:
            if linha[label] != '' and linha[label] != ' ':
                dados.append('$$' + linha[label] + '$$')
                tags.append(label)
                
                if (int(linha['id']),) in result_selec_tabela:
                    cursor_secundario.execute(f"UPDATE public.{tabela} SET {label} = $${linha[label]}$$ WHERE id = {linha['id']}")
                    conn_secundario.commit()
                    
            else:
                dados.append("null")
                tags.append(label)
                
                if (int(linha['id']),) in result_selec_tabela:
                    cursor_secundario.execute(f"UPDATE public.{tabela} SET {label} = null WHERE id = {linha['id']}")
                    conn_secundario.commit()
        
        
        conjunto_dados = str(tuple(dados)).replace("'", "")
        conjunto_tags = str(tuple(tags)).replace("'","")
        
        if (int(linha['id']),) not in result_selec_tabela:
            cursor_secundario.execute(f"INSERT INTO public.{tabela} {conjunto_tags} VALUES {conjunto_dados}")
            conn_secundario.commit()
        
        print(conjunto_dados)


conn_secundario.close()

