# -----------------------------------------------------------------------------+
# Integração SimFretes - Envio automatizado                                    |
# Cacador, 12 de Agosto de 2021                                                |
# Luciano Carbonera                                                            |
# -----------------------------------------------------------------------------+

from libs.database import inc_database
from datetime import datetime
from unicodedata import normalize

def remover_acentos(txt):
    txt = str(txt).replace('[','').replace(']', '')
    return normalize('NFKD', txt).encode('ASCII', 'ignore').decode('ASCII')

conn = inc_database.Db('localhost', 'banco')

#busca o cnpj cadastrado para realizar envio das ocorrencias
def busca_cnpj():
    sql = (""" select * from web19_02 """)
    login = inc_database.Db.select(conn, sql)
    return login

#busca os ctes emitidos do cnpj buscado
def busca_ctes(cnpj):
    sql = (f"""
    select
        sco04.sco04_serial,
		sco04.sco04_nrodoc,
    	emitente_nf.sco01_cgccpf,
    	sco04_01.sco04_01_chave,
    	'' as sco04_preventrega,
		fnc_dtentrega(sco04.sco04_serial) as sco04_dtentrega,
		fnc_hrentrega(sco04.sco04_serial) as sco04_hrentrega,
    	case 
    		when sco31_data_agend = '1899-12-31' 
    		then null
    	else 
    		sco31_data_agend 
    	end as sco04_dtagenda,
    	'' as sco04_comprovante,
    	substring(cte006.cte002_chave,7,14) as cnpj_emissor_cte,
    	cte006.cte002_chave as chave_cte,
    	sco04.sco04_data_hora
    from sco04
    	inner join sco04_08 on
    	sco04_08.sco04_serial = sco04.sco04_serial
    	inner join sco01 on 
    	sco01.sco01_serial = sco04_08.sco04_08_pag_ser
    	inner join sco04_01 on
    	sco04_01.sco04_serial = sco04.sco04_serial
    	inner join sco01 emitente_nf on
    	emitente_nf.sco01_serial = sco04_01.sco01_codrem
    	left join sco31 on
    	sco31.sco04_serial = sco04.sco04_serial
    	inner join cte006 on
    	cte006.sco04_serial = sco04.sco04_serial

    where sco01.sco01_cgccpf = '{cnpj}'
    	and sco04.sco04_data between -5 current_time'
		and sco04_01.sco04_01_nf = 10690
		
    	and sco04.sco04_tiplan in('0', '1')
    	and sco04.sco03_filial != '99'
    	and sco04.sco04_combinado = 'N' """)
    result_cte = inc_database.Db.select(conn, sql)
    return result_cte

#busca cada ocorrência para aqele seriald e cte
def busca_ocorrencias(ocorrencias):
    global serial

    serial = str(ocorrencias[0])

    sql = (f"""
        SELECT 
            1 as ordem, 
            sco04.sco04_serial,
            case when sco59_02_de_para <> null then sco59_02_de_para else
            sco59_02_codjust end AS oco_codigo,                 
            TO_CHAR(CAST(cte003_data_hora AS DATE), 'DD/MM/YYYY') AS sco04_data, 
            TO_CHAR(CAST(cte003_data_hora AS TIMESTAMP), 'HH24:MI') AS sco04_hora, 
            'EMISSAO DO CONHECIMENTO DE TRANSPORTES' AS oco_descricao,
            cte006.cte002_chave as chave_cte
            FROM sco04
            
            INNER JOIN sco04_01 ON 
            sco04_01.sco04_serial = sco04.sco04_serial 
            INNER JOIN sco04_08 ON 
            sco04_08.sco04_serial = sco04.sco04_serial
            INNER JOIN cte001 ON
            cte001.cte001_serial = sco04_08.cte001_serial 
            INNER JOIN sco01 ON
            sco01.sco01_serial = sco04_08.sco04_08_pag_ser 
            INNER JOIN cte007 ON 
            cte007.sco04_serial = sco04.sco04_serial
            INNER join cte006 on
            cte006.sco04_serial = sco04.sco04_serial
            left join sco59_01 on 
            sco59_01.sco01_serial = sco04_08.sco04_08_pag_ser
            left join sco59_02 on 
            sco59_02.sco59_01_serial = sco59_01.sco59_01_serial and sco59_02.sco59_02_codjust = 902            
            WHERE sco04.sco04_serial ='{serial}'
            and not exists (select * from web19 where web19.sco04_serial = {serial} and web19.oco_serial = '902' and web19_01_serial = 5 )
            
            UNION 
            SELECT 
            2 as ordem,
            sco04.sco04_serial,
            case when sco59_02_de_para <> null then sco59_02_de_para else
            sco59_02_codjust end AS oco_codigo,           
            TO_CHAR(sco04_data_hora, 'DD/MM/YYYY') AS sco04_data, 
            TO_CHAR(sco04_data_hora, 'HH24:MI') AS sco04_hora,
            'PROCESSO DE TRANSPORTE JA INICIADO' AS oco_descricao,
            cte006.cte002_chave as chave_cte
            FROM 
            sco04
            LEFT JOIN sco04_08 ON 
            sco04_08.sco04_serial = sco04.sco04_serial
            INNER JOIN cte001 ON
            cte001.cte001_serial = sco04_08.cte001_serial 
            LEFT JOIN sco01 ON 
            sco01.sco01_serial = sco04_08.sco04_08_pag_ser 
            LEFT JOIN sco04_01 ON 
            sco04_01.sco04_serial = sco04.sco04_serial 
            LEFT JOIN sco01 remetente ON 
            remetente.sco01_serial = sco04_01.sco01_codrem
            INNER join cte006 on
            cte006.sco04_serial = sco04.sco04_serial
            left join sco59_01 on 
            sco59_01.sco01_serial = sco04_08.sco04_08_pag_ser
            left join sco59_02 on 
            sco59_02.sco59_01_serial = sco59_01.sco59_01_serial and sco59_02.sco59_02_codjust = 0
            WHERE sco04.sco04_serial ='{serial}'
            and not exists (select * from web19 where web19.sco04_serial = {serial} and web19.oco_serial = '0' and web19_01_serial = 5 )
            
            UNION 
            SELECT
            3 as ordem,
            sco04.sco04_serial,
            case when sco59_02_de_para <> null then sco59_02_de_para else
            sco59_02_codjust end AS oco_codigo,            
            TO_CHAR(sco42.sco42_data_chegada, 'DD/MM/YYYY') AS sco04_data, 
            TO_CHAR(sco42.sco42_hora_chegada, 'HH24:MI') AS sco04_hora, 
            'CHEGADA NA CIDADE OU FILIAL DE DESTINO' AS oco_descricao,
            cte006.cte002_chave as chave_cte
            FROM 
            sco16
            LEFT JOIN sco04 ON 
            sco04.sco04_nrodoc = sco16.sco04_nrodoc AND 
            sco04.sco03_filial = sco16.sco03_filial AND 
            sco04.sco04_serie = sco16.sco04_serie AND 
            sco04.sco03_fildes = sco16.sco16_destino 
            LEFT JOIN sco04_01 ON 
            sco04_01.sco04_serial = sco04.sco04_serial 
            LEFT JOIN sco04_08 ON 
            sco04_08.sco04_serial = sco04.sco04_serial
            INNER JOIN cte001 ON
            cte001.cte001_serial = sco04_08.cte001_serial 
            LEFT JOIN sco01 ON      
            sco01.sco01_serial = sco04_08.sco04_08_pag_ser 
            LEFT JOIN sco42 ON 
            sco16.sco42_serial = sco42.sco42_serial 
            LEFT JOIN sco01 remetente ON 
            remetente.sco01_serial = sco04_01.sco01_codrem
            INNER join cte006 on
            cte006.sco04_serial = sco04.sco04_serial
            left join sco59_01 on 
            sco59_01.sco01_serial = sco04_08.sco04_08_pag_ser
            left join sco59_02 on 
            sco59_02.sco59_01_serial = sco59_01.sco59_01_serial and sco59_02.sco59_02_codjust = 507
            WHERE sco04.sco04_serial ='{serial}'
            and not exists (select * from web19 where web19.sco04_serial = {serial} and (web19.oco_serial = 507 or web19.oco_serial = 98) and web19_01_serial = 5 )
            AND sco16.sco16_destino != sco16.sco16_origem 
            
            UNION 
            SELECT 
            5 as ordem,
            sco04.sco04_serial,
            case when sco59_02_de_para <> null then sco59_02_de_para else
            sco59_02_codjust end AS oco_codigo,
            TO_CHAR(sco42.sco42_data_saida, 'DD/MM/YYYY') AS sco04_data, 
            '0821' AS sco04_hora,
            'MERCADORIA EM ROTA DE ENTREGA' AS oco_descricao,
            cte006.cte002_chave as chave_cte
            FROM 
            sco16 
            LEFT JOIN sco04 ON 
            sco04.sco04_nrodoc = sco16.sco04_nrodoc AND 
            sco04.sco03_filial = sco16.sco03_filial AND 
            sco04.sco04_serie = sco16.sco04_serie AND 
            sco04.sco03_fildes = sco16.sco16_destino 
            LEFT JOIN sco04_01 ON 
            sco04_01.sco04_serial = sco04.sco04_serial 
            LEFT JOIN sco04_08 ON 
            sco04_08.sco04_serial = sco04.sco04_serial
            INNER JOIN cte001 ON
            cte001.cte001_serial = sco04_08.cte001_serial 
            LEFT JOIN sco01 ON      
            sco01.sco01_serial = sco04_08.sco04_08_pag_ser 
            LEFT JOIN sco42 ON 
            sco16.sco42_serial = sco42.sco42_serial 
            LEFT JOIN sco01 remetente ON 
            remetente.sco01_serial = sco04_01.sco01_codrem
            INNER join cte006 on
            cte006.sco04_serial = sco04.sco04_serial
            left join sco59_01 on 
            sco59_01.sco01_serial = sco04_08.sco04_08_pag_ser
            left join sco59_02 on 
            sco59_02.sco59_01_serial = sco59_01.sco59_01_serial and sco59_02.sco59_02_codjust = 214
            WHERE sco04.sco04_serial ='{serial}'
            and not exists (select * from web19 where web19.sco04_serial = {serial} and web19.oco_serial = '214' and web19_01_serial = 5 )
            AND sco16.sco16_destino = sco16.sco16_origem 
            
            UNION 
            
            SELECT 
            6 as ordem,
            sco04.sco04_serial,
            case when sco59_02_de_para <> null then sco59_02_de_para else
            sco59_02_codjust end AS oco_codigo,              
            TO_CHAR(fnc_dtentrega(sco04.sco04_serial), 'DD/MM/YYYY') AS sco04_data, 
            '1724' AS sco04_hora, 
            'ENTREGA REALIZADA NORMALMENTE' AS oco_descricao,
            cte006.cte002_chave as chave_cte
            FROM sco04 
            LEFT JOIN sco04_08 ON 
            sco04_08.sco04_serial = sco04.sco04_serial
            INNER JOIN cte001 ON
            cte001.cte001_serial = sco04_08.cte001_serial 
            LEFT JOIN sco01 ON 
            sco01.sco01_serial = sco04_08.sco04_08_pag_ser 
            LEFT JOIN sco04_01 ON 
            sco04_01.sco04_serial = sco04.sco04_serial 
            LEFT JOIN sco04_05 ON 
            sco04_05.sco04_serial = sco04.sco04_serial 
            LEFT JOIN sco01 remetente ON 
            remetente.sco01_serial = sco04.sco01_codrem
            INNER join cte006 on
            cte006.sco04_serial = sco04.sco04_serial
            left join sco59_01 on 
            sco59_01.sco01_serial = sco04_08.sco04_08_pag_ser
            left join sco59_02 on 
            sco59_02.sco59_01_serial = sco59_01.sco59_01_serial and sco59_02.sco59_02_codjust = 1
            WHERE sco04.sco04_serial ='{serial}'
            and not exists (select * from web19 where web19.sco04_serial = {serial} and web19.oco_serial = '1' and web19_01_serial = 5 )
            AND sco04.sco04_dtaent is not null
            
            UNION
            SELECT 
            5 AS ordem,
            sco04.sco04_serial,
            case when sco59_02_de_para <> null then sco59_02_de_para else
            sco59_02_codjust end AS oco_codigo,
            TO_CHAR(sco31_data, 'DD/MM/YYYY') AS sco04_data,
            TO_CHAR(sco31_hora, 'HH24:MI') AS sco04_hora,
            'ENTREGA PROGRAMADA' AS oco_descricao,
            cte006.cte002_chave as chave_cte
            FROM sco04
            INNER JOIN sco04_08 ON 
            sco04_08.sco04_serial = sco04.sco04_serial
            INNER JOIN sco01 ON
            sco01.sco01_serial = sco04_08.sco04_08_pag_ser
            INNER JOIN sco31 ON
            sco31.sco04_serial = sco04.sco04_serial
            AND (sco31.sco31_data_agend IS NOT NULL OR sco31.sco31_data_agend != '1899-12-31')
            INNER join cte006 on
            cte006.sco04_serial = sco04.sco04_serial
            left join sco59_01 on 
            sco59_01.sco01_serial = sco04_08.sco04_08_pag_ser
            left join sco59_02 on 
            sco59_02.sco59_01_serial = sco59_01.sco59_01_serial and sco59_02.sco59_02_codjust = 91
            WHERE sco04.sco04_serial ='{serial}'
            and not exists (select * from web19 where web19.sco04_serial = {serial} and web19.oco_serial = '91' and web19_01_serial = 5 )
            
            UNION
            SELECT 
            4 as ordem,
            sco04.sco04_serial,
            sco26_03_cod_oco AS oco_codigo,                 
            TO_CHAR(sco26_03.sco26_03_data, 'DD/MM/YYYY') AS sco04_data, 
            '1546' AS sco04_hora,
            sco59_02_descjust AS oco_descricao,
            cte006.cte002_chave as chave_cte
            FROM 
            sco04 
            LEFT JOIN sco04_01 ON 
            sco04_01.sco04_serial = sco04.sco04_serial 
            LEFT JOIN sco04_08 ON 
            sco04_08.sco04_serial = sco04.sco04_serial
            INNER JOIN cte001 ON
            cte001.cte001_serial = sco04_08.cte001_serial 
            LEFT JOIN sco01 ON      
            sco01.sco01_serial = sco04_08.sco04_08_pag_ser 
            LEFT JOIN sco01 remetente ON 
            remetente.sco01_serial = sco04_01.sco01_codrem 
            LEFT JOIN sco26_03 ON 
            sco26_03.sco04_serial = sco04.sco04_serial 
            inner JOIN sco59_01 ON 
            sco59_01.sco01_serial = sco01.sco01_serial 
            INNER JOIN sco59_02 ON 
            sco59_02.sco59_01_serial = sco59_01.sco59_01_serial and sco26_03.sco26_03_cod_oco = sco59_02.sco59_02_codjust 
            INNER join cte006 on
            cte006.sco04_serial = sco04.sco04_serial
            
            WHERE sco04.sco04_serial = '{serial}'
            
            ORDER BY ordem, sco04_data
        """)

    result_oco = inc_database.Db.select(conn, sql)
    return result_oco

def grava_log(retorno, serial, ocorrencia):
    global msg, data, status, cod_ocorrencia
    serial = str(serial)
    notas = ocorrencia['notas']
    notas = notas[0]
    ocorrencias = notas['ocorrencias']

    sql_data = ("select current_date")
    data = inc_database.Db.select(conn, sql_data)
    data = str(data[0][0])
    sql_time = ("select current_time")
    hora = inc_database.Db.select(conn, sql_time)
    hora = str(hora[0][0])[0:8]
    data = data + ' ' + hora

    #print(data)
    print(data, retorno)

    for i in ocorrencias:
        cod_ocorrencia = i['ocorrenciaCodigo']

        if retorno['status'] == '0':
            status = 'True'
            msg = remover_acentos(str(retorno['mensagem']))
        else:
            status = 'False'
            msg = remover_acentos(str(retorno['mensagem']))

        if cod_ocorrencia is not None:
            sql = (f"insert into web19 (web19_mensagem, web19_protocolo, web19_data_requisicao, web19_sucesso, sco04_serial, oco_serial, web19_01_serial) values (sem_acentos('{msg}'),'protocol test','{data}','{status}','{serial}','{cod_ocorrencia}','5')")

            sql = sql.encode('utf-8')
            try:
                #pass
                inc_database.Db.inserir_db(conn, sql)
            except:
                print("Nao foi possivel gravar o log")
        else:
            print(f"Código da ocorrencia para o serial {serial} vazio")

