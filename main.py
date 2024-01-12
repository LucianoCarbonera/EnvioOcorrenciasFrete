# -----------------------------------------------------------------------------+                                                    |
# Integração SimFretes - Envio automatizado                                    |
# Cacador, 12 de Agosto de 2021                                                |
# Luciano Carbonera                                                            |
# -----------------------------------------------------------------------------+

from libs.database import inc_database
import requests
import inc_db
from datetime import datetime
import traceback

#ENVIO E RETORNO DA REQUISIÇÃO
def enviar_request(body):
    ws_url = {'url_ocorren': 'https://portalws.simfrete.com/Api/Tracking/'} #producao
    #ws_url = {'url_ocorren': 'https://portaltest.simfrete.com/Api/Tracking/'} #homologacao


    headers = {'content-type': 'application/json', 'Accept-Encoding': 'UTF-8'}
    requisicao_ocorrencia = requests.post(ws_url['url_ocorren'], json=body, headers=headers)
    retorno = requisicao_ocorrencia.json()
    return retorno

def montar_body(ctes, login, senha):
    global nfEntrega, nfPrevEntrega, nfEntrega_hora, nfEntrega_data, ocorrencia, body, ocorrencia_data_loka, nfDataAgendamento, cod_ocorrencia, o

    if ctes is not None:

        for cte in ctes:
            serial = cte[0]
            docTranspEmissao = str(datetime.now())
            docTranspEmissao_split = docTranspEmissao.split(" ")
            docTranspEmissao_hora = docTranspEmissao_split[1]
            docTranspEmissao_hora_split = docTranspEmissao_hora.split('.')
            docTranspEmissao_hora = docTranspEmissao_hora_split[0]
            docTranspEmissao = docTranspEmissao_split[0]+'T'+docTranspEmissao_hora

            cnpjEmitente = cte[2]

            nfChave = cte[3]
            docTranspEmitenteCnpj = cte[9]
            docTranspChave = cte[10]

            ocorrencias = inc_db.busca_ocorrencias(cte)
            ocorrencias_json = []
            if ocorrencias != []:

                for o in ocorrencias:
                    cod_ocorrencia = o[2]
                    ocorrencia = str(o)
                    ocorrencia_hora = []
                    ocorrencia_data = []
                    nfDataAgendamento = ''

                    if o[3] is None or o[4] is None:
                        ocorrencia_dt_hr = str(cte[11])
                        ocorrencia_dt_hr = ocorrencia_dt_hr.split(" ")
                        ocorrencia_data = ocorrencia_dt_hr[0]
                        ocorrencia_data_split = str(ocorrencia_data.split('/'))
                        ocorrencia_data = ocorrencia_data_split.replace("/", "-")
                        ocorrencia_hora = ocorrencia_dt_hr[1]
                        ocorrencia_data = ocorrencia_data + 'T' + ocorrencia_hora

                    if o[4] != None and ':' in o[4]:
                        ocorrencia_hora = str(o[4])
                        ocorrencia_data = str(o[3])
                        ocorrencia_data_split = ocorrencia_data.split('/')
                        ocorrencia_data = ocorrencia_data_split[2]+'-' + ocorrencia_data_split[1]+'-' + ocorrencia_data_split[0]
                        ocorrencia_data = str(ocorrencia_data) + 'T' + str(ocorrencia_hora) + ':00'


                    elif o[4] is not None and o[3] is not None and ':' not in o[4]:
                        ocorrencia_hora = str(o[4])
                        ocorrencia_data = str(o[3])
                        ocorrencia_data_split = ocorrencia_data.split('/')
                        ocorrencia_data = str(ocorrencia_data_split[2] + '-' + ocorrencia_data_split[1] + '-' + ocorrencia_data_split[0])
                        ocorrencia_hora_part1 = str(ocorrencia_hora[0]) + str(ocorrencia_hora[1])
                        ocorrencia_hora_part2 = str(ocorrencia_hora[2]) + str(ocorrencia_hora[3])
                        ocorrencia_hora = str(ocorrencia_hora_part1) + ':' + str(ocorrencia_hora_part2)
                        ocorrencia_data = str(ocorrencia_data) + 'T' + str(ocorrencia_hora + ':00')
                        ocorrencia_data_loka = ocorrencia_data


                    elif o[4] is None or o[3] is None and ':' not in o[4]:

                        ocorrencia_dt_hr = str(cte[11])
                        ocorrencia_dt_hr = ocorrencia_dt_hr.split(" ")
                        ocorrencia_data = ocorrencia_dt_hr[0]
                        ocorrencia_data = ocorrencia_data.replace("/", "-")
                        ocorrencia_hora = ocorrencia_dt_hr[1]
                        ocorrencia_hora = ocorrencia_hora.split('.')
                        ocorrencia_data = ocorrencia_data + 'T' + ocorrencia_hora[0]
                        ocorrencia_data_loka = ocorrencia_data

                    elif ':' not in o[4] and o[3] == '':
                        ocorrencia_dt_hr = str(cte[11])
                        ocorrencia_dt_hr = ocorrencia_dt_hr.split(" ")
                        ocorrencia_data = ocorrencia_dt_hr[0]
                        ocorrencia_data = ocorrencia_data.replace("/", "-")
                        ocorrencia_hora = ocorrencia_dt_hr[1]
                        ocorrencia_data = ocorrencia_data + 'T' + ocorrencia_hora
                        ocorrencia_data_loka = ocorrencia_data

                    #VERIFICAÇÕES QUANDO FOI ENTREGUE
                    if o[2] == '1':
                        if o[3] != None and o[4] != None:
                            nfEntrega_data = str(o[3])
                            nfEntrega_data_split = nfEntrega_data.split('/')
                            nfEntrega_data = nfEntrega_data_split[2] + '-' + nfEntrega_data_split[1] + '-' + nfEntrega_data_split[0]
                            nfEntrega_hora = str(o[4])
                            if ':' in nfEntrega_hora:
                                nfEntrega = str(nfEntrega_data) + 'T' + str(nfEntrega_hora) + ':00'
                            else:
                                nfEntrega_hora_1 = str(nfEntrega_hora[0]) + str(nfEntrega_hora[1])
                                nfEntrega_hora_2 = str(nfEntrega_hora[2]) + str(nfEntrega_hora[3])
                                nfEntrega_hora = str(nfEntrega_hora_1) + ':' + str(nfEntrega_hora_2) + ':00'
                                nfEntrega = str(nfEntrega_data) + 'T' + str(nfEntrega_hora)

                        elif str(o[3]): #cai aqui quando a data ta vazia
                            #ocorrencia_data_loka eh a data e hora da ocorrencia anterior
                            nfEntrega = ocorrencia_data_loka
                    else:
                        nfEntrega = ""

                    # VERIFICAÇÃO PREVISÃO DE ENTREGA
                    if cte[4] != '':
                        nfPrevEntrega = str(cte[4])
                        nfPrevEntrega = nfPrevEntrega.replace("/", "-")
                    elif cte[4] == '' and o[2] == '1':
                        nfPrevEntrega = nfEntrega
                    else:
                        docTranspEmissao = str(cte[11])
                        docTranspEmissao_split = docTranspEmissao.split(" ")
                        docTranspEmissao_hora = docTranspEmissao_split[1]
                        docTranspEmissao_hora_split = docTranspEmissao_hora.split('.')
                        docTranspEmissao_hora = docTranspEmissao_hora_split[0]
                        docTranspEmissao = docTranspEmissao_split[0] + 'T' + docTranspEmissao_hora
                        nfPrevEntrega = docTranspEmissao

                    #VERIFICAÇÃO DE AGENDAMENTO DE ENTREGA
                    if o[2] == '91' and cte[7] != None:
                        nfDataAgendamento = str(cte[7])
                        nfDataAgendamento = nfDataAgendamento + 'T15:00:00'
                    elif o[2] == '91' and cte[7] == None:
                        nfDataAgendamento = ''

                    ocorrencias_json.append({
                        "ocorrenciaCodigo": o[2],
                        "ocorrenciaDescricao": o[5],
                        "ocorrenciaData": ocorrencia_data,
                        "ocorrenciaDataDigitacao": ocorrencia_data,
                        "docTranspChave": o[6]

                    })
                body = [{
                    "login": login,
                    "senha": senha,
                    "notas": [
                        {
                            "cnpjEmitente": cnpjEmitente,
                            "nfSerie": "",
                            "nfNum": "",
                            "nfChave": nfChave,
                            "nfPrevEntrega": nfPrevEntrega,
                            "nfEntrega": nfEntrega,
                            "nfDataAgendamento": nfDataAgendamento,
                            "comprovanteEntrega":'',

                            "docsTransporte": [
                                {
                                    "docTranspEmitenteCnpj": docTranspEmitenteCnpj,
                                    "docTranspSerie": "",
                                    "docTranspNumero": "",
                                    "docTranspChave": docTranspChave,
                                    "docTranspEmissao": docTranspEmissao
                                }
                            ],
                            "ocorrencias": []
                        }
                    ]}
                ]

                for nota in body[0]['notas']:
                    nota['ocorrencias'] = ocorrencias_json
                try:
                    request = enviar_request(body[0])
                    inc_db.grava_log(request, serial, body[0])

                except Exception as e:
                    #pass
                    print(f'Erro ao enviar requisicao {traceback.format_exc()}')
            else:
                #pass
                print(f"Nenhum evento ainda não enviado para o serial de cte N° {serial}.")
    else:
        #pass
        print("Nenhum registro encontrado.")

#----------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
 #CHAMADAS DAS FUNÇÕES PARA BUSCAR CTES, REGISTROS E ENVIAR OCORRENCIAS

    login_cnpj = inc_db.busca_cnpj()
    #print(login_cnpj)

    cnpj = ''
    login = ''
    senha = ''
    for i in login_cnpj:
        login = i[1]
        senha = i[2]
        cnpj  = i[3]

        #chamada para buscar ctes
        ctes = inc_db.busca_ctes(cnpj)
        body = montar_body(ctes, login, senha)






