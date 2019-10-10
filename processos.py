# -*- coding: utf-8 -*-
"""
Created on Wed Oct  9 10:22:57 2019

@author: gabriel
"""

import pandas as pd
import fdb
import numpy as np 
from striprtf.striprtf import rtf_to_text


con = fdb.connect(
    host='localhost', database='/home/gabriel/_BD/BD_ADVOGAR.FDB',
    user='sysdba', password='masterkey', charset='WIN1252'
  )
  
mapa= {'Administrativo':'DIREITO ADMINISTRATIVO','Ambiental':'DIREITO AMBIENTAL','Cível':'DIREITO CÍVEL','Criminal':'DIREITO PENAL', 'Família': 'DIREITO FAMÍLIA', 
'Previdenciário':'DIREITO PREVIDENCIÁRIO', 'Trabalhista':'DIREITO DO TRABALHO', 'Tributário':'DIREITO TRIBUTÁRIO', 'JUIZADO': 'DIREITO CÍVEL', 'MARIA DA PENHA':'DIREITO PENAL'}

#           ETL             #
insert_outro = '''insert into ADV_CONTATO (CON_CODIGO
, CON_NOME, CON_TIPO, 
CON_ANIVERSARIANTE) values (0, 'Outro', 'F', 'N')'''

insert_tipoAcao = '''insert into ADV_TIPOACAO(TIP_CODIGO, TIP_NOME) values (0, 'Outro')'''
insert_jurisdicao = '''insert into ADV_JURISDICAO(JUS_CODIGO, JUS_NOME) values (0, 'Outro')'''
insert_foro = '''insert into ADV_FORO(FOR_CODIGO, FOR_NOME) values (0, 'Outro')'''
insert_encerramento = '''insert into ADV_ENCERRAMENTO values (0, 'Não Aplicável')'''

update_adverso = '''update ADV_PROCESSO r set r.PROC_FK_ADVERSOTITULAR = 0 where r.PROC_FK_ADVERSOTITULAR is null'''
update_advogado = '''update ADV_PROCESSO r set r.PROC_FK_ADVOGADOTITULAR = 0 where r.PROC_FK_ADVOGADOTITULAR is null'''
update_natureza = '''update ADV_PROCESSO r set r.PROC_FK_NATUREZA = 3 where r.PROC_FK_NATUREZA is null'''
update_jurisdicao = '''update ADV_PROCESSO r set r.PROC_FK_JURISDICAO_ATUAL = 0 where r.PROC_FK_JURISDICAO_ATUAL is null'''
update_tipoAcao = '''update ADV_PROCESSO r set r.PROC_FK_TIPO_ACAO = 0 where r.PROC_FK_TIPO_ACAO is null'''
update_foro = '''update ADV_PROCESSO r set r.PROC_FK_FORO_ATUAL = 0 where r.PROC_FK_FORO_ATUAL is null'''
update_encerramento = '''update ADV_PROCESSO r set r.PROC_FK_TIPOENCERRAMENTO = 0 where r.PROC_FK_TIPOENCERRAMENTO is null'''


cursor = con.cursor()
cursor.execute(insert_outro)
con.commit()
cursor = con.cursor()
cursor.execute(insert_tipoAcao)
con.commit()
cursor = con.cursor()
cursor.execute(insert_jurisdicao)
con.commit()
cursor = con.cursor()
cursor.execute(insert_foro)
con.commit()
cursor = con.cursor()
cursor.execute(insert_encerramento)
con.commit()

cursor = con.cursor()
cursor.execute(update_adverso)
con.commit()
cursor = con.cursor()
cursor.execute(update_advogado)
con.commit()
cursor = con.cursor()
cursor.execute(update_natureza)
con.commit()
cursor = con.cursor()
cursor.execute(update_jurisdicao)
con.commit()
cursor = con.cursor()
cursor.execute(update_tipoAcao)
con.commit()
cursor = con.cursor()
cursor.execute(update_foro)
con.commit()
cursor = con.cursor()
cursor.execute(update_encerramento)
con.commit()

##################

sql= '''SELECT C.CON_TIPO as tipo, c.CON_NOME as nome_cliente,c2.CON_NOME as adverso,
r.PROC_NUMEROATUAL as numero_processo, r.PROC_SITUACAO_CLIENTE as status_procesual,
n.NAT_NOME as area_atuacao, ta.TIP_NOME as obj_acao, r.PROC_ASSUNTO as assunto,
j.JUS_NOME as local_tramite, f.FOR_NOME as comarca, f.FOR_UF as comarca_uf,
r.PROC_SITUACAO as fase, r.PROC_PASTA as pasta, c3.CON_EMAIL, e.ENC_NOME as detalhes, r.PROC_APENSO as apenso,
r.PROC_DATACADASTRAMENTO as data_contratacao, r.PROC_DATAARQUIVAMENTO as data_encerramento, 
r.PROC_OBSERVACAO as observacao
FROM ADV_PROCESSO r 
join ADV_CONTATO C on r.PROC_FK_CLIENTETITULAR = C.CON_CODIGO
join ADV_TIPOACAO ta on r.PROC_FK_TIPO_ACAO = ta.TIP_CODIGO
join ADV_NATUREZA n on r.PROC_FK_NATUREZA = n.NAT_CODIGO
join ADV_JURISDICAO j on r.PROC_FK_JURISDICAO_ATUAL = j.JUS_CODIGO
join ADV_FORO f on r.PROC_FK_FORO_ATUAL = f.FOR_CODIGO
join ADV_CONTATO c2 on r.PROC_FK_ADVERSOTITULAR = c2.CON_CODIGO
join ADV_CONTATO c3 on r.PROC_FK_ADVOGADOTITULAR = c3.CON_CODIGO
join ADV_ENCERRAMENTO e on r.PROC_FK_TIPOENCERRAMENTO = e.ENC_CODIGO'''

pf_path = '/home/gabriel/Downloads/MODELO PROMAD de Migração de Dados (Processos).xls'
pf = pd.read_excel(pf_path)
pf.drop(pf.index, inplace=True)
cursor = con.cursor()
cursor.execute(sql)
data = cursor.fetchall()




for c in data:
    c = list(c)
    
    #coloca se a pessoa eh fisica ou juridica
    if c[0] == 'F':
        c[0] = 'PESSOA FÍSICA'
    else:
        c[0] ='PESSOA JURÍDICA'
    
    c.insert(3, 'JUDICIAL')    
    #completa os dados     
    c.insert(4, None)
    c.insert(4, None)
    
    #seta se eh autor ou reu
    if c[7] == 'A':
        c[7] = 'AUTOR'
    elif c[7] == 'R':
        c[7] = 'RÉU'
    
    c.insert(11, None)
    
    
    c.insert(17, None)
    c.insert(17, None)
    
    c.insert(21, None)
    c.insert(21, None)
    c.insert(21, None)
    
    
    c.insert(25, None)
    c.insert(25, None)
    c.insert(25, None)
    
    c.insert(29, None)
    c.insert(29, None)
    
    c.insert(32, None)
    c.insert(32, None)
    c.insert(32, None)
    c.insert(32, None)
    c.insert(32, None)
    c.insert(32, None)


    s = pd.Series(c, index= pf.columns)
    pf = pf.append(s, ignore_index=True)

pf.fillna(value=pd.np.nan, inplace=True)
pf['ÁREA DE ATUAÇÃO'] = pf['ÁREA DE ATUAÇÃO'].map(mapa)
#pf['DATA DA CONTRATAÇÃO'] = pf['DATA DA CONTRATAÇÃO'].astype('datetime64[ns]')
#pf['DATA DA CONTRATAÇÃO'] = pf['DATA DA CONTRATAÇÃO'].dt.strftime('%d/%m/%Y')
#pf['DATA ENCERRAMENTO'] = pf['DATA ENCERRAMENTO'].astype('datetime64[ns]')
#pf['DATA ENCERRAMENTO'] = pf['DATA ENCERRAMENTO'].dt.strftime('%d/%m/%Y')
pf.to_excel('/home/gabriel/processos.xlsx', engine='xlsxwriter', index=False)