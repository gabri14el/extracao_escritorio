# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd
import fdb
import numpy as np 
from striprtf.striprtf import rtf_to_text


con = fdb.connect(
    host='localhost', database='/home/gabriel/_BD/BD_ADVOGAR.FDB',
    user='sysdba', password='masterkey', charset='WIN1252'
  )

pf_path = '/home/gabriel/Downloads/Modelo de Migração de Dados (Clientes - PJ).xls'
pf = pd.read_excel(pf_path)




sql= ''' 
SELECT r.CON_NOME, r.CON_JURIDICA_NOMEFANTASIA, r.CON_CGC, r.CON_JURIDICA_CONTATO1, r.CON_JURIDICA_RAMOATIVIDADE, 
r.CON_INSCESTADUAL, r.CON_INSCMUNICIPAL, r.CON_JURIDICA_TELCONTATO1, r.CON_JURIDICA_TELCONTATO2, r.CON_EMAIL, r.CON_FAX,
r.CON_HOMEPAGE, r.CON_CIDADE, r.CON_ESTADO, r.CON_CEP, r.CON_BAIRRO, r.CON_ENDERECO, 
r.CON_JURIDICA_CONTATO2, r.CON_TELCOMERCIAL, CAST(SUBSTRING(CON_OBSERVACOES FROM 1 FOR 32000) AS VARCHAR(32000))
FROM ADV_CONTATO r where r.CON_TIPO = 'J'
'''
cursor = con.cursor()
cursor.execute(sql)

data = cursor.fetchall()

for c in data:
    c = list(c)
    
    c.insert(0, None)
    c.insert(0, None)
    
    c.insert(12, None)
    if c[-1] != None: 
#        c[-1] =c[-1].decode('windows-1252')
        c[-1] = rtf_to_text(c[-1])
    else:
        c[-1] = ''
    s = pd.Series(c, index= pf.columns)
    pf = pf.append(s, ignore_index=True)

pf.fillna(value=pd.np.nan, inplace=True)
pf.to_excel('/home/gabriel/clientes_pj.xlsx', engine='xlsxwriter', index=False)

