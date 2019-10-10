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
    host='localhost', database='/home/gabriel/Downloads/_BD/db1.FDB',
    user='sysdba', password='masterkey', charset='WIN1252'
  )

pf_path = '/home/gabriel/Downloads/Modelo de Migração de Dados (Clientes - PF).xls'
pf = pd.read_excel(pf_path)

cursor = con.cursor()
cursor.execute('select CON_NOME,CON_PROFISSAO,CON_CEP,CON_ESTADO, (CON_ENDERECO||\', \'||CON_NUMERO),CON_CIDADE, CON_CPF,CON_IDENTIDADE,CON_PIS,CON_DATANASCIMENTO, CON_TELRESIDENCIAL, CON_TELCOMERCIAL, CON_TELCELULAR, CON_FAX, CON_NACIONALIDADE, CON_ESTADOCIVIL, CON_BAIRRO, CON_EMAIL, CON_HOMEPAGE, CAST(SUBSTRING(CON_OBSERVACOES FROM 1 FOR 32000) AS VARCHAR(32000)) from ADV_CONTATO where CON_TIPO = \'F\'')

data = cursor.fetchall()

for c in data:
    c = list(c)
    c.insert(0, None)
    c.insert(0, None)
    c.insert(16, None)
    c.insert(16, None)
    c.insert(22, None)
    c.insert(22, None)
    c.insert(22, None)
    c.insert(22, None)
    c.insert(22, None)
    #campo livrw 2 corresponde ao item "home page" do banco de dado 
    if c[-1] != None: 
#        c[-1] =c[-1].decode('windows-1252')
        c[-1] = rtf_to_text(c[-1])
    else:
        c[-1] = ''
    s = pd.Series(c, index= pf.columns)
    pf = pf.append(s, ignore_index=True)

pf.fillna(value=pd.np.nan, inplace=True)
pf.to_excel('/home/gabriel/clientes_pf.xlsx', engine='xlsxwriter', index=False)

