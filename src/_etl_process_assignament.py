    # -*- coding: utf-8 -*-
"""
Created on Tue Oct 29 09:05:20 2024

@author: Ronal.Barberi

READ.ME: 
    This script performs data cleansing by filtering only the necessary columns for database management,
    where we conduct data mining to extract all available phone numbers to manage clients. It removes duplicate
    numbers and empty records to create a comprehensive ETL that receives raw data and delivers processed data
    for management, and inserts the data into the server (SQL) to maintain a management history.
"""

#%% Import libraries

import os
import re
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from _cls_nav_directorys import Nav_Directorys as nd
from _cls_mysql_conector import MySQLConnector as ms
from _cls_sqlalchemy import SqlAchemy as sa

#%% Create class

class ProcessStructuredData:
    def __init__(self, dicArgs):
        self.dicArgs = dicArgs
        self.engine = ms.funConectSql60(dicArgs['varSchema'])
    
    def coalesce_phone(self, df):
        key_columns = [
            'idasignacion', 'nombrecampana', 'apellido', 'ciudad', 'cuenta', 'direccion', 'email', 
            'estrategia', 'estrato', 'nombre', 'numeroidentificacion', 'paquete', 'valoroferta1', 
            'valoroferta2', 'estado', 'nombrecliente', 'tipocliente', 'antiguedad', 'periodo', 'segmento'
        ]
        df = df.melt(id_vars=key_columns,
                        value_vars=['telenum','celular1','celular2','celular3','telefono1','telefono2','telefono3'],
                        value_name='phone')
        
        return df
    
    def phone_debuger(self, phone):
        phone_ok = re.sub(r'\D', '', str(phone))
        phone_fijo = re.compile(r'^(60\d{8})$')
        phone_movil = re.compile(r'^(3\d{9})$')
        
        if phone_fijo.match(phone_ok):
            return phone_ok
        elif phone_movil.match(phone_ok):
            return phone_ok
        else:
            return ''
    
    def read_dataset(self):
        map_columns = {
            'IdAsignacion': 'idasignacion',
            'NombreCampaña': 'nombrecampana',
            'Apellido': 'apellido',
            'Celular1': 'celular1',
            'Celular2': 'celular2',
            'Celular3': 'celular3',
            'Ciudad': 'ciudad',
            'Cuenta': 'cuenta',
            'Direccion': 'direccion',
            'Email': 'email',
            'Estrategia': 'estrategia',
            'Estrato': 'estrato',
            'Nombre': 'nombre',
            'NumeroIdentificacion': 'numeroidentificacion',
            'Paquete': 'paquete',
            'Telefono1': 'telefono1',
            'Telefono2': 'telefono2',
            'Telefono3': 'telefono3',
            'TelefonoTelmex': 'telefonotelmex',
            'ValorOferta1': 'valoroferta1',
            'ValorOferta2': 'valoroferta2',
            'TeleNum': 'telenum',
            'Estado': 'estado',
            'NombreCliente': 'nombrecliente',
            'TipoCliente': 'tipocliente',
            'Antiguedad': 'antiguedad',
            'Periodo': 'periodo',
            'Segmento': 'segmento',
        }
        dfs = []
        for filename in os.listdir(self.dicArgs['PathFile']):
            if filename.endswith('.xlsx'):
                file_path = os.path.join(self.dicArgs['PathFile'], filename)
                df = pd.read_excel(file_path, dtype={'IdAsignacion': str})
                df = df.rename(columns=map_columns).loc[:, map_columns.values()]
                col_to_str = [
                    'celular1',
                    'celular2',
                    'celular3',
                    'telefono1',
                    'telefono2',
                    'telefono3',
                    'telenum',
                    ]
                for col in col_to_str:
                    df[col] = df[col].astype(str)
                df = self.coalesce_phone(df)
                df['phone'] = df['phone'].apply(self.phone_debuger)
                df = df.dropna(subset=['phone']).drop('variable', axis=1)
                df = df[df['phone'] != ''].drop_duplicates(subset=['phone'])
                upload_date = datetime.now()
                upload_date = upload_date.strftime('%Y-%m-%d')
                df['upload_date'] = upload_date
                df['idasignacion'] = df['idasignacion'].astype(str)
                dfs.append(df)
        df = pd.concat(dfs, ignore_index=True)
        df.to_excel(self.dicArgs['PathFileExport'], index=False)
        print(df.dtypes)
        print("Amount Register: ", df.shape[0])
        sa.funInsertDFNotTruncate(self.engine, df, self.dicArgs['varSchema'], self.dicArgs['varTable'])

    def main(self):
        self.read_dataset()

# Use class

if __name__ == "__main__":
    dicArgs = {
        'PathFile': nd.funJoinTwoDic(nd.funDicOneBack(), 'data', 'tmp_assignament'),
        'PathFileExport': nd.funJoinTwoDic(nd.funDicOneBack(), 'data', 'dataset_process_export.xlsx'),
        'varSchema': 'basedataname',
        'varTable': 'tablename',
    }
    
    etl = ProcessStructuredData(dicArgs)
    etl.main()