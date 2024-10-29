# -*- coding: utf-8 -*-
"""
Created on Tue Oct 29 09:05:20 2024

@author: Ronal.Barberi
READ.ME:
    The following ETL extracts information from received emails, performs data transformation as needed, and inserts the data into the SQL server,
    where it also triggers the execution of the necessary stored procedures.
"""

#%% Imported libraries

import os
import shutil
import pandas as pd
from tqdm import tqdm
import win32com.client
from _cls_sqlalchemy import SqlAchemy as sa
from datetime import date, datetime
from _cls_nav_directorys import Nav_Directorys as nd
from _cls_mysql_conector import MySQLConnector as ms

#%% Create Class

class LoadVentasTMK:
    def __init__(self, varSchema, dicSender, dicMail, varPathSave, dicTables, lisSps):
        self.varSchema = varSchema
        self.engine = ms.funConectSql60(varSchema)
        self.dicSender = dicSender
        self.dicMail = dicMail
        self.varPathSave = varPathSave
        self.dicTables = dicTables
        self.lisSps = lisSps

    def extracted_data_mail(self):
        self.now_date = datetime.now()
        self.date_start_month = self.now_date.strftime('%Y-%m-01')
        today = date.today()
        date_start_month_obj = datetime.strptime(self.date_start_month, '%Y-%m-%d')
        today_str = today.strftime('%Y-%m-%d')
        today_obj = datetime.strptime(today_str, '%Y-%m-%d')
        self.count_days = (today_obj - date_start_month_obj).days

        outlook = win32com.client.Dispatch('Outlook.Application')
        namespace = outlook.GetNamespace('MAPI')
        root_folder = namespace.Folders[self.dicMail['Correo']]
        first_folder = root_folder.Folders[self.dicMail['Carpeta_Principal']]
        second_folder = first_folder.Folders[self.dicMail['Carpeta_Secundaria']]
        correos = second_folder.Items
        file_index = 1
        for correo in correos:
            if correo.ReceivedTime.date() == today:
                sender_email = correo.SenderEmailAddress.lower()
                for campaign, senders in self.dicSender.items():
                    if any(sender.lower() == sender_email for sender in senders):
                        for adjunto in correo.Attachments:
                            base_name, extension = os.path.splitext(adjunto.FileName)
                            full_path_save = f'{self.varPathSave}\{campaign}'
                            if not os.path.exists(full_path_save):
                                os.makedirs(full_path_save)
                            if extension.lower() == '.xlsx':
                                new_file_name = f"{base_name}_{file_index}{extension}"
                                ruta_adjunto = os.path.join(full_path_save, new_file_name)
                                adjunto.SaveAsFile(ruta_adjunto)
                                file_index += 1                            

    def esctructured_df_hogar(self, df):
        map_columns = {
            'FECHA DE VENTA': 'FECHA DE VENTA',
            'NUMERO DE VENTA': 'NUMERO DE VENTA',
            'TIPO DE VENTA': 'TIPO DE VENTA',
            'CEDULA': 'CEDULA',
            'NOMBRE DEL ASESOR': 'NOMBRE DEL ASESOR',
            'OPERADOR': 'OPERADOR',
            'ESTADO FINAL ': 'ESTADO FINAL',
            'SEGMENTO': 'SEGMENTO',
            'CUENTA': 'CUENTA',
        }
        df = df.rename(columns=map_columns)
        if not pd.api.types.is_datetime64_any_dtype(df['FECHA DE VENTA']):
            df['FECHA DE VENTA'] = pd.to_datetime('1899-12-30') + pd.to_timedelta(df['FECHA DE VENTA'], unit='d')
        df = df[df['FECHA DE VENTA'] >= self.date_start_month]
        df['upload_at'] = self.now_date
        return df

    def esctructured_df_movil(self, df):
        map_columns = {
            'FECHA DE VENTA': 'FECHA DE VENTA',
            'NUMERO DE VENTA ': 'NUMERO DE VENTA',
            'TIPO DE VENTA': 'TIPO DE VENTA',
            'CEDULA ASESOR': 'CEDULA ASESOR',
            'NOMBRE DEL ASESOR': 'NOMBRE DEL ASESOR',
            'OPERADOR': 'OPERADOR',
            'ESTADO FINAL ': 'ESTADO FINAL',
            'SEGMENTO': 'SEGMENTO',
        }
        df = df.rename(columns=map_columns)
        df = df.iloc[:, :8]
        if not pd.api.types.is_datetime64_any_dtype(df['FECHA DE VENTA']):
            df['FECHA DE VENTA'] = pd.to_datetime('1899-12-30') + pd.to_timedelta(df['FECHA DE VENTA'], unit='d')
        df = df[df['FECHA DE VENTA'] >= self.date_start_month]
        df['upload_at'] = self.now_date
        return df

    def estructured_insert(self):
        for campaign, table in self.dicTables.items():
            full_path_save = f'{self.varPathSave}\{campaign}'
            for filename in os.listdir(full_path_save):
                file_path = os.path.join(full_path_save, filename)
                df = pd.read_excel(file_path)
                if not df.empty:
                    query = f"DELETE FROM `{self.varSchema}`.`{table}` WHERE `FECHA DE VENTA` >= '{self.date_start_month}';"
                    sa.funExecuteQuery(self.engine, query)
                    if campaign == 'HOGAR':
                        df = self.esctructured_df_hogar(df)
                    elif campaign == 'MOVIL':
                        df = self.esctructured_df_movil(df)
                    sa.funInsertDFNotTruncate(self.engine, df, self.varSchema, table)
        shutil.rmtree(self.varPathSave)
        for sp in self.lisSps:
            sp_complet = f'{sp}({self.count_days});'
            sa.funExecuteQuery(self.engine, sp_complet)

    def main(self):
        functions = [
            #self.extracted_data_mail,
            self.estructured_insert,
        ]
        for fun in tqdm(functions, desc='Upload Sales'):
            fun()

#%% Use class

if __name__ == "__main__":
    varSchema = 'basedataname'
    dicSender = {
        'HOGAR': ['remitanten_org1@outlook.com; remitanten_org2@outlook.com;'],
        'MOVIL': ['remitanten_org3@outlook.com; remitanten_org4@outlook.com; remitanten_org5@outlook.com;'],
    }
    dicMail = {
        'Correo': 'Ronal.Barberi@outlook.com',
        'Carpeta_Principal': 'SALES',
        'Carpeta_Secundaria': 'BACKOFICE',
    }
    varPathSave = nd.funJoinTwoDic(nd.funDicOneBack(), 'data', 'tmp_ventas')
    dicTables = {
        'HOGAR': 'tb_crudo_ventas_hogar',
        'MOVIL': 'tb_crudo_ventas_movil',
    }
    lisSps = [
        'CALL basedataname.sp_bdd_ventas_hogar_dts',
        'CALL basedataname.sp_bdd_ventas_migracion_dts',
        'CALL basedataname.sp_bdd_ventas_portabilidad_dts',
    ]
    
    bot = LoadVentasTMK(varSchema, dicSender, dicMail, varPathSave, dicTables, lisSps)
    bot.main()
