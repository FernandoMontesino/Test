# -*- coding: utf-8 -*-
"""
Created on Mon Oct 19 14:01:46 2020 --- TEST GITHUB ------
no aparecen cambios?

@author: B082

Script para analizar le calidad del catálogo HLS. Comprueba si la duración real de los eventos es la correcta, 
si presentan errores de discontinudad o si tiene manifest

Para ello procesa foto del catálogo y va descargando el manifest de los contenidos

v3: incluye delta para ejecutarse de manera diaria
Lunes analiza contenidos del fin de semana. De martes a viernes del día anterior

v5: incluye generacion de historico para ver evolucion. Tambien detecta entre errores de discontinudad leves y criticos

"""
from funciones_ESC import envio_correo
import warnings
warnings.filterwarnings("ignore")
import glob
import os
import pandas as pd
import requests
import datetime
import time
import calendar
import urllib.request
import openpyxl
#import os



def get_duracion(url):
    print(url)
    url = 'http://live.euskaltel.tv/Content/HLS_PRM/LLCU/EUROSPORT/ltcu_CF120033466/Stream(04)/index.m3u8'
    url = 'http://live.euskaltel.tv/Content/HLS_PRM/LLCU/TVG/ltcu_CF120286386/Stream(03)/index.m3u8'
    url = 'http://live.euskaltel.tv/Content/HLS_PRM/LLCU/LA2/ltcu_ME12664568/Stream(03)/index.m3u8'



    try:
        # urllib.request.urlretrieve(url)
        c=pd.read_csv(url, error_bad_lines=False)

    except urllib.error.HTTPError as err:
        print(err.code)
        return '-9999999999 0 1 9999999999'
   
    
    aux = c['#EXTM3U'].str.contains(pat = 'Segment' , case = False)
    df = c[aux == True]


    df['#EXTM3U'] = df['#EXTM3U'].str.replace('Segment\(', '')
    df['#EXTM3U'] = df['#EXTM3U'].str.replace('\).ts', '')
    df['#EXTM3U'] = df['#EXTM3U'].astype(float)        
    ans = (df.iloc[-1] - df.iloc[0])/10000000 
    segment = str(df.iloc[-1][0])
    
    df2 = c.copy()
    aux = c['#EXTM3U'].str.contains(pat = 'Segment|DISCONTINUITY' , case = False)
    df2 = df2[aux == True]
    df2["dos"] = df2['#EXTM3U'].shift(1)
    

    ## busca los segmentos con discontinuidades
    aux2 = df2['#EXTM3U'].str.contains(pat = 'DISCONTINUITY' , case = False)
    df2 = df2[aux2 == True]
    critico = 0 
    # segment = ''
    ##se ejecuta si hay errores de discontinuidad
    if(len(df2) > 0):
        df2['url'] = url
        df2['url'] = df2['url'].str.replace('index.m3u8', '')
        df2['urla'] = df2['url'] + df2['dos']
        df2['dos'] = df2['dos'].str.replace('Segment\(', '')
        df2['dos'] = df2['dos'].str.replace('\).ts', '')


        for index, value in df2['urla'].items():
            r = requests.get(value, allow_redirects = True)
            print(r.status_code)
            if(r.ok == False):
                critico = 1
                segment = str(df2.iloc[index]['dos'])

                break            
            
    segment = int(float(segment)/1e7)
    segment = time.gmtime(segment)
    auxi =  datetime.datetime(*segment[:6])
    segment = auxi.strftime("%m/%d/%Y--%H:%M:%S")


    #EXT-X-PROGRAM-DATE-TIME:
    aux3 = c['#EXTM3U'].str.contains(pat = 'DATE' , case = False)
    df3 = c[aux3 == True]
    df3['#EXTM3U'] = df3['#EXTM3U'].str.replace('#EXT-X-PROGRAM-DATE-TIME:', '')

    res = str(ans['#EXTM3U']) + ' ' + str(len(df2)) + ' ' + segment + ' '  + str(critico) + ' ' + str(df3.iloc[0]['#EXTM3U'])
    
    time.sleep(0.3)
    
    return res


def is_downloadable(url): ##funcion quecomprueba si la url contiene un archivo descargable
    """
    Does the url contain a downloadable resource
    """
    print(url)

    h = requests.head(url, allow_redirects=True)
    header = h.headers
    content_type = header.get('content-type')
    if content_type is None:
        return False
    if 'html' in content_type.lower():
        return False
    return True

def stream_canal(canales): ##funcion que comprueba el stream de mayor calidad para cada canal
    canales_stream = []
    for f in range (0,len(canales)):
    
        url = canales.iloc[f]['url4']
        a = is_downloadable(url)
        if(a == True):
            canales.iloc[f]['aux'] = 4
            aa = canales.iloc[f]['serviceId'] + ' 4'
            canales_stream.append(aa)
            continue
    
        url = canales.iloc[f]['url3']
        a = is_downloadable(url)
        if(a == True):
            aa = canales.iloc[f]['serviceId'] + ' 3'
            canales_stream.append(aa)
            continue
        
        url = canales.iloc[f]['url2']
        a = is_downloadable(url)
        if(a == True):
            aa = canales.iloc[f]['serviceId'] + ' 2'
            canales_stream.append(aa)
            continue
    
        url = canales.iloc[f]['url1']
        a = is_downloadable(url)
        if(a == True):
            aa = canales.iloc[f]['serviceId'] + ' 1'
            canales_stream.append(aa)
            
    return canales_stream

#my_date = datetime.datetime.today()
my_date = datetime.date.today()
margen_duracion_erronea_min=-3

dia = calendar.day_name[my_date.weekday()]  #'Wednesday'
i = 1
if dia == 'Monday':
    i = 3
    

start_date = my_date - datetime.timedelta(days=i)
end_date = my_date

#path_informes = '//inventario.oym.r.lan/INVENTARIO/ADV/INFORMES_4K/INFORMESOTT'
path_informes = r"/datos/tv/galicia/catalogo"
#path_hist = 'C:/Users/B082/Documents'
path_hist = r"/datos/tv/grupo/informes/contenidos"

mascara_informes = "contenidos_OTT4K+OTTEXT_visibles*"
mascara_hist = 'historico_duracion_cont_*'

mascara_url = 'http://live.euskaltel.tv'

files_informes = sorted(glob.glob(path_informes + '/*' + mascara_informes+ '*'), key=os.path.getmtime)
file_hist = sorted(glob.glob(path_hist + '/*' + mascara_hist + '*'), key=os.path.getmtime)


contenidos =  pd.read_csv(files_informes[-1], sep = ';', encoding = 'latin-1')

contenidos['date'] = pd.to_datetime(contenidos['periodStartDate'], format = '%Y-%m-%d')
#mask = (contenidos['date'] > start_date) & (contenidos['date'] <= end_date)
mask = (contenidos['date'].dt.date >= start_date) & (contenidos['date'].dt.date <= end_date)   
contenidos = contenidos[mask == True]

llcu = contenidos['FileName'].str.contains(pat = 'LLCU' , case = False)
contenidos_LLCU = contenidos[llcu == True]

## se crean las 4 url posibles
contenidos_LLCU['urla'] = contenidos_LLCU['FileName'].str.replace('index.m3u8', '')
contenidos_LLCU['url1'] = mascara_url + contenidos_LLCU['urla'] + 'Stream(01)/index.m3u8'
contenidos_LLCU['url2'] = mascara_url + contenidos_LLCU['urla'] + 'Stream(02)/index.m3u8'
contenidos_LLCU['url3'] = mascara_url + contenidos_LLCU['urla'] + 'Stream(03)/index.m3u8'
contenidos_LLCU['url4'] = mascara_url + contenidos_LLCU['urla'] + 'Stream(04)/index.m3u8'

## se llama a la funcion stram_canal con un contenido de cada canal
## para probar cual es el stram de mayor calidad disponible
canales_str = stream_canal(contenidos_LLCU.drop_duplicates('serviceId'))

## la funcion devuelve una lista que se convierte en un df
## para hacer un merge con la lista de contenidos llcu y obtener asi el stream adecuado
## para canada contenido
df_canales = pd.DataFrame(canales_str,columns=['Name'])
df_canales = pd.DataFrame(df_canales.Name.str.split().tolist(), columns = ['serviceId','stream'])
contenidos_LLCU = pd.merge(contenidos_LLCU, df_canales)

#contenidos_LLCU = contenidos_LLCU.head(10)

## se crea la url de maxima calidad disponible para cada elemento
contenidos_LLCU['url'] = ''
contenidos_LLCU['url'] = mascara_url + contenidos_LLCU['urla'] + 'Stream(0'+ contenidos_LLCU['stream']  + ')/index.m3u8'


## aplica la funcion get duracion a la lista de contenidos_LLCU

contenidos_LLCU['Name'] = [get_duracion(x) for x in contenidos_LLCU['url']]
df = pd.DataFrame(contenidos_LLCU.Name.str.split().tolist(), columns = ['duracion_real','N_discontinuidades','Hora_Segmento', 'Disc_crit', 'fecha_manifest'])
contenidos_LLCU['duracion_real'] = df['duracion_real'].astype(float)
contenidos_LLCU['N_discontinuidades'] = df['N_discontinuidades'].astype(float)
contenidos_LLCU['Hora_Segmento'] = df['Hora_Segmento']
contenidos_LLCU['Disc_crit'] = df['Disc_crit'].astype(float)
contenidos_LLCU['fecha_manifest'] = df['fecha_manifest']


## se eliminan las columnas no necesarias (sin usar drop columns)
contenidos_LLCU = contenidos_LLCU[['originalId', 'eventId', 'periodStartDate', 'periodEndDate',
       'titleEsEs', 'season', 'episode', 'genre', 'subgenre', 'serviceId',
       'duration', 'priceEkt', 'priceR', 'FileName', 'Caratula',
       'serviceLongName', 'canonicalId', 'year', 'systemScore', 'imdbScore',
       'rtScore', 'seriesId','url','duracion_real','N_discontinuidades', 'Disc_crit', 'fecha_manifest']]

contenidos_LLCU['diff_seg'] = (contenidos_LLCU['duracion_real'] - contenidos_LLCU['duration'])
contenidos_LLCU['diff_min'] = contenidos_LLCU['diff_seg']/60

## Calculo de contenidos errones por tipo y de ratios
cont_neg = sum(n < 0 for n in contenidos_LLCU.diff_min)
p_neg = cont_neg/len(contenidos_LLCU) 
p_neg = "{:.2%}".format(p_neg)

cont_disc = sum(n > 0 for n in contenidos_LLCU.N_discontinuidades)
p_disc = cont_disc/len(contenidos_LLCU)
p_disc = "{:.2%}".format(p_disc)

cont_disc_crit = sum(n > 0 for n in contenidos_LLCU.Disc_crit)
p_disc_crit = cont_disc_crit/len(contenidos_LLCU)
p_disc_crit = "{:.2%}".format(p_disc_crit)

df_hist = pd.read_csv(file_hist[-1], sep = ';', encoding = 'latin-1')

df_hist = df_hist.append(pd.Series([my_date.strftime("%d-%m-%Y"), cont_neg, p_disc, cont_disc, p_disc, cont_disc_crit, p_disc_crit,len(contenidos_LLCU)], index=df_hist.columns ), ignore_index=True)

yesterday = (my_date - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

file = 'contenidos_cortados_' + str(yesterday) + '.csv'
path_destino=r"/datos/tv/grupo/informes/contenidos"

contenidos_LLCU.to_csv(path_destino+"/"+file, sep=';' , decimal=",", index = False)
file_hist = path_hist + '/' + 'historico_duracion_cont_' + str(yesterday) + '.csv'
df_hist.to_csv(file_hist, sep=';' , decimal=",", index = False)


## Creamos excell con contenidos afecfados para envio

contenidos_LLCU = contenidos_LLCU[['originalId', 'eventId', 'periodStartDate', 'periodEndDate',
       'titleEsEs', 'serviceId','duration', 'FileName', 'Caratula','serviceLongName', 'canonicalId',
       'seriesId','url','duracion_real','diff_min','N_discontinuidades', 'Disc_crit', 'fecha_manifest',"Hora_Segmento"]]

aux = contenidos_LLCU['diff_min'] < margen_duracion_erronea_min
contentidos_cortados = contenidos_LLCU[aux == True]

aux = contenidos_LLCU['Disc_crit'] > 0 
contentidos_crit = contenidos_LLCU[aux == True]

aux = contenidos_LLCU['duracion_real'] == -9999999999 
contentidos_sin_index = contenidos_LLCU[aux == True]
contentidos_cortados = contentidos_cortados[aux == False]
file2='contenidos_erroneos_' + str(yesterday) + '.xlsx'
writer = pd.ExcelWriter(path_destino+"/"+ file2, engine="openpyxl")

# Write each dataframe to a different worksheet.
contentidos_cortados.to_excel(writer, sheet_name='Contenidos_cortados',index=False)
contentidos_crit.to_excel(writer, sheet_name='Contenidos_errores_crit', index=False)
contentidos_sin_index.to_excel(writer, sheet_name='Contentidos_sin_index', index=False)
df_hist.to_excel(writer, sheet_name='Evolucion_contenidos_erroneos', index=False)

# Close the Pandas Excel writer and output the Excel file.
writer.save()

## Envio correo con info de contenidos erroneos
cuerpo= """ 
    Detección Contenidos HLS erroneos:
        - Contenidos en catálogo sin manifest asociado
        - Contenidos con duraciones reales inferiores a las teóricas/mínimas
        - Contenidos con errores de continuidad
    """
asunto="Contenidos erroneos catálogo HLS --- " + dia + " " + my_date.strftime("%d%m%Y")
envio_correo(asunto, cuerpo, file2, path_destino+"/"+file2)

