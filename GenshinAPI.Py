import time
import requests
import os
import pandas as pd
import numpy as np
import pyspark.pandas as ps
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, DateType
from pyspark.sql.functions import round 
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import xml.etree.ElementTree as ET
from matplotlib.patches import Shadow

spark = SparkSession.builder.getOrCreate()

def extract_data(url):
    '''
    Får en masse karaktere med en masse forskelige data, dataen omhandler spillet Genshin impact. Som jeg har valgt fordi der en 
    masse forskelige ting at måle karakterne på og der kommer nye karaktere ca en gang om måneden
    '''
    result = requests.get(url)
    return result


def transform_data(data):
    '''
    Sortere karaktere efter release date i stedet for alphabetisk rækkefølge, som den er i på API'en. uskriver antallet af karaktere fra 
    de forkslige lande og hvor mange der fra hvor procentmæssigt. Den laver også en dataframe af hvor mange karaktere der har et hvis element.
    Karakteren Traveler er den eneste som ikke har fielded 'title' eller 'gender' af en eller anden grund. Der også flere karaktere der hedder travler fordi
    det er den karakter man selv laver og API'en har flere forskelige versioner.
    '''
    jaison =data.json()
    transformedData = []
    for character in jaison:
        if character['name'] != 'Traveler':
            transformedData.append([character['name'],character['title'],character['vision'],character['weapon'],character['gender'],character['nation'],datetime.strptime(character['release'], '%Y-%m-%d').date()])
        else:
            transformedData.append([character['name'],'',character['vision'],character['weapon'],'Selectable',character['nation'],datetime.strptime(character['release'], '%Y-%m-%d').date()])
    
    schema = StructType([ \
        StructField("Name",StringType(),True), \
        StructField("Title",StringType(),True), \
        StructField("Element",StringType(),True), \
        StructField("Weapon", StringType(), True), \
        StructField("Gender", StringType(), True), \
        StructField("Nation", StringType(), True), \
        StructField("ReleaseDate", DateType(), True) \
     ])
    
    dataframe = spark.createDataFrame(data=transformedData,schema=schema)
    dataframe = dataframe.orderBy(dataframe.ReleaseDate.desc())
    dataframe.show(truncate=False)
    
    numberOfCharcters = len(jaison)
    nationframe = (dataframe.groupBy('Nation').count())
    nationframe = nationframe.withColumn('Procent',(round(nationframe[1]/numberOfCharcters*100,2)))
    nationframe.show(truncate=False)
    
    elementframe = (dataframe.groupBy('Element').count())
    elementframe = elementframe.withColumn('Procent',(round(elementframe[1]/numberOfCharcters*100,2)))
    elementframe.show(truncate=False)

    return (dataframe,nationframe,elementframe)


def load_data(data):
    '''
    Opretter nogle parquet filer til dataframes, hvis de ikke allerede ekistere.
    '''
    createPieGraf(data[1])
    
    if not os.path.exists('GenshinChr.parquet'):
        data[0].write.parquet('GenshinChr.parquet') 
        data[1].write.parquet('GenshinNations.parquet') 
        data[2].write.parquet('GenshinElements.parquet') 
         
    else:
        if checkIfNewer(data[0]):
            data[0].write.mode("overwrite").parquet('GenshinChr.parquet')
            data[1].write.mode("overwrite").parquet('GenshinNations.parquet')
            data[2].write.mode("overwrite").parquet('GenshinElements.parquet')


def checkIfNewer(data):
    '''
    Checkker om det nye data har en en nyer karakter en den gamle data. For hvis den har det skal
    den nye data erstatte den gamle. Grunden til jeg ikke bare kun tilføjere de nye karaktere
    er fordi at spillet nogle gange ændre ting på gamle karaktere.
    '''
    oldChrDF=spark.read.parquet("GenshinChr.parquet")
    newestChrInOldVersion = data.select('ReleaseDate').collect()[0]
    newestChrInNewData = oldChrDF.select('ReleaseDate').collect()[0]
    if (newestChrInNewData > newestChrInOldVersion):
        return True
    
    return False


def createPieGraf(data):
    '''
    skaber en rund graf med data om hvilke lande karaktere kommer fra procent mæssigt. Den gemmer denne graf som Pie.png, 
    hvis den fil allerede eksistere erstatter den filen. 
    '''
    fig = plt.figure(figsize=(6,6))
    ax = fig.add_axes([0.1, 0.1, 0.8, 0.8])
    labels = [val.Nation for val in data.select('Nation').collect()]
    fracs = [val.Procent for val in data.select('Procent').collect()]
    pies = ax.pie(fracs, labels=labels, autopct='%1.1f%%')
    
    for w in pies[0]:
        w.set_gid(w.get_label())
        w.set_edgecolor("none")
        
    plt.savefig("Pie.png",dpi=900)


def main():
    '''
    et while true loop med de forskelige funktioner der køre ca en gang om måneden.
    '''
    while(True):
        url = 'https://genshin.jmp.blue/characters/all'
        data = extract_data(url)
        transformedData = transform_data(data)
        load_data(transformedData)
        time.sleep(30*24*60*60*60)


if __name__ == '__main__':
    main()