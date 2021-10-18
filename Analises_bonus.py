#!/usr/bin/env python
# coding: utf-8

# In[3]:


import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
import os
import requests


# In[27]:



data2009 = []
data2010 = []
data2011 = []
data2012 = []

""" 

Tive que fazer os downloads por partes e ir apagando
as variáveis conforme não fossem mais necessárias.
 
"""

""" 

Da forma como estavam escritos os arquivos json, 
a leitura só foi possível por linhas

"""
url ="https://s3.amazonaws.com/data-sprints-eng-test/data-sample_data-nyctaxi-trips-2009-json_corrigido.json"
trip2009 = pd.read_json(url, lines = True)


# In[28]:


url ="https://s3.amazonaws.com/data-sprints-eng-test/data-sample_data-nyctaxi-trips-2010-json_corrigido.json"
trip2010 = pd.read_json(url, lines = True)


# In[29]:


url ="https://s3.amazonaws.com/data-sprints-eng-test/data-sample_data-nyctaxi-trips-2011-json_corrigido.json"
trip2011 = pd.read_json(url, lines = True)


# In[31]:


url ="https://s3.amazonaws.com/data-sprints-eng-test/data-sample_data-nyctaxi-trips-2012-json_corrigido.json"
trip2012 = pd.read_json(url, lines = True)


# In[33]:


#Selecionando somente as colunas que serão necessárias nas análises
trip2009=trip2009[['pickup_datetime','dropoff_datetime',
                   'pickup_longitude', 'pickup_latitude',
                   'dropoff_longitude', 'dropoff_latitude']]
trip2010=trip2010[['pickup_datetime','dropoff_datetime',
                   'pickup_longitude', 'pickup_latitude',
                   'dropoff_longitude', 'dropoff_latitude']]
trip2011=trip2011[['pickup_datetime','dropoff_datetime',
                   'pickup_longitude', 'pickup_latitude',
                   'dropoff_longitude', 'dropoff_latitude']]
trip2012=trip2012[['pickup_datetime','dropoff_datetime',
                   'pickup_longitude', 'pickup_latitude',
                   'dropoff_longitude', 'dropoff_latitude']]


# Após os dados serem filtrados, concatenei os arquivos

# In[34]:


trips_all=pd.concat([trip2009,trip2010,trip2011,trip2012])


# ## RESOLVENDO QUESTAO 1 - BONUS
# 
# **1.  Qual o tempo médio das corridas nos dias de sábado e domingo**
# 

# In[35]:
# Selecionei as colunas de interesse
trips_delta=trips_all[['pickup_datetime','dropoff_datetime']]
# mudei pro formato de data
trips_delta[['dropoff_datetime','pickup_datetime']] = trips_delta[['dropoff_datetime','pickup_datetime']].apply(pd.to_datetime)
# Subtraí a hora de chegada menos saída e passei pra minutos
trips_delta['Delta_min'] = (trips_delta['dropoff_datetime'] - trips_delta['pickup_datetime']).dt.seconds/60
# Descobri o dia da semana de quando o passageiro entrou no carro
trips_delta['day_of_week'] = pd.to_datetime(trips_delta['pickup_datetime'])
trips_delta['day_of_week'] = trips_delta['day_of_week'].dt.day_name()

# Selecionando somente as corridas de final de semana
trips_weekends=trips_delta.loc[((trips_delta['day_of_week']=='Sunday')|(trips_delta['day_of_week']=='Saturday')),['Delta_min']]


mean_weekend=trips_weekends['Delta_min'].mean()

#plotando as distancias 
y1=trips_weekends['Delta_min']
x1=pd.Series(np.linspace(0,len(y1),len(y1)))
y2=mean_weekend


fig=plt.figure()
ax=fig.add_subplot(111)
ax.set(title='Trip duration on weekends')
plt.xlabel('Trips')
plt.ylabel('Trip Duration in minutes')
plt.axhline(y=y2,color='r',linewidth=3,label='Mean time duration= '+str(round(mean_weekend,2) )+'min')
plt.legend()
ax.plot(x1,y1,'bo', markersize=0.005)


# ## RESOLVENDO QUESTAO 2 - BONUS
# 
# **1.  Fazer uma visualização em mapa com latitude e longitude de pickups and dropoffs no ano
# de 2010**
# 

# In[36]:
# Selecionei as colunas de interesse
trips_pickups=trip2010[['pickup_longitude', 'pickup_latitude']]
trips_dropoffs=trip2010[['dropoff_longitude', 'dropoff_latitude']]



fig = plt.subplots(figsize =(8, 4)) 
ax.set(title='Geolocal trips in 2010')

plt.scatter(x=trips_dropoffs['dropoff_longitude'], y=trips_dropoffs['dropoff_latitude'],color='b',label ='Dropoffs')
plt.scatter(x=trips_pickups['pickup_longitude'], y=trips_pickups['pickup_latitude'],color='r',label ='Pickups')

plt.xlabel('Longitude')
plt.ylabel('Latitude')
plt.legend(loc='center right')
plt.show()







