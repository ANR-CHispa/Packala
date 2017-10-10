# -*- coding: utf-8 -*-
"""
Created on Thu Jul 27 09:10:29 2017

@author: Michael Nauge
"""

#description : Script permettant de réaliser les operations de bases sur nakala via l'api REST
#creation de collection, creation de data, mise a jour de metadata
#requete get, post, put, etc

import pycurl
import certifi
import json
import os


#lib pour les requetes GET 
import urllib.request

#lib pour le parsing de xml ou html
from bs4 import BeautifulSoup


try:
    from io import BytesIO
except ImportError:
    from StringIO import StringIO as BytesIO
    
try:
    # python 3
    from urllib.parse import urlencode
except ImportError:
    # python 2
    from urllib import urlencode
    
from math import ceil
    
#https://www.saltycrane.com/blog/2012/08/example-posting-binary-data-using-pycurl/

class objConfigNakalaRestApi:
    """ class contenant les parametres pour la rest api 
    cela permet uniquement de réduire artificiellement le nombre de parametre a passer aux fonctions
    """
    
    baseUrl=''
    email=''
    keyApi=''
    project=''

    def __init__(self, baseUrl, email, keyApi, project):
        self.baseUrl=baseUrl
        self.email=email
        self.keyApi=keyApi
        self.project=project
        

def createCollection(objConfigNkl, pathCollectionZip):
    """
    fonction permettant de creer une collection sur un nakala distant via l'api rest
    
    :param objConfigNakalaRestApi: un sac de donnée contenant les variables minimales pour l'utilisation de l'api rest
    :type objConfigNakalaRestApi: objConfigNakalaRestApi. 
    
    :param pathCollectionZip: le chemin vers le .zip contenant les metas descriptives de la collection
    :type pathCollectionZip: str. 
    
    :returns:  l'id attribué a cette collection
    :raises: pycurl error
    """
    
    
    myUrl = objConfigNkl.baseUrl+"collection?email="+objConfigNkl.email+"&key="+objConfigNkl.keyApi+"&project="+objConfigNkl.project
    
    c = pycurl.Curl()
    c.setopt(c.VERBOSE, True)
    
    buffer = BytesIO()
    
    #pour un test local en utilisant l'utilitaire netcat en mode écoute
    #myUrl = 'http://localhost:1234'
    #lancer netcat nc - l -p 1234
    c.setopt(c.URL, myUrl)
        
    c.setopt(pycurl.CAINFO, certifi.where())
    
    c.setopt(pycurl.POST, 1)
    c.setopt(pycurl.HTTPHEADER, ['Content-Type: application/octet-stream'])
    
    filesize = os.path.getsize(pathCollectionZip)
    c.setopt(pycurl.POSTFIELDSIZE, filesize)
    fin = open(pathCollectionZip, 'rb')
    c.setopt(pycurl.READFUNCTION, fin.read)

    c.setopt(c.WRITEDATA, buffer)
    
    c.perform()
        
    responseStr = str(buffer.getvalue().decode('utf-8'))

    dicJson = json.loads(responseStr)
    
    #for k in dicJson:
    #    print('k :',k, dicJson[k])
    
    # HTTP response code, e.g. 200.
    print('Create Collection Status: %d' % c.getinfo(c.RESPONSE_CODE))

    c.close()    
    
    return dicJson["handleId"]

 

def updateCollection(objConfigNkl, handleCollection, pathCollectionZip):
    """
    fonction permettant de modifier une collection sur un nakala distant via l'api rest
    
    :param objConfigNakalaRestApi: un sac de donnée contenant les variables minimales pour l'utilisation de l'api rest
    :type objConfigNakalaRestApi: objConfigNakalaRestApi. 
    
    :param handleCollection: le handle de la collection a modifier (attention ce handle doit également etre present dans nkl:identifier dans le xml dans le zip)
    :type handleCollection: str. 
    
    :param pathCollectionZip: le chemin vers le .zip contenant les metas descriptives de la collection
    :type pathCollectionZip: str. 
    
    :raises: pycurl error
    """
    
    myUrl = objConfigNkl.baseUrl+"collection/"+handleCollection+"?email="+objConfigNkl.email+"&key="+objConfigNkl.keyApi+"&project="+objConfigNkl.project
    
    c = pycurl.Curl()
    c.setopt(c.VERBOSE, True)
    
    buffer = BytesIO()
    
    #pour un test local en utilisant l'utilitaire netcat en mode écoute
    #myUrl = 'http://localhost:1234'
    #lancer netcat nc - l -p 1234
    c.setopt(c.URL, myUrl)
        
    c.setopt(pycurl.CAINFO, certifi.where())
    
    c.setopt(pycurl.HTTPHEADER, ['Content-Type: application/octet-stream'])
    
    filesize = os.path.getsize(pathCollectionZip)
        
    c.setopt(pycurl.POSTFIELDSIZE, filesize)
    fin = open(pathCollectionZip, 'rb')
    c.setopt(pycurl.READFUNCTION, fin.read)

    c.setopt(c.WRITEDATA, buffer)
    
    c.setopt(pycurl.CUSTOMREQUEST, "PUT")
        
    c.perform()
        
    responseStr = str(buffer.getvalue().decode('utf-8'))

    dicJson = json.loads(responseStr)
    
    print('Update Collection Status: %d' % c.getinfo(c.RESPONSE_CODE))

    c.close()    
    
    return dicJson["handleId"]



