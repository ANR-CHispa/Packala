#!/usr/bin/env python
# -*- coding: utf-8 -*-
#python 3 

#author : Nauge Michael 
#description : script contenant plusieurs outils de traitement de fichier CSV
#par exemple sauvegarder toutes les images distantes stockees dans un fichier CSV


import urllib.request

import csv
import sys

import re

import os

import time

import glob



#pour outrepasser le nombre de caractere par cellule
csv.field_size_limit(sys.maxsize)

def saveWebImagesFromCSV(readerCSV, ext, pathOut): 
    """Cette fonction parcours toutes les lignes du fichier CSV (ouvert par open puis reader) passé en parametre.
    pour chaque ressource de type image 
    une requete get est effecuée afin de recuperer cette ressource distante 
    puis elle est sauvegardé localement dans le dossier specifié
    
    :param readerCSV: un fichier CSV deja ouvert
    :type readerCSV: readerfile. 
    
    :param ext: permet le filtrage des images suivant leur extension (jpg)
    :type ext: str.   
    

    :returns: la list des images qui n'ont pas pu etre sauvegardées
    :raises: urllib.URLError
    """
    
    
    listUrlImgs = []
    
    for i, row in enumerate(readerCSV):
        if not(i == 0):
            #print(row)
            for column in row:
                #print(column)
                
                #il n'est pas possible d'avoir une virgule dans une url
                #si il y en a c'est surement quil y a plusieurs url dans la meme column
                for v in column.split(','):
                    for m in re.findall("(http:.*/(.*\.(jpg|png|gif)))", v): 
                        #print(m[0])
                        listUrlImgs.append(m[0]) 
                    
    
    listRes = saveWebImagesFromListUrl(listUrlImgs,pathOut)
    print ('nb images non recuperees :', len(listRes))
    
    return listRes
    

def saveWebImagesFromListUrl(listUrl, pathOut):
    """
    :returns: la list des images qui n'ont pas pu etre sauvegardées
    :raises: urllib.URLError
    """
    
    #on fait une copie de la liste 
    #que l'on va progressivement supprimmer
    #pour savoir quelles images n'ont pas été sauvegardé
    listRes = listUrl[:]
    
    print('nb image a recupere ',len(listUrl))
    
    for urlImage in listUrl:
        
        listRes.pop(0)
        
        filenameImg = urlImage.split('/')[-1]
        
        pathFileOut = pathOut+filenameImg
        
        #pour ne pas aller recuperer plusieur fois la meme image on test si elle existe deja en local
        if not(os.path.isfile(pathFileOut)):
            #attention, si le nom du fichier distant est le meme mais que lurl avant ete differente
            #ce nest pas bien geree.... L image ne sera pas recupere...
        
            print(filenameImg)
            try:
                urllib.request.urlretrieve(urlImage, pathFileOut)
            except:
                print('probleme de connexion...ou de sauvegarde de fichier...')
                        
    return listRes
   
 
    
def test_saveWebImagesFromCSV():
    """
    fonction de test de la fonction saveWebImagesFromCSV
    """
    fileCSVIn = "./../../Guarnido/03-metadatas/extractionDepuisSiteExistant/dirOutCSVGuarnido/merge/Guarnido_part1-12.csv"
    
    ext = "jpg"
    pathOut = "./../../Guarnido/02-numerisation/compress-Jpg/"
    
    f = open(fileCSVIn, encoding='utf-8')
    readerCSV = csv.reader(f) 
    #on le fait en boucle car sur certains site on est bloqué si on fai  trop de requetes rapidement
    
    
    encore = True
    
    while encore:
        try:
            listRes = saveWebImagesFromCSV(readerCSV, ext, pathOut)
            if len(listRes)==0:
                encore = False
            
            
        except:
            time.sleep(20)


    
    
def remappForOmekaImportCSV(pathFileCSV_Source, pathFileCSV_Mapp, pathFileCSV_Out):
    """
    fonction permettant de realiser une conversion des colonnes en utilisant un fichier de mapping.
    Ex: Disposant d'un tableur contenant les colonnes nommees : "etat document", "nombre de page", "date de publication"
    et que je souhaite un tableur de sortie avec les colonne : "Dublin Core:Date", "Dublin Core:Description"
    je creer un tableur de mapping de la forme "etat document":"Dublin Core:Description";"nombre de page":"Dublin Core:Description";"date de publication":"Dublin Core:Date"
    
    
    :returns: None
    :raises: IOerror (si probleme de d'ouverture de fichier) 
   
    """


    separator = "|"
    #on commence par lire le fichier de mapping
    #normalement il y a deux lignes
    #la ligne 0 contenant les champs source
    #la ligne 1 contenant les champs destination
    dicoMapp = {}
    with open(pathFileCSV_Mapp,encoding='utf-8') as csvfileMapp:
        readerMapp = csv.DictReader(csvfileMapp, delimiter=';')
        #on peut obtenir un dico par ligne
        countLine = 1
        for dicMappCur in readerMapp:
            #print("le mapping des champs : ",dicMappCur)
            countLine+=1
            
            if countLine==2:
                dicoMapp = dicMappCur
            
        if not(countLine==2):
            print("error [FileCSV_Mapp] : nb ligne presente : ",str(countLine)," attendu :2" )
     
    """
    #dans le cas present il sera plus interressant que les clefs soit la value et inversement
    #mais pour le tableur c'était plus logique pour humain dans lautre sens....
    inv_dicoMapp = {v: k for k, v in dicoMapp.items()}       
    """
    
    #on peut maintenant lire le CSV source pour le convertir
    #et ouvrir le CSV out pour sauvegarder la conversion
    with open(pathFileCSV_Source, encoding='utf-8') as csvfileSource:
        readerSource = csv.DictReader(csvfileSource, delimiter=';')
        
        
        #on ouvre le fichier Out
        with open(pathFileCSV_Out, 'w', encoding='utf-8',newline="\n") as csvfileOut:
                        
            
            listChampDistinctTriees= []
            listChampDistinct = set(dicoMapp.values())
            listChampDistinctTriees = sorted(listChampDistinct)
                
            #on peut obtenir un dico par ligne
            countLine = 0
            for dicSourceCur in readerSource:
                countLine+=1
                
                #cas particulier pour la premiere ligne
                if countLine==1:           
                #on va commencer par verifier que tous les champs de ce fichier d'entree
                #sont present dans le dictionnaire de mapping
                #(quil ne manque pas une clef ce qui poserai probleme pdt la conversion...)
                #dans le dico de Mapping
                                                
                    if not(dicSourceCur.keys()==dicoMapp.keys()):
                        raise ValueError("error [FileCSV Source] : probleme de champs present dans pathFileCSV_Source et inexistants dans FileCSV_Mapp")
                        
                    else:
                        
                        #on a egalite de champs donc on peut faire la copie
                        csvfileOut = csv.writer(csvfileOut, delimiter=';')
                        
                        #ecriture de la premiere dans le fichier CSV de sortie
                        csvfileOut.writerow(listChampDistinctTriees)
                            
                #maintenant nous traitons toutes les lignes de la meme facon
                
                rowOut = []
                #pour chaque champs de sortie 
                #on va regarder dans le dico de mapping  puis chercher dans le dicSourceCur
                
                for champCur in listChampDistinctTriees:
                    champOutCur = ""
                    for keyOut in dicoMapp:
                        if dicoMapp[keyOut] == champCur:
                            champOutCur+= dicSourceCur[keyOut]+separator
                            
                    rowOut.append(champOutCur)
                            
                csvfileOut.writerow(rowOut)
                    
         


def test_remappForOmekaImportCSV():
    """
    fonction de test de remappCSV
    """
    pathFileCSV_Source = "./remapp/Guarnido-All.csv"
    pathFileCSV_Mapp = "./remapp/mappingOmeka.csv"
    pathFileCSV_Out ="./remapp/Guarnido-remapped.csv"
    
    remappForOmekaImportCSV(pathFileCSV_Source, pathFileCSV_Mapp,pathFileCSV_Out)
    
    
    

def orientedOmekaCsv2LineByFileCsv(pathFileCSV_Source, pathFileCSV_Out, fileColumnName, additionalColumnName, columnUsedForCollection ) :
    """
    fonction permettant de transformer un CSV de données orienté omeka (plusieurs fichiers pour un meme item)
    vers un csv orienté nakala une ligne par fichier
    plus précisement dans omeka il y a 3 types: des collections, des items, et des images
    nativement une collection ne peut pas avoir de collection fille mais peut contenir plusieurs items et images
    tandis un item peut contenir une ou plusieurs images.....
    
    Dans nakala il n'est question que de data et de collection.
    Chaque image est une data, il n'est pas question d'item,
    pour relier plusieurs images entre elle sur le principe de l'item
    il faut utiliser les collections.
    par contre dans nakala une collection peut contenir plusieurs collections!
    
    Dans nakala une méta donnée doit être liée à une data
    si la data n'existe pas la métadata non plus...
    Ce traitement fait également disparaitre les lignes sans fichiers de data ...
    
    :param pathFileCSV_Source: un fichier CSV deja ouvert
    :type pathFileCSV_Source: readerfile. 
    
    :param ext: permet le filtrage des images suivant leur extension (jpg)
    :type ext: str.   
    

    :returns: none
    :raises: urllib.URLError
    """
    
    #on ouvre le fichier d'entree
    #et le fichier de sortie
    
    with open(pathFileCSV_Source, encoding='utf-8') as csvfileSource:
        readerSource = csv.DictReader(csvfileSource, delimiter=';')
        #on vient de lire tout le fichier est il est stoqué dans un dico
        #on peut traiter chaque ligne du dico
        
            
     
        countItemWithImage = 0
        countItemWithNoneStr = 0
        countItemWithNone = 0
        countImages = 0
        countMotherCollection = 0
        #on ouvre le fichier Out
        with open(pathFileCSV_Out, 'w', encoding='utf-8',newline="\n") as csvfileOut:
            csvfileOut = csv.writer(csvfileOut, delimiter=';')

            
            #on parcourir toute les lignes du fichier d'entree
            #pour la reecrire une ou plusieur fois dans le fichier de sortie
            #on ecrira plusieurs fois la ligne si en entrée il a plusieurs images dans la column file
            
            countLine = 0
            for dicSourceCur in readerSource:
                
                listChampDistinctTriees= []
                listChampDistinct = set(dicSourceCur.keys())
                listChampDistinctTriees = sorted(listChampDistinct)
            
                countLine+=1
                
                #cas particulier pour la premiere ligne
                # va reecrire les noms de column du fichier dentree plus le nom de la comun additionnel
                if countLine==1:
                    

                    rowOut = list(listChampDistinctTriees)
                    rowOut.append(additionalColumnName)
                    rowOut.append('linkedInCollection')
                    
                    csvfileOut.writerow(rowOut)
                    #on creer un dico de sortie avec une column de plus qu'en entree!
                 
                #pour chaque ligne de contenu:
                #on analyse la column File
                #print(dicSourceCur[fileColumnName])
                input_line = dicSourceCur[fileColumnName]
                
                #quand il n'y a pas de fichier pour le moment on ne les recreer pas...
                #car on ne sais pas comment les gerer dans nakala...
                if input_line=='none' :
                    countItemWithNoneStr+=1
                    #print('ya du none en str')
                
                elif input_line==None:
                    countItemWithNone+=1
                    #print('ya du none en vide')
                    
                else:
                    countItemWithImage+=1
                    listFile = [s.strip() for s in input_line[1:-1].split(',')]
                    countImages+=len(listFile)
                    #print(listFile)
                    
                    if len(listFile)==0:
                        countItemWithNone+=1
                    else:
                        countMotherCollection+=1
                        #on va creer une une ligne dans le fichier de sortie par fichier trouve
                        countPart = 0
                        for file in listFile:
                            
                            countPart+=1
                            rowOut = []
                            for columName in listChampDistinctTriees:
                                
                                
                                if not(columName==fileColumnName):
                                    rowOut.append(dicSourceCur[columName])
                                    #il ne faut pas traiter la column file comme les autres du coup...
                                
                            rowOut.append([file.replace("'",'')])
                            rowOut.append(countPart)
                            if len(listFile)>1:
                                nameCollection = dicSourceCur[columnUsedForCollection]
                                nameCollection = nameCollection.replace("[",'')
                                nameCollection = nameCollection.replace("]",'')
                                nameCollection = nameCollection.replace("'",'')
                                rowOut.append(['collection '+nameCollection])
                            else:
                                rowOut.append('')
                            
                            
                            csvfileOut.writerow(rowOut)
                        
                    
                    
                    
        print('countItemWithImage',countItemWithImage)
        print('countItemWithNoneStr',countItemWithNoneStr)
        print('countItemWithNone',countItemWithNone)      
        print('total items',str(countItemWithImage+countItemWithNoneStr+countItemWithNone))
        print('nbImagevue',countImages)
        print('nbMotherCollection',countMotherCollection)
                    
                   
                
            
    
def test_orientedOmekaCsv2LineByFileCsv():
    """
    fonction de test de remappCSV
    """
    pathFileCSV_Source = "./../../Guarnido/03-metadatas/extractionDepuisSiteExistant/dirOutCSVGuarnido/merge/Guarnido_part1-12.csv"
    pathFileCSV_Out ="./../../Guarnido/03-metadatas/extractionDepuisSiteExistant/dirOutCSVGuarnido/merge/Guarnido_part1-12_lineByFile.csv"
    
    fileColumnName = "file"
    additionalColumnName = "numeroDeFolio"
    columnUsedForCollection = "Title"
    
    
    orientedOmekaCsv2LineByFileCsv(pathFileCSV_Source, pathFileCSV_Out, fileColumnName, additionalColumnName, columnUsedForCollection)    
  
    
    
def creatLineFromMultiValuesInColumn(inputFile, outputFile, columnName, separator, columnNameNumbered):
    """
    fonction permettant de transformer un CSV de données contenant plusieurs fichiers pour un meme item
    vers un csv orienté contenant une ligne par fichier.
    Cas pratique : Dans omeka il y a 3 types: des collections, des items, et des images
    nativement une collection ne peut pas avoir de collection fille mais peut contenir plusieurs items et images
    tandis qu'un item peut contenir une ou plusieurs images.....
    
    Dans nakala il n'est question que de data et de collection.
    Chaque image est une data, il n'est pas question d'item,
    pour relier plusieurs images entre elle sur le principe de l'item
    il faut utiliser les collections.
    par contre dans nakala une collection peut contenir plusieurs collections!
    
    Dans nakala une méta donnée doit être liée à une data
    si la data n'existe pas la métadata non plus...
    Ce traitement fait également disparaitre les lignes sans fichiers de data ...

    
    :param inputFile: le chemin vers le fichier de data d'entree
    :type inputFile: str. 
    
    :param outputFile: le chemin vers le fichier de data de sortie (contenant les nouvelles lignes et colonnes)
    :type inputFile: str. 
    
    :param columnName: le nom de la colonne contenant plusieurs valeurs
    :type: columnName: str
    
    :param columnNameNumbered: le nom de la colonne permettant d'acceuillir un comptage (par exemple 'Page ' dans le cas de plusieurs pages d'un même ouvrage). Cela permet de garder une trace de la numérotation (ou plutôt de l'ordonnacement des fichiers d'origine)
    :type: columnNameNumbered: str
    
    :raise : IO exception
    
    """
        
    writerOut = None
    outFile = None
    with open(inputFile, encoding='utf-8', newline="\n") as dataFile:
        reader = csv.DictReader(dataFile, delimiter=';')
        countLine = 2 #on commence a 2 pour etre cohérent avec les lignes du tableau de donnée quand on l'ouvre sous Excell/libreOffice
        
        for dicDataCur in reader:
            #on gere l'ouverture et l'ecriture de la permiere ligne du fichier de sortie
            #---------------------------
            if countLine== 2 :
                listNomCol = reader.fieldnames
                listNomCol.append(columnNameNumbered)
                
                outFile = open(outputFile, 'w', encoding='utf-8', newline="\n") 
                writerOut = csv.DictWriter(outFile, delimiter=';', fieldnames = listNomCol)
                writerOut.writeheader()

            
            dicDataCur[columnNameNumbered] = ''
            #on recupere le contenu de la column a traiter
            if dicDataCur[columnName] == None or dicDataCur[columnName] == 'None' or dicDataCur[columnName] =='':
                writerOut.writerow(dicDataCur)
            else:
                #on est sure d'avoir du contenu, on découpe grace au seprator
                listValuesInColumnName = dicDataCur[columnName].split(separator)
                if len(listValuesInColumnName) > 1 :
                    
                    countTour = 1
                    dicTmp  = dicDataCur.copy()
                    for value in listValuesInColumnName:
                        dicTmp[columnNameNumbered] = str(countTour)
                        dicTmp[columnName] = value
                        writerOut.writerow(dicTmp)
                        countTour+=1
                    
                else:
                    writerOut.writerow(dicDataCur)
            
                
            countLine+=1
                
    outFile.close() 
    
def creatLineFromMultiValuesInColumn_test():
    pathDataFile = "C:/Users/Public/Documents/MSHS/workflow/Guarnido/03-metadatas/"
    inputFile = pathDataFile+"Guarnido-revise.csv"
    outputFile = pathDataFile+"Guarnido-revise_nklSplitFile.csv"
    
    columnName = "file"
    separator = ", "
    columnNameNumbered = "Page"
    
    creatLineFromMultiValuesInColumn(inputFile, outputFile, columnName, separator, columnNameNumbered)  

def fillValueFromCondition():
    #TODO:
    pass
    

    
    
def changeColumnFileWithLocalPath(pathFileCSV_Source, pathFileCSV_Out, fileColumnName, localPath):
    """
    fonction permettant de changer le contenu d'une column contenant une url vers un fichier distant
    en specifiant un chemin vers un fichier local.
    C'est aussi l'occasion de vérifier la présence en local de ce fichier et de lever une exception si le fichier n'existe pas.
    
    :param pathFileCSV_Source: le chemin vers le fichier CSV contenant des métadonnées et une column contenant des chemins fichiers a modifier
    :type pathFileCSV_Source: str.   
    
    :param pathFileCSV_Out: le chemin vers le fichier CSV de sortie ayant le même contenant que le FileCSV_Source mais dont les chemins des fichiers sont mis a jours
    :type pathFileCSV_Out: str.   
    
        
    
    :param localPath: le chemin vers les fichier locaux
    :type localPath::str
    
    :returns: None
    :raises: IOerror (si probleme d'acces aux fichiers) 
    """  
    #on ouvre le fichier d'entree
    #et le fichier de sortie
    
    with open(pathFileCSV_Source, encoding='utf-8') as csvfileSource:
        readerSource = csv.DictReader(csvfileSource, delimiter=';')
        #on vient de lire tout le fichier est il est stoqué dans un dico
        #on peut traiter chaque ligne du dico
        
        #on ouvre le fichier Out
        with open(pathFileCSV_Out, 'w', encoding='utf-8',newline="\n") as csvfileOut:
            csvfileOut = csv.writer(csvfileOut, delimiter=';')
            
            #on parcourir toute les lignes du fichier d'entree
            #pour la reecrire une ou plusieur fois dans le fichier de sortie
            #on ecrira plusieurs fois la ligne si en entrée il a plusieurs images dans la column file
            
            countLine = 0
            for dicSourceCur in readerSource:
                
                
                listChampDistinctTriees= []
                listChampDistinct = set(dicSourceCur.keys())
                listChampDistinctTriees = sorted(listChampDistinct)
            
                countLine+=1
                
                #cas particulier pour la premiere ligne
                # va reecrire les noms de column du fichier dentree plus le nom de la comun additionnel
                if countLine==1:
                    rowOut = list(listChampDistinctTriees)                  
                    csvfileOut.writerow(rowOut)
                
                
                rowOut = []
                for columName in listChampDistinctTriees:
                    
                    if columName==fileColumnName:
                        #il ne faut pas traiter la column file comme les autres du coup..
                        
                        input_line = dicSourceCur[columName]
                        listpathFileCur = [s.strip() for s in input_line[1:-1].split(',')]
                        #normalement un seul fichier!
                        for pathFileCur in listpathFileCur:
                            
                            #extraire uniquement le chemin vers le fichier normalement distant
                            head, tail = os.path.split(pathFileCur) 
                            
                            #pour le remplacer par le chemin local
                            #on replace head (chemin distant) par localPath
                            newpathFileName = localPath+tail
                            newpathFileName = newpathFileName.replace("'",'')
                                                            
                            #on en profite pour tester l'existance du fichier en local et lever une exeption si ce n'est pas le cas
                            if not(os.path.isfile(newpathFileName)):
                                raise IOError("le fichier ",newpathFileName, "n'existe pas localement")
                            
                            rowOut.append([newpathFileName])
                    else:
                        rowOut.append(dicSourceCur[columName])
                                                        
                    
                csvfileOut.writerow(rowOut)
    
    
    
    
    
def test_changeColumnFileWithLocalPath():
    pathFileCSV_Source = "./../../Guarnido/03-metadatas/extractionDepuisSiteExistant/dirOutCSVGuarnido/merge/Guarnido_part1-12_lineByFile.csv"
    pathFileCSV_Out = "./../../Guarnido/03-metadatas/extractionDepuisSiteExistant/dirOutCSVGuarnido/merge/Guarnido_part1-12_lf_localPath.csv"
    fileColumnName = "file"
    localPath = "./../../Guarnido/02-numerisation/compress-Jpg/"
    
    changeColumnFileWithLocalPath(pathFileCSV_Source, pathFileCSV_Out, fileColumnName, localPath)
    
   
    
    
    
def checkColumnExist(listColumn, pathFile):
    """
    fonction permettant de vérifier la présence de colonnes dans un fichier cible
    la fonction renvoie True si toutes les colonnes attendues sont présentes 
    sinon elle retourne False en fesant des print sur les colonnes non trouvées
    
    
    :param listColumn: la liste des noms de colonnes dont on chercher a vérifier la présence
    :type listColumn: list[str]. 
    
    :param pathFile: le chemin vers le fichier a vérifier
    :type pathFile: str. 
    
    :returns: bool 
    :raises: IO exception (probleme d'ouverture des fichiers: chemin non valide ou fichier locké)
    
    """

    try :

        with open(pathFile, encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=';')
            countLine = 1 
            
            for row in reader:
                if countLine == 1 :
                    #ici nous disposons d'un dico dans la variable row ou chaque key contient un nom de colonne
                    #on peut donc vérifier la presence de chaque nom de colonne présent
                    #dans listColumn avec les key de row
                    
                    for nameColumn in listColumn:
                        if nameColumn in row:
                            pass
                        else:
                            print("[error dataFile] : la colonne obligatoire",nameColumn,"n'est pas présente")
                            csvfile.close()
                            return False
                        
                    csvfile.close()
                    return True
                        
    except Exception as e:
            print(e)
            csvfile.close()
            return False  
        
        
def checkColumnExist_test():    
    
    datasPath = "C:/Users/Public/Documents/MSHS/workflow/Guarnido/03-metadatas/"
    #datasFilename = "GuarnidoShort.csv"
    datasFilename = "GuarnidoShortNklReady.csv"
    
    datasPath = datasPath+datasFilename
    
    listColonneACheck = ['Linked in collection','Nkl dataType','Nkl dataFormat','Nkl hdl root collection','Nkl hdl root collection','Nkl hdl branch collection','Nkl hdl leaf collection','Nkl hdl leaf data']

    res = checkColumnExist(listColonneACheck, datasPath)
    
    if res:
        print('le datafile contient les colonnes obligatoires')
    else:
        print('[error] : le dataFile ne contient pas toutes les colonnes obligatoires')
        print('[help] : pensez à utiliser la fonction transformDataFileForeNakalaPush()')
    
    
def checkMappingFileWithDataFile(pathMappingFile,pathDataFile):
    """
    Cette fonction permet de tester la validité du fichier de mapping en le comparant au fichier de données
    
    Un fichier de mapping doit contenir 2 lignes. 
    La 1ere ligne contient des Noms de métas données (ex: Titre, Description, Date, Licence) (au moins 1). 
    La 2ieme ligne contient la correspondance de chaque Métas donées vers un format ou une norme cible (ex: DC:Title, <TEI><Header><Title>?</Title></Header></TEI>) 

    Un fichier de mapping ne peut pas contenir de valeurs Blank ou None dans la premiere ligne
    Un fichier de mapping ne peut pas contenir de valeurs Blank dans la 2ieme ligne, mais peut contenir 'None' pour signifier explicitement que la méta ne doit pas etre mappé.
    
    Tous les Noms de métas contenus dans le fichier MappingFile doivent exister dans le fichier DataFile
    Tous les Noms de métas contenus dans le fichier DataFile n'existent pas obligatoire dans le fichier MappingFile. 
        Cependant un avertissement sera levé. Nous conseillons la présence de tous les Noms de colonnes du DataFile dans le MappingFile en spécifiant 'None' dans la 2ieme ligne du MappingFile.
        
    Un fichier de data doit contenir au moins deux lignes (la 1ere contenant les noms de colonnes de méta et la 2ieme les datas)
        
    :param pathMappingFile: le chemin vers le fichier de mapping
    :type pathMappingFile: str. 
    
    :param pathDataFile: le chemin vers le fichier de données
    :type pathDataFile: str. 
    
    :returns: bool 
    :raises: IO exception (probleme d'ouverture des fichiers: chemin non valide ou fichier locké)
    
    """
    
    #Un fichier de mapping doit contenir 2 lignes. 
    #on test en même temps la validité du chemin !
    try :
        
        dicMapping = None
        with open(pathMappingFile, encoding='utf-8') as csvfileMapp:
            readerMapp = csv.DictReader(csvfileMapp, delimiter=';')
            countLine = 1
            #on obtient un dico par ligne
            #un premier tour pour compter le nombre de ligne
            for row in readerMapp:
                countLine += 1

                
            if not(countLine==2):
                print("[error mappingFile] : le mappingFile ne contient pas 2 lignes! il en contient :", countLine)
                return False
            
            else:
                #on refait un tour pour tester la presence de Blank et None 
                #on commence par remmettre l'index de fichier à la premiere ligne avec la fonction seek
                countLine = 1
                csvfileMapp.seek(0)
                for row in readerMapp:
                    
                    #le check pour la 1ere
                    if countLine == 1 :
                        #print('row mapp ',row)
                        #La 1ere ligne contient des Noms de métas données (ex: Titre, Description, Date, Licence)
                        #(au moins 1). 
                        nbNomsCol = len(row)
                        if(nbNomsCol == 0):
                            print("[error mappingFile] : il n'y a pas de colonne")
                            return False
                        else:
                            #Un fichier de mapping ne peut pas contenir de valeurs None ou Blank dans la premiere ligne
                            #test du None
                            if 'None' in row:
                                print("[error mappingFile] : Un fichier de mapping ne peut pas contenir de valeurs None dans la premiere ligne")
                                return False
                            #test du Blank
                            elif '' in row:
                                print("[error mappingFile] : Un fichier de mapping ne peut pas contenir de valeurs Blank dans la premiere ligne")
                                return False
                            
                    #le check pour la 2eme ligne
                    if countLine == 2 :
                        #Un fichier de mapping ne peut pas contenir de valeurs Blank dans la 2ieme ligne, 
                        #mais peut contenir 'None' pour signifier explicitement que la méta ne doit pas etre mappé.
                        #test du Blank
                        
                        if '' in row.values():
                            print("[error mappingFile] : Un fichier de mapping ne peut pas contenir de valeurs Blank dans la 2ieme ligne")
                            return False      
                        
                        #on sauvegarde ce dictionnaire de mapping
                        dicMapping = row
                        
                    countLine += 1
                    
                csvfileMapp.close()
    
    except Exception as e:
        print(e)
        return False
        
    #Tous les Noms de métas contenus dans le fichier MappingFile doivent exister dans le fichier DataFile
    #on ouvre le fichier de Data
    try :
        
        dicData = None
        with open(pathDataFile, encoding='utf-8') as csvfileData:
            readerData = csv.DictReader(csvfileData, delimiter=';')
            countLine = 1
            #on obtient un dico par ligne
            #un premier tour pour compter le nombre de ligne
            for row in readerData:
                
                countLine += 1
                
            if countLine < 2:
                print("[error dataFile] : le dataFile doit contenir au minimum 2 lignes, il en contient :", countLine)
                return False   
            else:
                #on refait un tour pour tester la presence de Blank et None 
                #on commence par remmettre l'index de fichier à la premiere ligne avec la fonction seek
                countLine = 1
                csvfileData.seek(0)
                for row in readerData:
                    #le check pour la 1ere
                    if countLine == 1 :
                        #La 1ere ligne contient des Noms de métas données (ex: Titre, Description, Date, Licence)
                        #(au moins 1). 
    
                        nbNomsCol = len(row)
                        if(nbNomsCol == 0):
                            print("[error dataFile] : il n'y a pas de colonne")
                            return False
                        else:
                            #Un fichier de mapping ne peut pas contenir de valeurs None ou Blank dans la premiere ligne
                            #test du None
                            if 'None' in row:
                                print("[error dataFile] : Un fichier de données ne peut pas contenir de valeurs None dans la premiere ligne")
                                return False
                            #test du Blank
                            
                            elif '' in row:
                                
                                print("[error dataFile] : Un fichier de mapping ne peut pas contenir de valeurs Blank dans la premiere ligne")
                                return False
                            else:
                                #cette 1ere ligne est valide
                                #on sauvegarde ce dico pour le tester avec le dico de mapping
                                dicData = row
            csvfileData.close()
    except Exception as e:
        print(e)
        return False
    
    #Tous les Noms de métas contenus dans le fichier MappingFile doivent exister dans le fichier DataFile
    for keyInMapp in dicMapping.keys():
        if not(keyInMapp in dicData):
            print("[error mappingFile] :",keyInMapp, "n'existe pas dans le dataFile")
            
            #print(dicData)
            return False
    
    #Tous les Noms de métas contenus dans le fichier DataFile n'existent pas obligatoire dans le fichier MappingFile. 
    #    Cependant un avertissement sera levé. Nous conseillons la présence de tous les Noms de colonnes du DataFile dans le MappingFile en spécifiant 'None' dans la 2ieme ligne du MappingFile.
    for keyInData in dicData.keys():
        if not(keyInData in dicMapping):
            print("[warning mappingFile] :",keyInData,"n'existe pas dans le mappingFile mais est present dans le dataFile. C'est un oubli ?")
            #print("nous vous conseillons d'expliciter dans mappingFile toutes les colones présentes dans le dataFile, en precisant None dans la dexieme ligne de mappinfFile si vous ne souhaitez pas transferer cette colonne")
            
    
    #print("le dico de mappingValide:",dicMapping)
    #print("le dico de dataValide:",dicData)
    
    return True

   
def checkMappingFileWithDataFile_test():
    mappingPath = "C:/Users/Public/Documents/MSHS/workflow/Guarnido/03-metadatas/nakala/"
    #mappingFilename = "mappingNkl_badNbLines.csv"
    #mappingFilename = "mappingNkl_badfirstLineNone.csv"
    #mappingFilename = "mappingNkl_badfirstLineBlank.csv"
    #mappingFilename = "mappingNkl_badsecondline.csv"
    #mappingFilename = "mappingNklBadName.csv"
    #mappingFilename = "mappingNkl.csv"
    #mappingFilename = "mappingShortNklBadCorresp.csv"
    #mappingFilename = "mappingShortNklWarning.csv"
    mappingFilename = "mappingShortNkl.csv"
    mappingPath = mappingPath+mappingFilename
    
    
    datasPath = "C:/Users/Public/Documents/MSHS/workflow/Guarnido/03-metadatas/"
    #datasFilename = "Guarnido-revise1ligne_1lineByFile_bad1erlineNone.csv"
    #datasFilename = "Guarnido-revise1ligne_1lineByFile_bad1erlineBlank.csv"
    #datasFilename = "Guarnido-revise1ligne_1lineByFile.csv"
    
    datasFilename = "GuarnidoShort.csv"
    
    datasPath = datasPath+datasFilename
    
    res = checkMappingFileWithDataFile(mappingPath, datasPath)
    
    if res==True:
        print("le fichier de mapping est valide et compatible avec le fichier de données")
        
    else:
        print("le fichier de mapping n'est pas valide et/ou imcompatible avec le fichier de données")
    
    
"""
def sampleCodeReadWriteCSV():
    from tempfile import NamedTemporaryFile
    import shutil
    import csv

    filename = 'tmpEmployeeDatabase.csv'
    tempfile = NamedTemporaryFile(delete=False)

    with open(filename, 'rb') as csvFile, tempfile:
        reader = csv.reader(csvFile, delimiter=',', quotechar='"')
        writer = csv.writer(tempfile, delimiter=',', quotechar='"')
    
        for row in reader:
            row[1] = row[1].title()
            writer.writerow(row)
    
    shutil.move(tempfile.name, filename)
"""

def getMappingDicFromFile(pathMappingFile):
    """
    fonction permettant de récupérer un dictionnaire de mapping à partir d'un fichier CSV
    il est conseillé d'appeler cette focntion après une vérificatio par la fonction
    checkMappingFileWithDataFile()
    
    :param pathMappingFile: le chemin vers le fichier de mapping
    :type pathMappingFile: str. 
    
    :returns: Dictionnary 
    """
    dicMapping = None
    try :
        with open(pathMappingFile, encoding='utf-8') as csvfileMapp:
            readerMapp = csv.DictReader(csvfileMapp, delimiter=';')
            #normalement un fichier de mapping ne contient que 2 ligne les colonnes et les vlauers mappé
            #donc un tour de boucle
            for row in readerMapp:
                dicMapping = row
                break
            csvfileMapp.close()
            
    except Exception as e:
        print(e)
        csvfileMapp.close()
    return dicMapping
    


def checkRequireValueInDataFile(pathDataFile, columnName):
    """
    cette fonction permet de vérifier que toutes les lignes de data contiennent
    bien du contenu dans la column spécifié
    
    :param pathDataFile: le chemin vers le fichier de data
    :type pathDataFile: str. 
    
    :param columnName: le nom de la colonne pour laquelle on souhaite vérifier le contenu
    :type columnName: str. 
    
    :returns: bool 
    """
    
    try :
        with open(pathDataFile, encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=';')
            #normalement un fichier de mapping ne contient que 2 ligne les colonnes et les valuers mappé
            #donc un tour de boucle
            countLine = 1
            for row in reader:
                if row[columnName] == None or row[columnName].lower() == 'None' or row[columnName] =='':
                    print('[error DataFile]: pas de data ligne',countLine,"colonne",columnName)
                    csvfile.close()
                    return False
                
                countLine += 1
                
            csvfile.close()
            return True
            
    except Exception as e:
        print(e)
        csvfile.close()   
        return False
    
  
def checkPathFileInDataFile(pathDataFile, columnName):
    """
    Lorsqu'un fichier CSV contient des chemins vers des fichiers locaux,
    il peut etre utile de tester la présence de ces fichiers à l'emplacement indiqué!
    
    :param pathDataFile: le chemin vers le fichier de data
    :type pathDataFile: str. 
    
    :param columnName: le nom de la colonne contenant des chemins de fichier
    :type columnName: str. 
    
    :returns: bool 
    """
    res = True
    try :
        with open(pathDataFile, encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=';')
            countLine = 1
            for row in reader:
                if row[columnName]==None or row[columnName].lower()=='none' or row[columnName] =='':
                    #print('ligne : ',str(countLine),'pas de chemin de fichier')
                    pass
                else:
                    if not(os.path.isfile(row[columnName])):
                        print("le fichier ",row[columnName], "n'existe pas localement")
                        res = False
                countLine += 1
                
            csvfile.close()
            return res
            
    except Exception as e:
        print(e)
        csvfile.close() 
        res=False
        return res
    
    
    
    
 
def addColumnToFileWithoutValue(inputFile, outputFile, listColumnNames):
    """
    fonction permettant d'ajouter plusieurs colonnes (dont les valeurs seront vides) à un fichier CSV d'entree
    
    :param inputFile: le chemin vers le fichier de data d'entree
    :type inputFile: str. 
    
    :param outputFile: le chemin vers le fichier de data de sortie (contenant les nouvelles colonnes)
    :type inputFile: str. 
    
    :param listColumnNames: list de noms de colones
    :type: list['str']
    
    :raise : IO exception
    """

    writerOut = None
    outFile = None
    with open(inputFile, encoding='utf-8', newline="\n") as dataFile:
        reader = csv.DictReader(dataFile, delimiter=';')
        countLine = 2 #on commence a 2 pour etre cohérent avec les lignes du tableau de donnée quand on l'ouvre sous Excell/libreOffice
        
        for dicDataCur in reader:
            t = []
            #on ajoute les column au datacur
            for k in listColumnNames:
                dicDataCur[k] = ''
                t.append(k)
            
            #on gere l'ouverture et l'ecriture de la permiere ligne du fichier de sortie
            #---------------------------
            if countLine== 2 :
                outFile = open(outputFile, 'w', encoding='utf-8', newline="\n") 
                writerOut = csv.DictWriter(outFile, delimiter=';', fieldnames = reader.fieldnames+t)
                writerOut.writeheader()

            writerOut.writerow(dicDataCur)
                
            countLine+=1
                
    outFile.close() 
    
    
    
def addColumnToFileWithoutValue_test():
    
    pathDataFile = "C:/Users/Public/Documents/MSHS/workflow/Guarnido/03-metadatas/"
    
    inputFile = pathDataFile+"Guarnido-revise.csv"
    outputFile = pathDataFile+"Guarnido-revise_nklreadybyprog.csv"
    
    listColumnNames = ['Linked in collection branch','Linked in collection leaf', 'Nkl dataType','Nkl dataFormat','Nkl hdl root collection','Nkl hdl root collection','Nkl hdl branch collection','Nkl hdl leaf collection','Nkl hdl leaf data','Nkl statut']
    
    addColumnToFileWithoutValue(inputFile, outputFile, listColumnNames)
    
def suppColumn(inputFile, outputFile, listColumnNames):
    """
    fonction permettant de supprimer plusieurs colonnes inutiles à un fichier CSV d'entree
    
    :param inputFile: le chemin vers le fichier de data d'entree
    :type inputFile: str. 
    
    :param outputFile: le chemin vers le fichier de data de sortie (contenant les nouvelles colonnes)
    :type inputFile: str. 
    
    :param listColumnNames: list de noms de colones
    :type: list['str']
    
    :raise : IO exception
    """
    writerOut = None
    outFile = None
    with open(inputFile, encoding='utf-8', newline="\n") as dataFile:
        reader = csv.DictReader(dataFile, delimiter=';')
        countLine = 2 #on commence a 2 pour etre cohérent avec les lignes du tableau de donnée quand on l'ouvre sous Excell/libreOffice
        
        
        
        
        for dicDataCur in reader:
            #on supp les column au datacur
            for k in listColumnNames:
                del dicDataCur[k]
            
            #on gere l'ouverture et l'ecriture de la permiere ligne du fichier de sortie
            #---------------------------
            if countLine== 2 :
                listColumFinal = []
                for i in reader.fieldnames:
                    if not(i in listColumnNames):
                        listColumFinal.append(i)
                
                outFile = open(outputFile, 'w', encoding='utf-8', newline="\n") 
                writerOut = csv.DictWriter(outFile, delimiter=';', fieldnames = listColumFinal)
                writerOut.writeheader()
                
            writerOut.writerow(dicDataCur)
                
            countLine+=1
                
    outFile.close() 
    
def suppColumn_test():  
    pathDataFile = "C:/Users/Public/Documents/MSHS/workflow/Guarnido/03-metadatas/"
    inputFile = pathDataFile+"Guarnido-revise.csv"
    outputFile = pathDataFile+"Guarnido-revise_nkldelCol.csv"
    
    listColumnNames = ['urlEditItem','urlShowItem']
    suppColumn(inputFile, outputFile, listColumnNames)
        

def replaceValueWithOther(inputFile, outputFile, columnNameTest, valueTest, valueReplace):
    """
    fonction permetant de remplacer une chaine de caractere par une autre dans une column specifié
    """
    writerOut = None
    outFile = None
    with open(inputFile, encoding='utf-8', newline="\n") as dataFile:
        reader = csv.DictReader(dataFile, delimiter=';')
        countLine = 2 #on commence a 2 pour etre cohérent avec les lignes du tableau de donnée quand on l'ouvre sous Excell/libreOffice
        for dicDataCur in reader:
            #on gere l'ouverture et l'ecriture de la permiere ligne du fichier de sortie
            #---------------------------
            if countLine== 2 :
                outFile = open(outputFile, 'w', encoding='utf-8', newline="\n") 
                writerOut = csv.DictWriter(outFile, delimiter=';', fieldnames = reader.fieldnames)
                writerOut.writeheader()
                
            valueCur = dicDataCur[columnNameTest]
            if not(valueCur==None):
                if valueTest in valueCur:
                    dicDataCur[columnNameTest] = dicDataCur[columnNameTest].replace(valueTest, valueReplace)
                         
            else:
                pass
            
            writerOut.writerow(dicDataCur)
            countLine+=1
            
    outFile.close()     
    

    
    
def addPrefixValueFixedFromDic(inputFile, outputFile, dicPrefix, multiValueSeparator = ' | '):
    """
    fonction permettant d'ajouter un prefix et ou suffix aux valeurs présentes (n'ajoute rien pour les values '' ou 'none' ou 'None' ou None dans la column spécifié
    
    :param inputFile: le chemin vers le fichier de data d'entree
    :type inputFile: str. 
    
    :param outputFile: le chemin vers le fichier de data de sortie (contenant les nouvelles colonnes)
    :type inputFile: str. 
    
    :param dicPrefix: le dictionnaire contenant le nom des column sur lesquelles ajouter un prefix
    :type dicPrefix: dict['columName']='prefix'
    
    :raise : IO exception, KeyError
    
    """
    
    writerOut = None
    outFile = None
    with open(inputFile, encoding='utf-8', newline="\n") as dataFile:
        reader = csv.DictReader(dataFile, delimiter=';')
        countLine = 2 #on commence a 2 pour etre cohérent avec les lignes du tableau de donnée quand on l'ouvre sous Excell/libreOffice
        for dicDataCur in reader:
            dicRes = dicDataCur.copy()
            #on gere l'ouverture et l'ecriture de la permiere ligne du fichier de sortie
            #---------------------------
            if countLine== 2 :
                outFile = open(outputFile, 'w', encoding='utf-8', newline="\n") 
                writerOut = csv.DictWriter(outFile, delimiter=';', fieldnames = reader.fieldnames)
                writerOut.writeheader()
                           
            for k in dicPrefix:
                listvalue = dicDataCur[k].split(multiValueSeparator)

                dicRes[k] = ''
                for value in listvalue:
                    if not(value==None):
                        if not(value.lower()=='none' or value==''):
                            if dicRes[k] == '':
                                dicRes[k] = ''+dicPrefix[k]+value
                            else:
                                dicRes[k] = dicRes[k] + multiValueSeparator + dicPrefix[k]+value
                                
                #print('dicRes[k]',dicRes[k])  
            #print('dicRes',dicRes) 
            writerOut.writerow(dicRes)
            countLine+=1
            
    outFile.close() 
    
    
def addSuffixValueFixedFromDic(inputFile, outputFile, dicSuffix, multiValueSeparator = ' | '):
    """
    fonction permettant d'ajouter un prefix et ou suffix aux valeurs présentes (n'ajoute rien pour les values '' ou 'none' ou 'None' ou None dans la column spécifié
    
    :param inputFile: le chemin vers le fichier de data d'entree
    :type inputFile: str. 
    
    :param outputFile: le chemin vers le fichier de data de sortie (contenant les nouvelles colonnes)
    :type inputFile: str. 
    
    :param dicPrefix: le dictionnaire contenant le nom des column sur lesquelles ajouter un prefix
    :type dicPrefix: dict['columName']='prefix'
    
    :raise : IO exception, KeyError
    
    """
    
    writerOut = None
    outFile = None
    with open(inputFile, encoding='utf-8', newline="\n") as dataFile:
        reader = csv.DictReader(dataFile, delimiter=';')
        countLine = 2 #on commence a 2 pour etre cohérent avec les lignes du tableau de donnée quand on l'ouvre sous Excell/libreOffice
        for dicDataCur in reader:
            dicRes = dicDataCur.copy()
            #on gere l'ouverture et l'ecriture de la permiere ligne du fichier de sortie
            #---------------------------
            if countLine== 2 :
                outFile = open(outputFile, 'w', encoding='utf-8', newline="\n") 
                writerOut = csv.DictWriter(outFile, delimiter=';', fieldnames = reader.fieldnames)
                writerOut.writeheader()
                           
            for k in dicSuffix:
                listvalue = dicDataCur[k].split(multiValueSeparator)

                dicRes[k] = ''
                for value in listvalue:
                    if not(value==None):
                        if not(value.lower()=='none' or value==''):
                            if dicRes[k] == '':
                                dicRes[k] = ''+value+dicSuffix[k]
                            else:
                                dicRes[k] = dicRes[k] + multiValueSeparator + value+dicSuffix[k]
                                
                #print('dicRes[k]',dicRes[k])  
            #print('dicRes',dicRes) 
            writerOut.writerow(dicRes)
            countLine+=1
            
    outFile.close() 

def addValueFixed(inputFile, outputFile, ColumnName, value):
    """
    fonction permettant d'ajouter une valeur fix dans la column spécifié
    
    :param inputFile: le chemin vers le fichier de data d'entree
    :type inputFile: str. 
    
    :param outputFile: le chemin vers le fichier de data de sortie (contenant les nouvelles colonnes)
    :type inputFile: str. 
    
    :param columnName: nom de la colonnes à traiter
    :type: str

    :param value: le prefix a ajouter
    :type: str


    :raise : IO exception, KeyError
    
    """
    
    writerOut = None
    outFile = None
    with open(inputFile, encoding='utf-8', newline="\n") as dataFile:
        reader = csv.DictReader(dataFile, delimiter=';')
        countLine = 2 #on commence a 2 pour etre cohérent avec les lignes du tableau de donnée quand on l'ouvre sous Excell/libreOffice
        for dicDataCur in reader:
            #on gere l'ouverture et l'ecriture de la permiere ligne du fichier de sortie
            #---------------------------
            if countLine== 2 :
                outFile = open(outputFile, 'w', encoding='utf-8', newline="\n") 
                writerOut = csv.DictWriter(outFile, delimiter=';', fieldnames = reader.fieldnames)
                writerOut.writeheader()
                
                
            dicDataCur[ColumnName] = value   
                      
            writerOut.writerow(dicDataCur)
            countLine+=1
            
    outFile.close()    
    
    
def addPrefixSuffixValueFixed(inputFile, outputFile, ColumnName, prefixValue, suffixValue):
    """
    fonction permettant d'ajouter un prefix et ou suffix aux valeurs présentes (n'ajoute rien pour les values '' ou 'none' ou 'None' ou None dans la column spécifié
    
    :param inputFile: le chemin vers le fichier de data d'entree
    :type inputFile: str. 
    
    :param outputFile: le chemin vers le fichier de data de sortie (contenant les nouvelles colonnes)
    :type inputFile: str. 
    
    :param columnName: nom de la colonnes à traiter
    :type: str

    :param prefixValue: le prefix a ajouter
    :type: str
    
    :param suffixValue: le suffix a ajouter
    :type: str
    
    

    :raise : IO exception, KeyError
    
    """
    
    writerOut = None
    outFile = None
    with open(inputFile, encoding='utf-8', newline="\n") as dataFile:
        reader = csv.DictReader(dataFile, delimiter=';')
        countLine = 2 #on commence a 2 pour etre cohérent avec les lignes du tableau de donnée quand on l'ouvre sous Excell/libreOffice
        for dicDataCur in reader:
            #on gere l'ouverture et l'ecriture de la permiere ligne du fichier de sortie
            #---------------------------
            if countLine== 2 :
                outFile = open(outputFile, 'w', encoding='utf-8', newline="\n") 
                writerOut = csv.DictWriter(outFile, delimiter=';', fieldnames = reader.fieldnames)
                writerOut.writeheader()
                

            value = dicDataCur[ColumnName]
            if not(value==None):
                if not(value.lower()=='none' or value==''):
                    dicDataCur[ColumnName] = prefixValue+value+suffixValue
            
            writerOut.writerow(dicDataCur)
            countLine+=1
            
    outFile.close() 
    
        
def addSuffixValueConditional(inputFile, outputFile, columnNameTest, columnNameOut, contentValue, suffixValueTrue, suffixValueFalse):
    """
    fonction permettant d'ajouter un suffix aux valeurs présentes dans columnNameOut
    en testant la presence d'une contentValue dans le column columnNameTest
    
    
    :param inputFile: le chemin vers le fichier de data d'entree
    :type inputFile: str. 
    
    :param outputFile: le chemin vers le fichier de data de sortie (contenant les nouvelles colonnes)
    :type inputFile: str. 
    
    :param columnNameOut: nom de la colonnes à remplir
    :type: str

    :param columnNameTest: nom de la column a tester
    :type: str
    
    :param contentValue: valeur a tester
    :type: str
    
    :param suffixValueTrue: le suffix a ajouter en cas de True
    :type: str
    
    :param suffixValueFalse: le suffix a ajouter en cas de False
    :type: str
    
    :raise : IO exception, KeyError
    """
    
    writerOut = None
    outFile = None
    with open(inputFile, encoding='utf-8', newline="\n") as dataFile:
        reader = csv.DictReader(dataFile, delimiter=';')
        countLine = 2 #on commence a 2 pour etre cohérent avec les lignes du tableau de donnée quand on l'ouvre sous Excell/libreOffice
        for dicDataCur in reader:
            #on gere l'ouverture et l'ecriture de la permiere ligne du fichier de sortie
            #---------------------------
            if countLine== 2 :
                outFile = open(outputFile, 'w', encoding='utf-8', newline="\n") 
                writerOut = csv.DictWriter(outFile, delimiter=';', fieldnames = reader.fieldnames)
                writerOut.writeheader()
                
            valueCur = dicDataCur[columnNameTest]
            if not(valueCur==None):
                if contentValue in valueCur:
                    dicDataCur[columnNameOut] = dicDataCur[columnNameOut] +suffixValueTrue
                else:
                    dicDataCur[columnNameOut] = dicDataCur[columnNameOut] +suffixValueFalse                             
            else:
                pass
            
            writerOut.writerow(dicDataCur)
            countLine+=1
            
    outFile.close() 
    
def addSuffixValueConditional_test():
    pathDataFile = "C:/Users/Public/Documents/MSHS/workflow/Guarnido/03-metadatas/"
    inputFile = pathDataFile+"Guarnido_splitByFile.csv"
    outputFile = pathDataFile+"Guarnido_conditionalSuffix.csv"
    
    columnNameTest = 'Type'
    columnNameOut = 'Nkl dataType'
    
    contentValue = 'Iconographie'
    suffixValueTrue = 'Image'
    suffixValueFalse = 'Text'
    
    
    addSuffixValueConditional(inputFile, outputFile, columnNameTest, columnNameOut, contentValue, suffixValueTrue, suffixValueFalse)
    
        
def addPrefixValueFixed_test():
    pathDataFile = "C:/Users/Public/Documents/MSHS/workflow/Guarnido/03-metadatas/"
    inputFile = pathDataFile+"Guarnido-revise.csv"
    outputFile = pathDataFile+"Guarnido-revise_nklreadybyprogPrefix.csv"
    
    ColumnName = 'Etat général'
    prefixValue = 'Etat: '
    suffixValue = ''
    
    addPrefixSuffixValueFixed(inputFile, outputFile, ColumnName, prefixValue, suffixValue)
    
    """
    ColumnName = 'Etat génétique'
    prefixValue = 'Genetique: '
    addPrefixValueFixed(inputFile, outputFile, ColumnName, prefixValue)
    
    ColumnName = 'Destinataire'
    prefixValue = 'Destinataire: '
    addPrefixValueFixed(inputFile, outputFile, ColumnName, prefixValue)
    """
    




def duplicateColumn(inputFile, outputFile, columnNameIn, columnNameDup, appendMode, separator):
    """
    fonction permettant de recopier le copier le contenu d'une colonne dans une colonne d'un autre nom
    (par exemple copier le contenu d'un dc:date dans un dc:created)
    
    :param inputFile: le chemin vers le fichier de data d'entree
    :type inputFile: str. 
    
    :param outputFile: le chemin vers le fichier de data de sortie (contenant les nouvelles colonnes)
    :type inputFile: str. 
    
    :param columnNameIn: nom de la colonnes à copier
    :type: str
    
    :param columnNameDup: nom de la  colonne à remplir (on aura generer prealablement la colonne via addColumnToFileWithoutValue)
    :type: str
    
    :param appendMode: pour suffixer plutot que ecraser completement
    :type : bool
    
    :param separator: utile seulement en cas de appendMode (pour ajouter une valeur constante)
    :type : str
    """
    
    writerOut = None
    outFile = None
    with open(inputFile, encoding='utf-8', newline="\n") as dataFile:
        reader = csv.DictReader(dataFile, delimiter=';')
        countLine = 2 #on commence a 2 pour etre cohérent avec les lignes du tableau de donnée quand on l'ouvre sous Excell/libreOffice
        
        for dicDataCur in reader:
            #on gere l'ouverture et l'ecriture de la permiere ligne du fichier de sortie
            #---------------------------
            if countLine== 2 :
                listNomCol = reader.fieldnames
                
                outFile = open(outputFile, 'w', encoding='utf-8', newline="\n") 
                writerOut = csv.DictWriter(outFile, delimiter=';', fieldnames = listNomCol)
                writerOut.writeheader()

            if appendMode:
                #on ajoute que si ya du contenu ...
                valueIn = dicDataCur[columnNameIn]
                if valueIn == None or valueIn == 'None' or valueIn =='': 
                    pass
                else:
                    dicDataCur[columnNameDup] = dicDataCur[columnNameDup] + separator+ dicDataCur[columnNameIn]
            else:
                dicDataCur[columnNameDup] = dicDataCur[columnNameIn]
                
            writerOut.writerow(dicDataCur)
                
            countLine+=1
    outFile.close() 
    
    
def duplicateColumnIfOtherColumnIsnotEmpty(inputFile, outputFile, columnNameIn, columnNameOut, appendMode, separator, columnNameTest):
    """
    fonction permettant de recopier le copier le contenu d'une colonne dans une colonne d'un autre nom
    (par exemple copier le contenu d'un dc:date dans un dc:created)
    
    :param inputFile: le chemin vers le fichier de data d'entree
    :type inputFile: str. 
    
    :param outputFile: le chemin vers le fichier de data de sortie (contenant les nouvelles colonnes)
    :type inputFile: str. 
    
    :param columnNameIn: nom de la colonnes à copier
    :type: str
    
    :param columnNameOut: nom de la  colonne à remplir (on aura generer prealablement la colonne via addColumnToFileWithoutValue)
    :type: str
    
    :param appendMode: pour suffixer plutot que ecraser completement
    :type : bool
    
    :param separator: utile seulement en cas de appendMode (pour ajouter une valeur constante)
    :type : str
    
    columnNameTest, contentValue
    
    """
    
    writerOut = None
    outFile = None
    with open(inputFile, encoding='utf-8', newline="\n") as dataFile:
        reader = csv.DictReader(dataFile, delimiter=';')
        countLine = 2 #on commence a 2 pour etre cohérent avec les lignes du tableau de donnée quand on l'ouvre sous Excell/libreOffice
        
        for dicDataCur in reader:
            #on gere l'ouverture et l'ecriture de la permiere ligne du fichier de sortie
            #---------------------------
            if countLine== 2 :
                listNomCol = reader.fieldnames
                
                outFile = open(outputFile, 'w', encoding='utf-8', newline="\n") 
                writerOut = csv.DictWriter(outFile, delimiter=';', fieldnames = listNomCol)
                writerOut.writeheader()



            #on commence par regarder si il y a du contenu dans la column àa potentiellement dupliquer
            valueIn = dicDataCur[columnNameIn]
            if valueIn == None or valueIn == 'None' or valueIn =='': 
                pass
            else:
                #on test la condition (que si il y a du contenu)
                if dicDataCur[columnNameTest] == None or dicDataCur[columnNameTest] =='':
                    pass
                else:
                        if appendMode:
                            #on ajoute que si ya du contenu ...
                            dicDataCur[columnNameOut] = dicDataCur[columnNameOut] + separator+ dicDataCur[columnNameIn]
                        else:
                            dicDataCur[columnNameOut] = dicDataCur[columnNameIn]


                
            writerOut.writerow(dicDataCur)
                
            countLine+=1
    outFile.close()     
    

def duplicateColumn_test():
    pathDataFile = "C:/Users/Public/Documents/MSHS/workflow/Guarnido/03-metadatas/"
    inputFile = pathDataFile+"Guarnido-revise.csv"
    outputFile = pathDataFile+"Guarnido-revise_nklreadybyprogDup.csv"
    
    columnNameIn = 'Date'
    columnNameDup = 'Created'
    
    duplicateColumn(inputFile, outputFile, columnNameIn, columnNameDup)
    
  
def makeListFilesInCsv(pathInputFiles, pathOutputCsv):
    """
    construire un csv contenant la liste des fichiers presents dans un dossier (1 ligne par fichier trouvé)
    
    :param pathInputFiles: le chemin vers le dossier dont on souaite extrire la liste des fichiers
    :type inputFile: str. 
    
    :param pathOutputCsv: le chemin vers le fichier Csv de sortie
    :type pathOutputCsv: str.
    
    """
    
    #on ouvre le fichier de sortie
    writerOut = None
    outFile = None

    countLine = 2 #on commence a 2 pour etre cohérent avec les lignes du tableau de donnée quand on l'ouvre sous Excell/libreOffice
    nameFileColumn = 'file'
    listNomCol = [nameFileColumn]
    
    for file in glob.glob(pathInputFiles+'*'):
        
        if countLine== 2 :
            
            outFile = open(pathOutputCsv, 'w', encoding='utf-8', newline="\n") 
            writerOut = csv.DictWriter(outFile, delimiter=';', fieldnames = listNomCol)
            writerOut.writeheader()

        dicDataCur = {}
        dicDataCur[nameFileColumn] = file
        writerOut.writerow(dicDataCur)
            
        countLine+=1
                
    outFile.close() 
    
    
def makeListFilesInCsv_test():
    
    pathInputFiles = 'C:/Users/Public/Documents/MSHS/CHispa/backup/dirOutWebImageGuarnido/'
    pathOutputCsv = 'C:/Users/Public/Documents/MSHS/CHispa/backup/listFileGuarnido.csv'
    makeListFilesInCsv(pathInputFiles, pathOutputCsv)   
    
    
def checkMappingFileValues(pathMappingFile,listRequireMappedValues):
    """
    pour réaliser un envoi avec succès il faut remplir plusieurs champs de métas données obligatoires
        les champs dublincore: title,creator,type,created
        les champs nakala: format, incollection

    Cette fonction permet de vérifier dans les valeurs du fichier de mapping leurs présences
    
        
    :param pathMappingFile : le chemin vers le fichier CSV contenant le mapping entre les champs personalisées et leurs correspondances 
    :type pathMappingFile : str.   
    
    :param listMappedValues :la liste des columns qui doivent avoir du contenu
    :type listMappedValues : list[str].   
    
    """
    
    try :
        with open(pathMappingFile, encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=';')
            countLine = 1 
        
            for row in reader:
                if countLine == 1 :
                    
                    for requireMappedValue in listRequireMappedValues:
                        if not(requireMappedValue in row.values()):
                            print('[error mappingFile] : la valeur obligatoire',requireMappedValue, "n'est pas présente")
                            csvfile.close()
                            return False
                
                    csvfile.close()
                    return True
                countLine += 1
                
                        
    except Exception as e:
            print(e)
            csvfile.close()
            return False  
        
def getDistinctValueInColumn(pathInputFiles, columnNameIn):
    """
    Permet d'obtenir la liste des elements distincts d'une column

    Cette fonction permet de vérifier dans les valeurs du fichier de mapping leurs présences
    
        
    :param pathInputFiles : le chemin vers le fichier CSV 
    :type pathInputFiles : str.   
    
    :param columnNameIn :le nom de la column a analyse
    :type columnNameIn : str.   
    
    :return: la liste des valeurs distincts
    :raise : IO exception, KeyError
    """
    
    listDistinctValues = []
    try :
        with open(pathInputFiles, encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=';')
       
            for row in reader:
                #print(row[columnNameIn])
                if not(row[columnNameIn] in listDistinctValues ):
                    listDistinctValues.append(row[columnNameIn])
                    
                
    except Exception as e:
            print(e)

    return listDistinctValues  
    
    
def getDistinctValueInColumn_test():
    datasPath = "C:/Users/Public/Documents/MSHS/workflow/Molina/03-metadatas/"
    datasFilename = "Molina-revise.csv"
    
    datasPath = datasPath+datasFilename
    
    columnName = 'Type'

    listRes = getDistinctValueInColumn(datasPath,columnName)
    print(listRes)
   
def main(argv):

    #test_saveWebImagesFromCSV()
    #test_orientedOmekaCsv2LineByFileCsv()
    #test_changeColumnFileWithLocalPath()
    #test_renameFileFromCSV()
    
    #test_remappForOmekaImportCSV()
    #checkMappingFileWithDataFile_test()
    
    #checkColumnExist_test()
    
    #addColumnToFileWithoutValue_test()
    #addPrefixValueFixed_test()
    #suppColumn_test()

    #duplicateColumn_test()
    
    #creatLineFromMultiValuesInColumn_test()
    #makeListFilesInCsv_test()
    
    #addSuffixValueConditional_test()
    getDistinctValueInColumn_test()
    pass

if __name__ == "__main__":
    main(sys.argv) 
    
