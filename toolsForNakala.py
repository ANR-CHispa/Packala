#!/usr/bin/env python
# -*- coding: utf-8 -*-
#python 3 

#author : Nauge Michael (Université de Poitiers)
#description : script contenant plusieurs fonctions permettant de manipuler des données dans nakala
#par exemple générer les zip pour l'envoi en lot par nakala console



############# les imports ###############

#pour creer des zip pour lenvoi par lot depuis lapp console nakala
import zipfile

#pour la lecture et ecriture de fichier csv
import csv
import sys

#pour faire des requetes get sur le server nakala
import urllib.request

#pour parser des input xml ou html
from bs4 import BeautifulSoup

#pour appeler l'app nakalaconsole
import subprocess 

#pour tester l'existance de fichier
import os
import glob

#pour supprimer les accents sur les noms de fichier car ça pose probleme a nakala
from unidecode import unidecode

#pour la gestion de la date pour le champs dcterms:created
from datetime import date
   
#pour la gestion de temps d'attente
import time

#pour l'encodage en xml
from xml.dom.minidom import Text, Element


import toolsForCSV as toolCSV
import toolForSparql as toolSparql
######################################

"""

Dans Nakala il existe seulement 2 types : les nkl:data et les nkl:collection
une nkl:data est consitué d'une ressource (fichier image, fichier vidéo, fichier texte, etc..) et de ses étiquettes descriptives (métadatas)

Dans ce contexte voici la structuration choisi pour un projet type :

- une collection racine du projet, appelée "toutes les données" sur nakala.  
    C'est cette collection qui va contenir toutes les autres collections.

- des collections feuilles. Elles contiennent en général une seule data nakala 
    de type image (ex: un fac similé d'une photographie, 
    un fac similé d'une page d'un ouvrage édité, 
    un fac similé du recto d'une lettre...). 
    Elles sont créées de manière proactives pour faciliter l'ajout potentiel 
    de nouvelles nakala datas. C'est ce type de collection qui sert à agréger 
    les différentes nakala data créées à partir du fac similé (ou de l'objet d'origine). 
    Par exemple, la transcription automatique (OCR) au format pdf., 
    la transcription manuelle au format txt, la traduction au format XML/TEI, 
    l'analyse spectrale des pigments au format CSV. 
    
    Toutes ces ressources décrivant le même objet réel se retrouvent liées 
    par cette collection feuille.

- des collections branches. Elles sont créées pour agréger des collections 
    feuilles physiquement ou conceptuellement liées. 
    Par exemple regrouper toutes les pages d'un ouvrage/carnet/journal ou 
    plusieurs photos d'un même lieu, mais avec différents points de vue...
    

"""


class objConfigNakalaConsole:
    """ class contenant les parametres pour lancer l'application nakala console.jar
    cela permet uniquemet de réduire artificiellement le nombre de paramtrse a passer aux fonctions utilisant 
    le programme nakala-console.jar
    """
    
    pathJava = ''
    pathNakalaConsole = ''
    nameConsoleJar = ''

    keyApi = ''
    handlePjt = ''
    email = ''

    def __init__(self, pathJava, pathNakalaConsole, nameConsoleJar, keyApi, handlePjt, email):
        self.pathJava = pathJava  
        self.pathNakalaConsole = pathNakalaConsole  
        self.nameConsoleJar = nameConsoleJar

        self.keyApi = keyApi      
        self.handlePjt = handlePjt 
        self.email = email  
       

        
        
      
     
def transformDataFileForNakalaPush(pathMetaFileInput, pathMetaFileOutput):
    """
    pour envoyer facilement ses données sur nakala il faut structurer le fichier de (metas)données de maniere un peu spécifique.
    l'idée est d'avoir une fonction de push assez standard et générique, et ses plutot en structurant de maniere particuliere 
    le fichier de données que nous levons les ambiguités et spécificitées
    il est preferable transformer par programme le fichier de metadonnees
    
    cela peut etre fait manuellement à l'aide d'outils de manpulations tels que excell ou libre office
    mais pour garantir des modifications constantes et répétables nous préférons que ces operations soit réalisées par programme
    
    Le but de cette fonction est donc de gérer les transformations du fichiers de données vers un fichier de données
    particulierement adapaté à nakala
    
    Les spécificités identifiées :
        
    Le fichier de données doit contenir les champs :
        Title : Le titre de la ressource (unique) (j'encourage l'ajout d'un suffix fac-similé, OCR, transcription, traduction, part ou complet)
            
        Nkl hdl root collection permettant de préciser dans quelle collection mere les données doivent êtres envoyées
            En général toutes les données d'un même projets ont vocation à etre déposées dans une collection de projet appelé "toute les données" sur nakala
            A titre d'exemple pour le fond d'archive du projet Guarnido, le handle de ce projet est : 11280/b28f4a83
            A titre d'exemple pour le fond d'archive du projet Fernando Ainsa, le handle de ce projet est : 11280/7c0135eb
            A titre d'exemple pour le fond d'archive du projet Carlos Liscano, le handle de ce projet est : 11280/b5756318
    
        Linked in collection branch : c'est la colonne qui est utilisé pour demander la création de nkl branch collection. Son contenu substitu la colonne title pour donner un titre a la collection branch
        Linked in collection leaf : c'est la colonne qui est utilisé pour demander la création de nkl leaf collection. Son contenu substitu la colonne title pour donner un titre a la collection leaf
        
        Nkl dataType : décrit le type de la ressource (Nakala Type:Actualités|Archive ouverte|Articles|Autres|Bibliographies|Bibliothèque numérique|Billets de blog|Blog|Bulletin|Calendrier|Collection|Collection de livres|Colloques et conférences|Conférences|Dataset|Données d'enquêtes|Event|Expositions|Image|InteractiveResource|Manuscrits|Matériels pédagogiques|MovingImage|Mémoires, Thèses et HDR|Ouvrages et chapitres d'ouvrage|Page Web|Photos et images|PhysicalObject|Présentations|Périodiques|Rapports|Revue|Service|Site Web|Software|Son et audiovisuel|Sound|StillImage|Séminaires|Text|Textes|
        Nkl dataFormat	: décrit le format du fichier de ressource (JPG, odt, PDF...) liste des formats de fichiers http://facile.cines.fr/?media=XML
        
        Nkl hdl root collection	: elle doit contenir le handle de la collection root.
        
        Nkl hdl branch collection : elle est amené a contenir le handle de la collection branch
        Nkl hdl leaf collection : elle est amené a contenir le handle de la collection leaf
        Nkl hdl leaf data : elle est amené a contenir le handle de la data
        
        Nkl statut : elle est amené a contenir des informations relatives au dépots su nakala (errors...)


    Une ligne par ressource. 
    
    Certaines données vont dans description mais risque d'etre vide de sens  si on ajoute pas un prefix ou suffix. 
    
    Certaines données doivent attérir dans des nouvelles instances de balise dublin core alors que d'autres doivent rester grouper et piper (|)
    
    """
    
    return True



def metadataFileCSV2NakalaPushV3(objConfigNakalaConsole, pathDataFile, pathMappingFile, pathDataFileOutput, pathNklCollecInputFolder, pathNklCollecOutputFolder, pathNklDataInputFolder, pathNklDataOutputFolder, listColumnRejectedForCollectionBranch, listColumnRejectedForCollectionleaf, listColumnRejectedForDataleaf, updateMetas = True, updateDatas = False):
    """
    fonction permettant de faciliter l'envoi massif sur nakala
    cette fonction gere l'envoi et la mise a jour des donnnées
    elle peut etre interrompu a tout instant et etre relancé sans que cela necessite une quelquonque intervention
    
    elle utilise un fichier dataFile contenant les données descriptives des fichiers ressources à verser sur nakala
    elle utilise un fichier mappingFile contenant les correpondances entre les étiquettes personnalisées du chercheur présente dans le fichier dataFile et des étiquettes dublincore utilisées par nakala
    
    le fichier dataFile doit contenir des colonnes specifiques pour nakala cf : transformDataFileForeNakalaPush
    certaines colonnes doivent obligatoirement avoir du contenu
      il faut détecter si le fichier de data ne contient pas de valeurs pour certaines colonnes particuliere.
        les datas sont obligatoires pour [Nkl dataType], [Nkl dataFormat], [Nkl hdl root collection] obligatoire pour nakala
        
        

    
    pour chaque ligne de donnée
       
        on regarde si il faut créer une collection branche en regardant le contenu de la colonne [Linked in collection branch]
          si il y a None ou Blank il n'y a pas de collection branche a gérer
          si il y a un contenu on vérifie si cette collection existe sur nakala a l'aide d'une requete sparql basé sur le nom present dans la [Linked in collection branch] qui doit aller dans dcterms:title 

            si cette collection exist on a récupéré son handle et on peut l'écrire dans la colonne [Nkl hdl branch collection] (il est surement déjà écrit, mais dans le doute...)
            
            si elle n'existe pas on créer un .ZIP pour un envoi par console
              on lance l'envoi par console 
                il s'interomp on écrit dans le fichier de sortie l'erreur dans la colonne [Nkl statut]
                sinon a priori l'envoi c'est bien passé on peut relancer une requete pour recupérer le handle  et l'écrire dans la colonne [Nkl hdl leaf collection]
    
    
        si il faut des collection Leaf (faire presque la meme chose que pour la collection branch ;-)
        (D'après moi il faut toujours une collection leaf pour faciliter l'ajout de datas liées,mais ce n'est que mon point de vue)
        si il y a un contenu on vérifie si cette collection existe sur nakala a l'aide d'une requete sparql basé sur le nom present dans la [Linked in collection leaf] qui doit aller dans dcterms:title 
            si cette collection existe on a récupéré son handle et on peut l'écrire dans la colonne [Nkl hdl leaf collection] (il est surement déjà écrit, mais dans le doute...)
            
            si elle n'existe pas on créer un .ZIP pour un envoi par console
              on lance l'envoi par console 
                il s'interomp on écrit dans le fichier de sortie l'erreur dans la colonne [Nkl statut]
                sinon a priori l'envoi c'est bien passé on peut relancer une requete pour recupérer le handle  et l'écrire dans la colonne [Nkl hdl leaf collection]
    
    
    
    
    lorsque les datas sont sur Nakala (on peut obtenir leur Handle) on peut donc effecteur une mise ajour des métasdatas si c'est demandé via l'attribut  : updateMetas
    """
    #####################################################################
    #### les verifications de validité des fichiers de data et mapping
    #####################################################################
    
    #on commence par vérifier la présence des colonnes obligatoires dans le fichier de data
    listColonneACheck = ['Title','Linked in collection branch','Linked in collection leaf', 'Nkl dataType','Nkl dataFormat','Nkl hdl root collection','Nkl hdl root collection','Nkl hdl branch collection','Nkl hdl leaf collection','Nkl hdl leaf data','Nkl statut']
    resCheckColumn = toolCSV.checkColumnExist(listColonneACheck, pathDataFile)
    
    if resCheckColumn:
        #print('le datafile contient les colonnes obligatoires')
        pass
    else:
        print('[error] : le dataFile ne contient pas toutes les colonnes obligatoires')
        print('[help] : pensez à utiliser la fonction transformDataFileForeNakalaPush()')
        return False
    

    #on verifie ensuite la validite du fichier de mapping avec le fichier de data
    resCheckMappingWithDataFile = toolCSV.checkMappingFileWithDataFile(pathMappingFile,pathDataFile) 
    if resCheckMappingWithDataFile:
        #print('le fichier de mapping est compatible avec le fichier de data')
        pass
    else:
        print("[error] : le fichier de mapping n'est pas compatible avec le fichier de data")
        return False
    
    #on vérifie la présence de valeurs dans le fichier de mapping 
    #car pour envoyer sur nakala il faut fournir certaines métas obligatoires
    listRequireMappedValues = ['dcterms:title','dcterms:creator','dcterms:type','dcterms:created']
    resCheckMappingValues = toolCSV.checkMappingFileValues(pathMappingFile,listRequireMappedValues)
    if resCheckMappingValues:
        #print('le fichier de mapping contient bien les valeurs obligatoires pour nakala')
        pass
    else:
        print("[error] : le fichier de mapping ne contient pas les valeurs obligatoires pour nakala")
        return False
    
    #on verifie que le fichier de données contient bien du contenu pour toutes les datas dans les colonnes obligatoires
    listColumnRequired = ['Nkl dataType','Nkl dataFormat','Nkl hdl root collection']
    for columnRequired in listColumnRequired:
        if not(toolCSV.checkRequireValueInDataFile(pathDataFile,columnRequired)):
            print('il faut du contenu pour toutes les datas dans cette colonne:',columnRequired)
            return False
    #####################################################################
    
    #a priori nos fichiers données et mapping sont peuvent etre utilisés proprement
    #--------------------------------------------
    #on récupère le dictionnaire de mapping
    dicMapping = toolCSV.getMappingDicFromFile(pathMappingFile)
    #print('dicMapping',dicMapping)
    #--------------------------------------------
    
    #on ouvre le fichier de sortie permettant de sauvegarder les handles attribué et les eventuelles erreurs d'envoi
    outFile = None
    writerOut  = None

    
    #on traite les lignes du fichier de données
    with open(pathDataFile,encoding='utf-8') as dataFile:
        dataFile.seek(0)
        reader = csv.DictReader(dataFile, delimiter=';')
        #on obtient un dico par ligne
        countLine = 2 #on commence a 2 pour etre cohérent avec les lignes du tableau de donnée quand on l'ouvre sous Excell/libreOffice
        
        for dicDataCur in reader:
            #une variable qui jous permet de breaker le traitement d'une ligne posant probleme
            errorFatalForThisLine = False
            
            #on gere l'ouverture et l ecriture de la permiere ligne du fichier de sortie
            #---------------------------
            if countLine== 2 :
                outFile = open(pathDataFileOutput,'w', encoding='utf-8', newline="\n") 
                writerOut = csv.DictWriter(outFile, delimiter=';', fieldnames = reader.fieldnames)
                writerOut.writeheader()
            #---------------------------


            ##################################################
            ##### gestion des collection branches        #####
            ##################################################
            hdlCollBranch = None
            listColumnRejectedForCollectionBranchCurrent = listColumnRejectedForCollectionBranch.copy()
            #on regarde si il faut créer une collection branche en regardant le contenu de la colonne [Linked in collection]
            linkedInCollection = dicDataCur['Linked in collection branch']
            
            #si il y a None ou Blank il n'y a pas de collection branche a gérer
            if linkedInCollection=='' or linkedInCollection == None:
                print("L",countLine,': pas de collection branch a gerer')
            else :
                #si il y a un contenu 
                #on devra a terme avoir un handle de cette collection branch        
                #on vérifie si cette collection existe sur nakala à l'aide d'une requete sparql
                hdlCollRoot = dicDataCur['Nkl hdl root collection']

                hdlCollBranch = None
                try :
                    hdlCollBranch = toolSparql.nakalaCollectionExistByTitle(hdlCollRoot,linkedInCollection)
                except Exception as e:
                    print(e)
                    errorFatalForThisLine = True 
                    dicDataCur['Nkl statut'] = 'error nakalaCollectionExistByTitle Branch'+str(e)
                    
                if not(errorFatalForThisLine):
                    if not(hdlCollBranch==None):
                        print("L",countLine,'cette collection branch '+linkedInCollection+ " existe. Hdl trouvé:"+ hdlCollBranch)
                        #on a le handle de la collection branch !
                        #on le stock dans le dico de data
                        dicDataCur['Nkl hdl branch collection'] = hdlCollBranch
                        
                        #puisque ca existe et qu'on a demandé un update
                        if updateMetas:
                            #TODO
                            #update d'une collection c'est finalement comme un push pour la premiere fois
                            #on doit simplement rajouter la meta nkl:identifier avec comme valeur le handle de la collection a modifier
                            #print('on a demander un update')
                            pass
                            
                    #si elle n'existe pas on créer un .ZIP pour un envoi par console             
                    else:
                        #il faut creer cette collection branch via un push nakala 
                        print("L",countLine,"faut faire un push de collection branch nakala")
                        #on fait sauter la colonne title pour la colletio branch, 
                        #car c'est la colonne 'Linked in collection branch' qui va renseigner de title
    
                        listColumnRejectedForCollectionBranchCurrent.append('Title')
                        
                        #----------------------------------
                        #faudra remplacer ce code par celui de David
    
                        (dicMeta,dicNakalaMeta) = makeDicosForNklXmlMaker(dicMapping,dicDataCur,listColumnRejectedForCollectionBranchCurrent)
                        #print('dicMeta ',dicMeta) 
                        #print('dicNakalaMeta',dicNakalaMeta)
                        
                        nameXmlFileCur = 'tmpCollection.xml'
                        pathInputNameXml = pathNklCollecInputFolder+nameXmlFileCur
                        try :
                            writeNakalaCollectionXml(pathInputNameXml, dicMeta, dicNakalaMeta)
                            
                            #on a ecrit un fichier XML
                            #on va le mettre dans un fichier .ZIP 
                            nameZipFileCur = 'tmpCollection'+'.zip'
                            nameZipFileCur = unidecode(nameZipFileCur) 
                            pathInputNameZip = pathNklCollecInputFolder+nameZipFileCur
                            if not(os.path.isfile(pathInputNameZip)):
                                with zipfile.ZipFile(pathInputNameZip, 'w') as myzip:
                                    myzip.write(pathInputNameXml,arcname=nameXmlFileCur)
                            
                            #------------------------------------
                            
                            #on lance l'envoi par console 
                            #la key api doit etre dans unfichier .TXT pour l'app. nakala-console
                            #donc on ecrit la Key dans un fihcier :)
                            writeNakalaApiKeyTextFile(objConfigNakalaConsole.pathNakalaConsole,"myNklKey.txt",objConfigNakalaConsole.keyApi)
                            
                            pathNklKeyApi = objConfigNakalaConsole.pathNakalaConsole+"myNklKey.txt"
                            
                            #on lance l'envoi par console 
                            #il s'interomp on écrit dans le fichier de sortie l'erreur dans la colonne [Nkl statut]
                            #sinon a priori l'envoi c'est bien passé on peut relancer une requete pour recupérer le handle  et l'écrire dans la colonne [Nkl hdl branch collection]
                            resPushBranch = startNakalaConsoleV2(objConfigNakalaConsole.pathJava, objConfigNakalaConsole.pathNakalaConsole, objConfigNakalaConsole.nameConsoleJar, pathNklCollecInputFolder, pathNklCollecOutputFolder, objConfigNakalaConsole.email, objConfigNakalaConsole.handlePjt, pathNklKeyApi)
                            if resPushBranch:
                                hdlCollBranch  = toolSparql.nakalaCollectionExistByTitle(hdlCollRoot,linkedInCollection)
                                
                                if not(hdlCollBranch == None):
                                    
                                    print("L",countLine)
                                    print('cette collection branch exite maintenant '+linkedInCollection)
                                    print(" existe. Hdl trouvé:"+ hdlCollBranch)
                                    #on a le handle de la collection branch !
                                    #on le stock dans le dico de data
                                    dicDataCur['Nkl hdl branch collection'] = hdlCollBranch
                                else:
                                    print('le push de la branch a du échouer')
                                    errorFatalForThisLine = True
                                    
                                    dicDataCur['Nkl statut'] = 'error push branch'
                            else:
                                print("[error collBranch] :L",countLine,'cette collection branch '+linkedInCollection+ " n'a pas été correctement créee sur nakala")
                                print("on break de cette ligne car ça va poser soucis pour la collection leaf et la data")
                                errorFatalForThisLine = True
                                
                                dicDataCur['Nkl statut'] = 'error push branch'
                                
                        except Exception as e:
                            #on peut avoir une levé d'exception par la generation de xml nakala 
                            #ou la creation du .ZIP
                            print(e)
                            errorFatalForThisLine = True
                                       
            ##################################################
            ##### fin de gestion des collection branches #####
            ##################################################
            
            
            ##################################################
            ##### gestion des collection leaf            #####
            ##################################################
            if not(errorFatalForThisLine):
                listColumnRejectedForCollectionleafCurrent = listColumnRejectedForCollectionleaf.copy()
                #on regarde si il faut créer une collection leaf en regardant le contenu de la colonne [Linked in collection leaf]
                linkedInCollectionLeaf = dicDataCur['Linked in collection leaf']
                #si il y a None ou Blank il n'y a pas de collection branche a gérer
                if linkedInCollectionLeaf=='' or linkedInCollectionLeaf==None:
                    print("L",countLine,': pas de collection leaf a gerer')
                else :
                    #si il y a un contenu on vérifie si cette collection existe sur nakala à l'aide d'une requete sparql
                    hdlCollRoot = dicDataCur['Nkl hdl root collection']
                    hdlCollLeaf = None
                    
                    try:
                        hdlCollLeaf  = toolSparql.nakalaCollectionExistByTitle(hdlCollRoot,linkedInCollectionLeaf)
                    except Exception as e:
                        print(e)
                        errorFatalForThisLine = True 
                        dicDataCur['Nkl statut'] = 'error nakalaCollectionExistByTitle leaf'+str(e)
                    
                    if not(errorFatalForThisLine):
                        #si elle n'existe pas on créer un .ZIP pour un envoi par console     
                        
                        #une collection leaf est fille de root ou fille de branch
                        #le cas fille de root n'entraine pas de modification car nkl:incollection a deja pour valeur handle collection root
                        #le cas fille de branche entraine un remplacement de nkl:incollection par la valeur de handle collection branch
                        
                        linkedInCollectionBranch = dicDataCur['Linked in collection branch']
                        
                        #si il y a None ou Blank il n'y a pas de collection branche a gérer
                        if linkedInCollectionBranch=='' or linkedInCollectionBranch==None:
                            print("L",countLine,': pas de collection branch a gerer pour la collection leaf')
                        
                        else :
                            #on recupere le handle de la collection branche
                            #normalement on a stocké ce handle précedement dans la gestion de la collection branch
                            if 'Nkl hdl branch collection' in dicDataCur:
                                print('Nkl hdl branch collection for collection leaf',dicDataCur['Nkl hdl branch collection'])
                                #il faut faire en sorte que la collection leaf soit fille de la branch et non fille de root
                                
                                listColumnRejectedForCollectionleafCurrent.append('Nkl hdl root collection')
                                
                            else:
                                errorFatalForThisLine = True
    
                        
                        if not(hdlCollLeaf==None):
                            #print("L",countLine,'cette collection leaf '+linkedInCollection+ " existe. Hdl trouvé:"+ hdlCollLeaf)
                            #on a le handle de la collection leaf !
                            #on le stock dans le dico de data
                            dicDataCur['Nkl hdl leaf collection'] = hdlCollLeaf
                            #puisque ca existe et qu'on a demandé un update
                            if updateMetas:
                                #TODO
                                #update d'une collection c'est finalement comme un push pour la premiere fois
                                #on doit simplement rajouter la meta nkl:identifier avec comme valeur le handle de la collection a modifier
                                #print('on a demander un update')
                                pass
                                      
                        else:
                            #il faut creer cette collection leaf via un push nakala 
                            print("L",countLine,"faut faire un push de collection leaf nakala")
                            #on rejete la colonne 'title' 
                            #car c'est la colonne 'Linked in collection leaf' qui va renseigner le title
                            listColumnRejectedForCollectionleafCurrent.append('Title')
                            
                            #----------------------------------
                            #faudra remplacer ce code par celui de David
    
                            (dicMeta,dicNakalaMeta) = makeDicosForNklXmlMaker(dicMapping,dicDataCur,listColumnRejectedForCollectionleafCurrent)
                            #print('dicMeta ',dicMeta) 
                            #print('dicNakalaMeta',dicNakalaMeta)
                            
                            nameXmlFileCur = 'tmpCollectionLeaf.xml'
                            pathInputNameXml = pathNklCollecInputFolder+nameXmlFileCur
                            writeNakalaCollectionXml(pathInputNameXml, dicMeta, dicNakalaMeta)
                            
                            #on a ecrit un fichier XML
                            #on va le mettre dans un fichier .ZIP 
                            nameZipFileCur = 'tmpCollectionLeaf'+'.zip'
                            nameZipFileCur = unidecode(nameZipFileCur) 
                            pathInputNameZip = pathNklCollecInputFolder+nameZipFileCur
                            if not(os.path.isfile(pathInputNameZip)):
                                with zipfile.ZipFile(pathInputNameZip, 'w') as myzip:
                                    myzip.write(pathInputNameXml,arcname=nameXmlFileCur)
                            
                            #------------------------------------
                            
                            #on lance l'envoi par console 
                            #la key api doit etre dans unfichier .TXT pour l'app. nakala-console
                            #donc on ecrit la Key dans un fihcier :)
                            writeNakalaApiKeyTextFile(objConfigNakalaConsole.pathNakalaConsole,"myNklKey.txt",objConfigNakalaConsole.keyApi)
                            pathNklKeyApi = objConfigNakalaConsole.pathNakalaConsole+"myNklKey.txt"
                            
                            #on lance l'envoi par console 
                            #il s'interomp on écrit dans le fichier de sortie l'erreur dans la colonne [Nkl statut]
                            #sinon a priori l'envoi c'est bien passé on peut relancer une requete pour recupérer le handle  et l'écrire dans la colonne [Nkl hdl branch collection]
                            resPushBranch = startNakalaConsoleV2(objConfigNakalaConsole.pathJava, objConfigNakalaConsole.pathNakalaConsole, objConfigNakalaConsole.nameConsoleJar, pathNklCollecInputFolder, pathNklCollecOutputFolder, objConfigNakalaConsole.email, objConfigNakalaConsole.handlePjt, pathNklKeyApi)
                            if resPushBranch:
                                hdlCollLeaf = None
                                try:
                                    hdlCollLeaf  = toolSparql.nakalaCollectionExistByTitle(hdlCollRoot,dicDataCur['Linked in collection leaf'])
                                except Exception as e:
                                    #on peut avoir une levé d'exception par la generation de xml nakala 
                                    #ou la creation du .ZIP
                                    print(e)
                                    errorFatalForThisLine = True   
                                    dicDataCur['Nkl statut'] = 'error update leaf data'+ str(e)
                                    
                                if not(hdlCollLeaf == None) and not(errorFatalForThisLine==True):
                                    
                                    #print()
                                    print('L',countLine,'cette collection leaf exite maintenant '+dicDataCur['Linked in collection leaf'])
                                    #print(" existe. Hdl trouvé:"+ hdlCollLeaf)
                                    #on a le handle de la collection leaf !
                                    #on le stock dans le dico de data
                                    dicDataCur['Nkl hdl leaf collection'] = hdlCollLeaf
                                else:
                                    errorFatalForThisLine = True
                                    print('le push de la collection leaf a du échouer')
                                    dicDataCur['Nkl statut'] = 'error push leaf collection'
                                
                            else:
                                errorFatalForThisLine = True
                                print("[error collLeaf] :L",countLine,'cette collection leaf '+linkedInCollection+ " n'a pas été correctement créee sur nakala")
                                
                                dicDataCur['Nkl statut'] = 'error push leaf collection'
                        

                                  
            ##################################################
            ##### fin de gestion des collection leaf     #####
            ##################################################
            
            ##################################################
            ##### gestion des data leaf                  #####
            ##################################################
            if not(errorFatalForThisLine):
                listColumnRejectedForDataleafCurrent = listColumnRejectedForDataleaf.copy()

                #on regarde si il faut créer une data leaf en regardant le contenu de la colonne [file]
                dataLeafPathFileName = dicDataCur['file']
                #si il y a None ou Blank ou que le file not found il n'y a pas de datafile a gérer
                if dataLeafPathFileName=='' or dataLeafPathFileName==None or dataLeafPathFileName.lower()=='none':
                    #pass
                    print("L",countLine,': pas de data leaf a gerer')
                else :
                    #si il y a un contenu on vérifie si cette collection existe sur nakala à l'aide d'une requete sparql
                    hdlCollRoot = dicDataCur['Nkl hdl root collection']
                    #on se dit que le title est dans la colonne title
                    #dans le cas d'un document folioter on se dit que le script de modification
                    #des datas aura completer le titre initial avec un suffix page N par exemple
                    dataLeafTitle = dicDataCur['Title']
                    
                    try :
                        hdlDataLeaf  = toolSparql.nakalaDataExistByTitle(hdlCollRoot,dataLeafTitle)
                    except Exception as e:
                        print(e)
                        errorFatalForThisLine = True 
                        dicDataCur['Nkl statut'] = 'error nakalaDataExistByTitle '+str(e)
                        
                    if not(errorFatalForThisLine):
                        #si elle n'existe pas on créer un .ZIP pour un envoi par console     
                        
                        #une data leaf est fille de coll root ou fille de coll branch ou fille de coll leaf
                        #le cas fille de collection root n'entraine pas de modification car nkl:incollection a deja pour valeur handle collection root (on est fille de root qua si fille de branch et fille de leaf est vide)
                        #le cas fille de collection branche entraine une déconnexion de fille de root pour laisser effective la valeur de handle collection branch
                        #le cas fille de collection leaf entraine une déconnexion de fille de root, et branch pour laisser effective de handle collection leaf
                             
                        #on doit identifier dans quel cas on se trouve
                        #-------------------------------------------
                        isDaughterCollLeaf = False
                        isDaughterCollBranch = False
                        isDaughterCollRoot = False
                        #on commence par le plus profond
                        linkedInCollectionLeaf = dicDataCur['Linked in collection leaf']
                        #si il y a None ou Blank il n'y a pas de collection leaf a gérer
                        if linkedInCollectionLeaf=='' or linkedInCollectionLeaf==None:
                            #print("L",countLine,': pas de collection leaf sur laquelle se raccrocher')
                            pass
                        else :
                            isDaughterCollLeaf = True
                            
                        #si tu n'es pas fille de coll leaf es tu fille de coll branch ?
                        if not(isDaughterCollLeaf):
                            linkedInCollectionBranch = dicDataCur['Linked in collection branch']
                            #si il y a None ou Blank il n'y a pas de collection leaf a gérer
                            if linkedInCollectionBranch=='' or linkedInCollectionBranch==None:
                                #print("L",countLine,': pas de collection branch sur laquelle se raccrocher')
                                #tu es donc fille de root
                                isDaughterCollRoot = True
                            else :
                                isDaughterCollBranch = True
                        #------------------------------------------------
                        
                        #le cas fille de collection root n'entraine pas de modification car nkl:incollection a deja pour valeur handle collection root
                        if isDaughterCollRoot:
                            pass
                        
                        #le cas fille de collection branche entraine une déconnexion de fille de root pour laisser effective la valeur de handle collection branch
                        if isDaughterCollBranch:
                            listColumnRejectedForDataleafCurrent.append('Nkl hdl root collection')
                        
                        #le cas fille de collection leaf entraine une déconnexion de fille de root, et branch pour laisser effective de handle collection leaf
                        if isDaughterCollLeaf:
                            listColumnRejectedForDataleafCurrent.append('Nkl hdl root collection')
                            listColumnRejectedForDataleafCurrent.append('Nkl hdl branch collection')
                        
                        #dans tous les cas on a pas a utiliser les colones dédiés aux noms de collection.
                        listColumnRejectedForDataleafCurrent.append('Linked in collection branch')
                        listColumnRejectedForDataleafCurrent.append('Linked in collection leaf')
                            
                        
                        if not(hdlDataLeaf==None):
                            print("L",countLine,'cette data leaf '+dicDataCur['Title']+ " existe. Hdl trouvé:"+ hdlDataLeaf)
                            #on a le handle de la data leaf !
                            #on le stock dans le dico de data
                            dicDataCur['Nkl hdl leaf data'] = hdlDataLeaf
                            
                            #si la dataLeaf existe et qu'on a fait une demande d'update
                            if updateMetas or updateDatas:
                                print(">>>update de nkl:data", hdlDataLeaf)
                                print("L",countLine,"faut faire un update de data leaf nakala")
                                
                                #on ajoute dynamiquement un champs dans data nommee hdlid mappé sur nkl:identifier
                                dicDataCurTmp = dicDataCur.copy()
                                dicMappingTMp = dicMapping.copy()
                                dicDataCurTmp['hdlid'] = hdlDataLeaf
                                dicMappingTMp['hdlid'] = 'nkl:identifier'
                                
                                #----------------------------------
        
                                (dicMeta,dicNakalaMeta) = makeDicosForNklXmlMaker(dicMappingTMp,dicDataCurTmp,listColumnRejectedForDataleafCurrent)
                               
                                nameXmlFileCur = 'tmpDataLeafUpdate.xml'
                                pathInputNameXml = pathNklDataInputFolder+nameXmlFileCur
                                try:
                                    writeNakalaMetadataXml(pathInputNameXml, dicMeta, dicNakalaMeta)
                                    
                                    #on a ecrit un fichier XML
                                    #on va le mettre dans un fichier .ZIP 
                                    nameZipFileCur = 'tmpDataLeafUpdate'+'.zip'
                                    nameZipFileCur = unidecode(nameZipFileCur) 
                                    pathInputNameZip = pathNklDataInputFolder+nameZipFileCur
                                    if not(os.path.isfile(pathInputNameZip)):
                                        with zipfile.ZipFile(pathInputNameZip, 'w') as myzip:
                                            myzip.write(pathInputNameXml,arcname=nameXmlFileCur)
                                            
                                            #on ajoute aussi  la data dans le zip si besoin
                                            if updateDatas:
                                                cheminVersLaData = dataLeafPathFileName
                                                head, tail = os.path.split(dataLeafPathFileName) 
                                                nameData = tail
                                                myzip.write(cheminVersLaData, arcname=nameData)
            
                                    #------------------------------------
                                    
                                    #on lance l'envoi par console avec l'option -replace
                                    #la key api doit etre dans unfichier .TXT pour l'app. nakala-console
                                    #donc on ecrit la Key dans un fihcier :)
                                    writeNakalaApiKeyTextFile(objConfigNakalaConsole.pathNakalaConsole,"myNklKey.txt",objConfigNakalaConsole.keyApi)
                                    pathNklKeyApi = objConfigNakalaConsole.pathNakalaConsole+"myNklKey.txt"
                                    
                                    #on lance l'envoi par console 
                                    #il s'interomp on écrit dans le fichier de sortie l'erreur dans la colonne [Nkl statut]
                                    #sinon a priori l'envoi c'est bien passé on peut relancer une requete pour recupérer le handle  et l'écrire dans la colonne [Nkl hdl leaf data]
                                    resPushData = startNakalaConsoleV2(objConfigNakalaConsole.pathJava, objConfigNakalaConsole.pathNakalaConsole, objConfigNakalaConsole.nameConsoleJar, pathNklDataInputFolder, pathNklDataOutputFolder, objConfigNakalaConsole.email, objConfigNakalaConsole.handlePjt, pathNklKeyApi, update=True)
                                    if resPushData:
                                        hdlDataLeaf  = toolSparql.nakalaDataExistByTitle(hdlCollRoot,dicDataCur['Title'])
                                        
                                        if not(hdlDataLeaf == None):
                                            
                                            print("L",countLine)
                                            print('cette data leaf est MAJ '+dicDataCur['Title'])
                                            #on a le handle de la collection leaf !
                                            #on le stock dans le dico de data
                                            dicDataCur['Nkl hdl leaf data'] = hdlDataLeaf
                                        else:
                                            errorFatalForThisLine = True
                                            print('le push de la data leaf a du échouer')
                                        
                                    else:
                                        errorFatalForThisLine = True
                                        print("[error dataLeaf] :L",countLine,'cette data leaf '+dicDataCur['Title']+ " n'a pas été correctement MAJ sur nakala")
                                        dicDataCur['Nkl statut'] = 'error update leaf data'
                                        
                                except Exception as e:
                                    #on peut avoir une levé d'exception par la generation de xml nakala 
                                    #ou la creation du .ZIP
                                    print(e)
                                    errorFatalForThisLine = True   
                                    dicDataCur['Nkl statut'] = 'error update leaf data'+ str(e)
                                
                            
                        else:
                            #si la dataLeaf n'existe pas on créer un .ZIP pour un envoi par console  
                            #il faut creer cette data leaf via un push nakala 
                            print("L",countLine,"faut faire un push de data leaf nakala")
                           
                            #----------------------------------
                            #faudra remplacer ce code par celui de David
    
                            (dicMeta,dicNakalaMeta) = makeDicosForNklXmlMaker(dicMapping,dicDataCur,listColumnRejectedForDataleafCurrent)
                            #print('dicMeta ',dicMeta) 
                            #print('dicNakalaMeta',dicNakalaMeta)
                            
                            nameXmlFileCur = 'tmpDataLeaf.xml'
                            pathInputNameXml = pathNklDataInputFolder+nameXmlFileCur
                            try:
                                writeNakalaMetadataXml(pathInputNameXml, dicMeta, dicNakalaMeta)
                                
                                #on a ecrit un fichier XML
                                #on va le mettre dans un fichier .ZIP 
                                nameZipFileCur = 'tmpDataLeaf'+'.zip'
                                nameZipFileCur = unidecode(nameZipFileCur) 
                                pathInputNameZip = pathNklDataInputFolder+nameZipFileCur
                                if not(os.path.isfile(pathInputNameZip)):
                                    with zipfile.ZipFile(pathInputNameZip, 'w') as myzip:
                                        myzip.write(pathInputNameXml,arcname=nameXmlFileCur)
                                        
                                        #on ajoute aussi et surtout la data dans le zip 
                                        cheminVersLaData = dataLeafPathFileName 
                                        head, tail = os.path.split(dataLeafPathFileName) 
                                        nameData = tail
                                        myzip.write(cheminVersLaData, arcname=nameData)
        
                                #------------------------------------
                                
                                #on lance l'envoi par console 
                                #la key api doit etre dans unfichier .TXT pour l'app. nakala-console
                                #donc on ecrit la Key dans un fihcier :)
                                writeNakalaApiKeyTextFile(objConfigNakalaConsole.pathNakalaConsole,"myNklKey.txt",objConfigNakalaConsole.keyApi)
                                pathNklKeyApi = objConfigNakalaConsole.pathNakalaConsole+"myNklKey.txt"
                                
                                #on lance l'envoi par console 
                                #il s'interomp on écrit dans le fichier de sortie l'erreur dans la colonne [Nkl statut]
                                #sinon a priori l'envoi c'est bien passé on peut relancer une requete pour recupérer le handle  et l'écrire dans la colonne [Nkl hdl leaf data]
                                resPushData = startNakalaConsoleV2(objConfigNakalaConsole.pathJava, objConfigNakalaConsole.pathNakalaConsole, objConfigNakalaConsole.nameConsoleJar, pathNklDataInputFolder, pathNklDataOutputFolder, objConfigNakalaConsole.email, objConfigNakalaConsole.handlePjt, pathNklKeyApi)
                                if resPushData:
                                    hdlDataLeaf  = toolSparql.nakalaDataExistByTitle(hdlCollRoot,dicDataCur['Title'])
                                    
                                    if not(hdlDataLeaf == None):
                                        
                                        print("L",countLine)
                                        print('cette data leaf exite maintenant '+dicDataCur['Title'])
                                        print(" existe. Hdl trouvé:"+ hdlDataLeaf)
                                        #on a le handle de la collection leaf !
                                        #on le stock dans le dico de data
                                        dicDataCur['Nkl hdl leaf data'] = hdlDataLeaf
                                    else:
                                        errorFatalForThisLine = True
                                        print('le push de la data leaf a du échouer')
                                    
                                else:
                                    errorFatalForThisLine = True
                                    print("[error dataLeaf] :L",countLine,'cette data leaf '+dicDataCur['Title']+ " n'a pas été correctement créee sur nakala")
                                    dicDataCur['Nkl statut'] = 'error push leaf data'
                                
                            except Exception as e:
                                #on peut avoir une levé d'exception par la generation de xml nakala 
                                #ou la creation du .ZIP
                                print(e)
                                errorFatalForThisLine = True   
                                dicDataCur['Nkl statut'] = 'error push leaf data'+str(e)
                            
                    
                        
            ##################################################
            ##### fin de gestion des datas leaf          #####
            ##################################################            
            
            #on ecrit dans le fichier de sortie
            writerOut.writerow(dicDataCur)
            outFile.flush()
            print('----')
            countLine += 1
            
    outFile.close()    
    return True

def metadataFileCSV2NakalaPushV3_test():
    
    """
    une refonte complete du programme de push et update 
    en utilisant la derniere version de nakala console
    """
    
    pathNklCollecInputFolder = "C:/Users/Public/Documents/MSHS/workflow/Guarnido/03-metadatas/nakala/collection/input/"
    pathNklCollecOutputFolder = "C:/Users/Public/Documents/MSHS/workflow/Guarnido/03-metadatas/nakala/collection/output/"
  
    pathNklDataInputFolder = "C:/Users/Public/Documents/MSHS/workflow/Guarnido/03-metadatas/nakala/data/input/"
    pathNklDataOutputFolder = "C:/Users\Public/Documents/MSHS/workflow/Guarnido/03-metadatas/nakala/data/output/"
    
    listColumnRejectedForCollectionBranch = ["Linked in collection leaf","Nkl dataFormat","Nkl dataType","file"]
    listColumnRejectedForCollectionleaf = ["Linked in collection branch","Nkl dataFormat","Nkl dataType","file"]
    listColumnRejectedForDataleaf = ["file"]
        
    pathJava = "C:/Program Files (x86)/Java/jre1.8.0_111/bin/java"

    pathNklConsoleJar = "C:/Users/Public/Documents/MSHS/workflow/code/python/packALA/nakalaConsoleV2/"
    nameConsoleJar = "nakala-console.jar"
    keyApi = "a30fded9-XXXX-XXXX-8f62-e6c0f1f1f241"
    handlePjt = "11280/b28f4a83"
    email = "michael.nauge01@univ-poitiers.fr"
    
    confignkl = objConfigNakalaConsole(pathJava, pathNklConsoleJar, nameConsoleJar, keyApi, handlePjt, email)
    
    pathDataFile = "C:/Users/Public/Documents/MSHS/workflow/Guarnido/03-metadatas/"
    dataFilename = "GuarnidoDeaper_NklReady.csv"
    pathDataFile = pathDataFile+dataFilename
    
    pathOutFile = "C:/Users/Public/Documents/MSHS/workflow/Guarnido/03-metadatas/"
    outFilename = "GuarnidoDeaper_NklHandled.csv"
    pathOutFile = pathOutFile+outFilename
    
    pathMappingFile = "C:/Users/Public/Documents/MSHS/workflow/Guarnido/03-metadatas/nakala/"
    mappingFilename = "mappingShortNkl.csv"
    pathMappingFile = pathMappingFile+mappingFilename


    updateMetas = True
    updateDatas = False
    
    resPushUpdate = metadataFileCSV2NakalaPushV3(confignkl, pathDataFile, pathMappingFile, pathOutFile, pathNklCollecInputFolder, pathNklCollecOutputFolder, pathNklDataInputFolder, pathNklDataOutputFolder, listColumnRejectedForCollectionBranch, listColumnRejectedForCollectionleaf, listColumnRejectedForDataleaf, updateMetas, updateDatas)   
    
    if resPushUpdate:
        print('Tout est PUSH et MAJ !')
    else:
        print('[error] : il y a eu des soucis dans le push et MAJ')
    

def makeDicosForNklXmlMaker(dicMapping, dicDataCur, listColumnRejectedCur, listColumnRejectedSplit=['dcterms:title']):
    """
    fonction facilitant la creation des dictionnary utiles pour la fonction de creation des xml nakala
    return : les deux dictionnary (un contenant les metas dublincore, lautre contenant les metas nakala)
    """   
    dicMeta = {}
    dicNakalaMeta = {}
    #on creer deux dico
    for km in dicMapping:
        if not(dicDataCur[km] == '' or dicDataCur[km]== None):
            if not(dicDataCur[km].lower() == 'none'):
                if not(km in listColumnRejectedCur):
                    if ('dcterms' in dicMapping[km]): 
                        #test le contenu si il y a un | on split pour creer plusieurs instances
                        listValues = []
                        if not(dicMapping[km] in listColumnRejectedSplit) and  ' | ' in dicDataCur[km]:
                            listValues = dicDataCur[km].split(' | ')
                            
                            for v in listValues:
                            #test si la key existe
                                if not(dicMapping[km] in dicMeta):
                                    dicMeta[dicMapping[km]]=[v]
                                else:
                                    dicMeta[dicMapping[km]].append(v)
                                    
                        else:
                            if not(dicMapping[km] in dicMeta):
                                dicMeta[dicMapping[km]]=[dicDataCur[km]]
                            else:
                                dicMeta[dicMapping[km]].append(dicDataCur[km])
                        """
                        #test si la key existe
                        if not(dicMapping[km] in dicMeta):
                            dicMeta[dicMapping[km]]=[dicDataCur[km]]
                        else:
                            dicMeta[dicMapping[km]].append(dicDataCur[km])
                        """
                            
                    elif 'nkl' in dicMapping[km]:
    
                            if not(dicMapping[km] in dicNakalaMeta):
                                dicNakalaMeta[dicMapping[km]]=[dicDataCur[km]]
                            else:
                                dicNakalaMeta[dicMapping[km]].append(dicDataCur[km])
                    else:
                        #normalement il n'y a pas d'autre type de meta
                        pass
            else:
                #print('on rejette la colonne',dicMapping[km])
                pass
                
    return (dicMeta,dicNakalaMeta)
    
    
def metadataFileCSV2NakalaPushV2(objConfigNakalaConsole, pathFileCSV_Source, pathFileCSV_Out, pathFileCSV_Mapp, dicNakalaMetaMother, pathNklCollecInputFolder, pathNklCollecOutputFolder, pathNklDataInputFolder, pathNklDataOutputFolder,listColumnRejectedForCollectionRoot, listColumnRejectedForCollectionleaf, listColumnRejectedForDataleaf):
    """
    fonction permettant d'uploader les données et metadonnees locales
    vers nakala V2 humanum.
    nous devons disposer en entree
    d'un fichier csv contenant une ligne par donnée (data) ainsi que les métas pour cette donnée
    nous conseillons de creer une nkl:colletion par donnée pour plus de flexibilité pour l'ajout de futurs data sur cette data
    
    il faut donc éventuellement également créer des nkl:collection de nkl:collection de données.
    certaines données peuvent etre liées ensemble car appartenant à une meme oeuvre par exemple lorsque lon traite les différentes pages d'un ouvrage
    
    les .zip pour couples fichiers (data) et xml (metadatas)
    dans un dossier .zip pour un import dans nakala
    
    :param pathFileCSV_Source : le chemin vers le fichier CSV contenant des métadonnées dont une column contenant le chemins vers le fichier de donnée
    :type pathFileCSV_Source : str.   
    
    :param pathFileCSV_Out : le chemin vers le fichier CSV de sortie qui acceuillera le même contenu que le FileCSV_Source mais dont des columns additionnels contiennent les 
    :type pathFileCSV_Out : str.   
    
    :param pathFileCSV_Mapp : le chemin vers le fichier csv contenant le mapping entre des champs de metas personnalisées des champs normalisées type DublinCore compatible avec nakala
    :type pathFileCSV_Mapp: str.  
    
    :param dicNakalaMetaMother : le dictionnaire contenant les infos sur la nkl collection mere
    :type dicNakalaMetaMother: dictionnary (devant contenir {'nkl:inCollection':'11280/2b561e65'} avec un collection handle existant)
    
    :returns: None
    :raises: IOerror (si probleme d'acces aux fichiers) 
    """
    
    print('-start export to Nakala-')
    
    #lapp. console genere en autonomie deux sous dossiers (error et ok) dans le dossier output 
    pathNklCollecErrorFolder = pathNklCollecOutputFolder+"error/"
    pathNklDataErrorFolder = pathNklDataOutputFolder+"error/"
    
    #la key api doit etre dans unfichier .TXT pour l'app. nakala-console
    #donc on ecrit la Key dans un fihcier :)
    writeNakalaApiKeyTextFile(objConfigNakalaConsole.pathNakalaConsole,"myNklKey.txt",objConfigNakalaConsole.keyApi)
    
    #on commence par lire le fichier de mapping pour la conversion vers des champs acceptés par Nakala
    #normalement il y a deux lignes
    #la ligne 0 contenant les champs sources
    #la ligne 1 contenant les champs destinations
    dicoMapp = {}
    
    with open(pathFileCSV_Mapp,encoding='utf-8') as csvfileMapp:
        readerMapp = csv.DictReader(csvfileMapp, delimiter=';')
        #on obtient un dico par ligne
        countLine = 1
        for dicMappCur in readerMapp:

            countLine+=1
            
            if countLine==2:
                dicoMapp = dicMappCur
            
        if not(countLine==2):
            print("error [FileCSV_Mapp] : nb ligne presente : ",str(countLine)," attendu :2" )
            
    listChampDistinctTriees= []
    listChampDistinct = set(dicoMapp.values())
    listChampDistinctTriees = sorted(listChampDistinct)
            
    
    ##########################################################################
    ### gestion des collections mere/root (linkedInCollection)
    ##########################################################################
    #on traite ligne par ligne le pathFileCSV_Source
    with open(pathFileCSV_Source, encoding='utf-8') as csvfileSource:
        readerSource = csv.DictReader(csvfileSource, delimiter=';')
        #on peut obtenir un dico par ligne
        countLine = 0
        for dicSourceCur in readerSource:
            countLine+=1
        
            #cas particulier pour la premiere ligne
            if countLine==1:           
            #on va commencer par verifier que tous les champs de ce fichier d'entree
            #sont present dans le dictionnaire de mapping
            #(quil ne manque pas une clef ce qui poserai probleme pdt la conversion...)
                                            
                if not(dicSourceCur.keys()==dicoMapp.keys()):
                    print("tableur metadatas keys",dicSourceCur.keys())
                    print("tableur mapping keys",dicoMapp.keys())
                    raise ValueError("error [FileCSV Source] : probleme de champs present dans pathFileCSV_Source et inexistants dans FileCSV_Mapp")
                
                else:
                    #on a egalite de champs ! youpi
                    print('Le fichier de mapping est compatible avec le tableur de metadatas ')
                    
                    
            
                        
            #nous traitons toutes les lignes de la meme facon  
            #on commence par tester l'existance de groupement par collection grace a la column linkedInCollection
            #si il y a bien du contenu ds cette column il faut creer le .zip pour cette collection (si il n'existe pas dejà)
            if len(dicSourceCur['linkedInCollection'])>0:
                #on test l'existance d'un .xml avec ce nom dans pathNklCollecOutputFolder
                #si il existe cela veut dire que cette collection a deja été envoyé avec succes 
                #lors d'un precedant appel a cette fonction (mais peut etre suite a probleme reseau l'envoi a etait interrompu en cous de route..)
                
                nameXmlFileCur = dicSourceCur['linkedInCollection']+'.xml'
                #patch Nakala ne gere pas bien les accents sur les noms de fichier 
                nameXmlFileCur = unidecode(nameXmlFileCur) 
                    
                pathOutputNameXml = pathNklCollecOutputFolder+nameXmlFileCur

                if not(os.path.isfile(pathOutputNameXml)):
                    #print("le fichier n'existe pas : ",pathOutputNameXml)
                    #on va le creer le .xml et le mettre dans un zip pour un envoi par lot qui va suivre
                    #on verifie que ça n'a aps deja été fait a un tour de boucle precedant (genre plusieurs pages d'un meme ouvrage ...)
                    pathInputNameXml = pathNklCollecInputFolder+nameXmlFileCur
                    if not(os.path.isfile(pathInputNameXml)):
                        #print("le fichier n'existe pas : ",pathInputNameXml)
                        #ok ok on va le creer !
                        
                        #il faut recuperer toutes les métas données de cette ligne pour les mettre dans un .xml de collection puis zip ça
                        #on s'aide du fichier de mapping pour passer les métas personnalisées dans champs de métas standards
                        dicoMetaCur = {}
                        dicoMetaCur['dcterms:title'] = [dicSourceCur['linkedInCollection']]
                        for k in dicSourceCur.keys():
                            #verifie que cette méta doit etre integré 
                            if not(k in listColumnRejectedForCollectionRoot):
                                #on test dans le dico meta si cette key mapped exist deja
                                #si elle n'existe pas on creer une liste pour acceuillir cette premiere valeure
                                #si la cle existe deja, on complete la liste
                                if dicMappCur[k] in dicoMetaCur:
                                    dicoMetaCur[dicMappCur[k]].append(dicSourceCur[k])
                                else:
                                    dicoMetaCur[dicMappCur[k]]=[dicSourceCur[k]]
                                
                        #print('dico cur ',dicoMetaCur)
                        #on peut creer ce .xml
                        writeNakalaCollectionXml(pathInputNameXml, dicoMetaCur, dicNakalaMetaMother)
                        
                    #on met le xml courant dans un zip si ça n'a pas deja étét fait !
                    nameZipFileCur = dicSourceCur['linkedInCollection']+'.zip'
                    nameZipFileCur = unidecode(nameZipFileCur) 
                    pathInputNameZip = pathNklCollecInputFolder+nameZipFileCur
                    if not(os.path.isfile(pathInputNameZip)):
                        with zipfile.ZipFile(pathInputNameZip, 'w') as myzip:
                            myzip.write(pathInputNameXml,arcname=nameXmlFileCur)
                            
                else:
                    #normalement affiche au moins dexu fois chaque collection 
                    #car si il y a une collection, c'est qu'il y a 
                    #au moins deux fichiers à lier ensemble !!
                    print("root collec deja sur nakala ",nameXmlFileCur)      
            
    #a partir d'ici on peut disposer d'une liste de .zip de collection qui doivent etre envoyées sur nakala
    #on regarde chaque .zip present dans input et on verifie qu un xml du meme nom existe dans output 
    #si il existe on supprime le .zip pour eviter un envoi multiple de la meme collecton !
    for file in os.listdir(pathNklCollecInputFolder):
        if file.endswith(".zip"):
            print(file)
            nameXML = file.replace('.zip', '.xml')
            if os.path.isfile(pathNklCollecOutputFolder+nameXML):
                #suppression du .zip dans le dossier input 
                os.remove(pathNklCollecInputFolder+file)
                
            else:
                pass
            
    #envoi des .zip presents dans le dossier Input (linkedInCollection)
    #si il y en a 
    if len(os.listdir(pathNklCollecInputFolder))>0:
        #startNakalaConsoleV2(pathJava, pathNklConsoleJar, nameConsoleJar, pathNklInputFolder, pathNklOutputFolder, emailNkl, handlePjt, pathNklApiKey)
        startNakalaConsoleV2(objConfigNakalaConsole.pathJava, objConfigNakalaConsole.pathNakalaConsole, objConfigNakalaConsole.nameConsoleJar, pathNklCollecInputFolder, pathNklCollecOutputFolder, objConfigNakalaConsole.email, objConfigNakalaConsole.handlePjt,objConfigNakalaConsole.pathNakalaConsole+"myNklKey.txt")
        #on attend pour etre sure de l'instanciation distante
        time.sleep(30)
    
    #on regarde si il y a eu des errors
    #si c'est le cas on lève une exception pour tt arreter !
    
    #on commence par verifier si il existe un dossier error :
    if os.path.exists(pathNklCollecErrorFolder):
        
        for file in os.listdir(pathNklCollecErrorFolder):
            if file.endswith(".zip"):
                raise ValueError("error nakala ", file)
            else:
                pass
    
    
    
    ##########################################################################
    ### gestion des collections filles/feuilles 
    ### on prend le parti d'avoir une collection par data
    ### cela permet de facilement aggréger de futurs données sur cette data
    ##########################################################################     
    

    #le code qui va suivre ressemble beaucoup au code precedent mais a quelques differences
    #mettre ça dans une fonction generique qui gere les cas particuliers risque defaire faire perdre en lisibilité
    #donc difficilement maintenable
    #cependant ça aurait été cool quand même d'avoir une belle fonction... bon on laisse ça comme ça
    #pour le moment on corrigera ce défaut pour une prochaine release ... peut etre 
    
    #aller on repart pour une lecture                        
    #on traite ligne par ligne le pathFileCSV_Source
    with open(pathFileCSV_Source, encoding='utf-8') as csvfileSource:
        readerSource = csv.DictReader(csvfileSource, delimiter=';')
        countLine = 0
        for dicSourceCur in readerSource:
            countLine+=1
            if countLine==1:                                                      
                if not(dicSourceCur.keys()==dicoMapp.keys()):
                    raise ValueError("error [FileCSV Source] : probleme de champs present dans pathFileCSV_Source et inexistants dans FileCSV_Mapp")
                else:
                    print('fichier de mapping acceptable')
                        
            #nous traitons toutes les lignes de la meme facon  
            #on s'interresse maitenant a la data/file/le fichier/ le file
            if len(dicSourceCur['file'])>0: 
                #on test l'existance d'un .xml avec ce nom dans pathNklCollecOutputFolder
                #si il existe cela veut dire que cette collection a deja été envoyé avec succes 
                #lors d'un precedant appel a cette fonction (mais peut etre suite a probleme reseau l'envoi a etait interrompu en cous de route..)

                head, tail = os.path.split(dicSourceCur['file']) 
                nameXmlFileCur = tail+'.xml'
                #patch Nakala ne gere pas bien les accents sur les noms de fichier 
                nameXmlFileCur = unidecode(nameXmlFileCur) 
                pathOutputNameXml = pathNklCollecOutputFolder+nameXmlFileCur

                if not(os.path.isfile(pathOutputNameXml)):
                    #print("le fichier n'existe pas : ",pathOutputNameXml)
                    #on va le creer le .xml et le mettre dans un zip pour un envoi par lot qui va suivre
                    #on verifie que ça n'a pas deja été fait a un tour de boucle precedant (genre plusieurs pages d'un meme ouvrage ...)
                    
                    pathInputNameXml = pathNklCollecInputFolder+nameXmlFileCur
                    if not(os.path.isfile(pathInputNameXml)):
                        #print("le fichier n'existe pas : ",pathInputNameXml)
                       
                        #il faut recuperer toutes les métas données de cette ligne pour les mettre dans un .xml de collection puis zip ça
                        #on s'aide du fichier de mapping pour passer les métas personnalisées dans champs de métas standards
                        dicoMetaCur = {}
                        
                        #on met le nom de fichier comme title de cette collection
                        #on sous entends que le nom de fichier est pertinent
                        nameFileSansExt = tail.split('.')
                        dicoMetaCur['dcterms:title'] = [nameFileSansExt[0]]
                        for k in dicSourceCur.keys():
                            #verifie que cette méta doit etre integré 
                            if not(k in listColumnRejectedForCollectionleaf):
                                #on test dans le dico meta si cette key mapped exist deja
                                #si elle n'existe pas on creer une liste pour acceuillir cette premiere valeure
                                #si la cle existe deja, on complete la liste
                                if dicMappCur[k] in dicoMetaCur:
                                    dicoMetaCur[dicMappCur[k]].append(dicSourceCur[k])
                                else:
                                    dicoMetaCur[dicMappCur[k]]=[dicSourceCur[k]]

                        #on peut creer ce .xml
                        #on doit ajouter la bonne nkl collection
                        #on test si ya du linkedincollection
                        if len(dicSourceCur['linkedInCollection'])>0:
                            #si oui on va chercher le handle nakala
                            #pour ca on va chercher le fichier .xml dans le dossier output
                            #le lire pour en extraire le handle
                            nameXmlFileCur = dicSourceCur['linkedInCollection']+'.xml'
                            nameXmlFileCur = unidecode(nameXmlFileCur) 
                            pathOutputNameXml = pathNklCollecOutputFolder+nameXmlFileCur
                            handle = readReturnNakalaXml(pathOutputNameXml)
                            dicNakalaMetaLeaf = {}
                            dicNakalaMetaLeaf.update({'nkl:inCollection':[handle]})
                            writeNakalaCollectionXml(pathInputNameXml, dicoMetaCur, dicNakalaMetaLeaf)
                       
                        else:
                            writeNakalaCollectionXml(pathInputNameXml, dicoMetaCur, dicNakalaMetaMother)
                        
                    #on met le xml courant dans un zip si ça n'a pas deja été fait !
                    nameZipFileCur = tail+'.zip'
                    nameZipFileCur = unidecode(nameZipFileCur) 
                    pathInputNameZip = pathNklCollecInputFolder+nameZipFileCur
                    if not(os.path.isfile(pathInputNameZip)):
                        with zipfile.ZipFile(pathInputNameZip, 'w') as myzip:
                            myzip.write(pathInputNameXml,arcname=nameXmlFileCur)
                            
                else:
                    #normalement affiche u'une fois chaque collection car elle fait referenece a une data unique 
                    print("leaf collec deja sur nakala ",nameXmlFileCur)                 
    
    #a partir d'ici on peut disposer d'une liste de .zip de collection leaf qui doivent etre envoyées sur nakala
    #on regarde chaque .zip present dans input et on verifie qu un xml du meme nom existe dans output 
    #si il existe on supprime le .zip pour eviter un envoi multiple de la meme collecton !
    
    #on commence par verifier si il existe un dossier error :
    if os.path.exists(pathNklCollecErrorFolder):
        for file in os.listdir(pathNklCollecInputFolder):
            if file.endswith(".zip"):
                print(file)
                nameXML = file.replace('.zip', '.xml')
                if os.path.isfile(pathNklCollecOutputFolder+nameXML):
                    #suppression du .zip dans le dossier input 
                    os.remove(pathNklCollecInputFolder+file)
                    
                else:
                    pass
            
    #envoi des .zip presentes dans le dossier Input (linkedInCollection)
    if len(os.listdir(pathNklCollecInputFolder))>0:
        #startNakalaConsoleV2(pathJava, pathNklConsoleJar, nameConsoleJar, pathNklInputFolder, pathNklOutputFolder, emailNkl, handlePjt, pathNklApiKey)
        startNakalaConsoleV2(objConfigNakalaConsole.pathJava, objConfigNakalaConsole.pathNakalaConsole, objConfigNakalaConsole.nameConsoleJar, pathNklCollecInputFolder, pathNklCollecOutputFolder, objConfigNakalaConsole.email, objConfigNakalaConsole.handlePjt,objConfigNakalaConsole.pathNakalaConsole+"myNklKey.txt")
        #on attend pour etre sure de l'instanciation distante
        time.sleep(30)
    
    #on regarde si il y a eu des errors
    if os.path.exists(pathNklCollecErrorFolder): 
        for file in os.listdir(pathNklCollecErrorFolder):
            if file.endswith(".zip"):
                raise ValueError("error nakala ", file)
            else:
                pass
    
    
    ##########################################################################
    ### gestion des DATA / envoi des images ! 
    ##########################################################################     

    #le code qui va suivre ressemble beaucoup au code precedent mais a quelques differences
    #mettre ça dans une fonction generique qui gere les cas particuliers risque defaire faire perdre en lisibilité
    #donc difficilement maitenable
    #cependant ça aurait été cool quand même d'avoir une belle fonction... bon on laisse ça comme ça
    #pour le moment on corrigera ce défaut pour une prochaine release ... peut etre 
    #aller on repart encore pour une lecture                        
    #on traite ligne par ligne le pathFileCSV_Source
    with open(pathFileCSV_Source, encoding='utf-8') as csvfileSource:
        readerSource = csv.DictReader(csvfileSource, delimiter=';')
        countLine = 0
        for dicSourceCur in readerSource:
            countLine+=1
            if countLine==1:                                                      
                if not(dicSourceCur.keys()==dicoMapp.keys()):
                    raise ValueError("error [FileCSV Source] : probleme de champs present dans pathFileCSV_Source et inexistants dans FileCSV_Mapp")
                else:
                    print('fichier de mapping acceptable')
                        
            #nous traitons toutes les lignes de la meme facon  
            #on s'interresse maitenant a la data/file/le fichier
            if len(dicSourceCur['file'])>0: 
                #on test l'existance d'un .xml avec ce nom dans pathNklDataOutputFolder
                #si il existe cela veut dire que cette data a deja été envoyé avec succes 
                #lors d'un precedant appel a cette fonction (mais peut etre suite a probleme reseau l'envoi a etait interrompu en cous de route..)

                head, tail = os.path.split(dicSourceCur['file']) 
                nameXmlFileCur = tail+'.xml'
                #patch Nakala ne gere pas bien les accents sur les noms de fichier 
                nameXmlFileCur = unidecode(nameXmlFileCur) 
                pathOutputNameXml = pathNklDataOutputFolder+nameXmlFileCur
                
                pathInputNameXml = pathNklDataInputFolder+nameXmlFileCur

                if not(os.path.isfile(pathOutputNameXml)):
                    #on va le creer le .xml et le mettre dans un zip pour un envoi par lot qui va suivre
                    #on verifie que ça n'a pas deja été fait a un tour de boucle precedant
                    

                    if not(os.path.isfile(pathInputNameXml)):
                        #print("le fichier n'existe pas : ",pathInputNameXml)
                       
                        #il faut recuperer toutes les métas données de cette ligne pour les mettre dans un .xml de collection puis zip ça
                        #on s'aide du fichier de mapping pour passer les métas personnalisées dans champs de métas standards
                        dicoMetaCur = {}
                        dicNakalaMetaLeafData = {}
                        
                        #on met le nom de fichier comme title de cette data
                        #on sous entends que le nom de fichier est pertinent
                        nameFileSansExt = tail.split('.')
                        dicoMetaCur['dcterms:title'] = [nameFileSansExt[0]]
                        for k in dicSourceCur.keys():
                            #verifie que cette méta doit etre integré 
                            if not(k in listColumnRejectedForDataleaf):
                                #on test dans le dico meta si cette key mapped exist deja
                                #si elle n'existe pas on creer une liste pour acceuillir cette premiere valeure
                                #si la cle existe deja, on complete la liste
                                #on ajoute pas les key 'nkl'
                                if not('nkl' in dicMappCur[k]):
                                    if dicMappCur[k] in dicoMetaCur:
                                        dicoMetaCur[dicMappCur[k]].append(dicSourceCur[k])
                                    else:
                                        dicoMetaCur[dicMappCur[k]]=[dicSourceCur[k]]
                                else:
                                    #si ya du nkl on met ça dans le dico nkl
                                    if dicMappCur[k] in dicNakalaMetaLeafData:
                                        dicNakalaMetaLeafData[dicMappCur[k]].append(dicSourceCur[k])
                                    else:
                                        dicNakalaMetaLeafData[dicMappCur[k]]=[dicSourceCur[k]]

                        #on peut creer ce .xml
                        #on doit ajouter  le handle nkl collection leaf
                        #pour ca on va chercher le fichier .xml dans le dossier output collection 
                        #le lire pour en extraire le handle
                        #(pour le moment on garde une egalité de nom entre la collection leaf et la data)
                        pathOutputNameXml = pathNklCollecOutputFolder+nameXmlFileCur
                        handle = readReturnNakalaXml(pathOutputNameXml)
                        dicNakalaMetaLeafData.update({'nkl:inCollection':[handle]})
                        writeNakalaMetadataXml(pathInputNameXml, dicoMetaCur, dicNakalaMetaLeafData)
                   

                        
                #on met le xml courant dans un zip si ça n'a pas deja été fait !
                nameZipFileCur = tail+'.zip'
                nameZipFileCur = unidecode(nameZipFileCur) 
                pathInputNameZip = pathNklDataInputFolder+nameZipFileCur
                if not(os.path.isfile(pathInputNameZip)):
                    with zipfile.ZipFile(pathInputNameZip, 'w') as myzip:
                        myzip.write(pathInputNameXml,arcname=nameXmlFileCur)
                        #on ajoute aussi et surtout la data dans le zip 
                        cheminVersLaData = dicSourceCur['file'] 
                        head, tail = os.path.split(dicSourceCur['file']) 
                        nameData = tail
                        myzip.write(cheminVersLaData, arcname=nameData)
                            
                else:
                    #normalement affiche une fois par data
                    print("data deja sur nakala ",nameXmlFileCur)      
                    
    #a partir d'ici on peut disposer d'une liste de .zip de data qui doivent etre envoyées sur nakala
    #on regarde chaque .zip present dans data input et on verifie qu un xml du meme nom existe dans data output 
    #si il existe on supprime le .zip pour eviter un envoi multiple de la meme collection 
    #(la console nakala ne fait pas de controle basé sur les envois précédents) 
    if os.path.exists(pathNklDataErrorFolder): 
        for file in os.listdir(pathNklDataInputFolder):
            if file.endswith(".zip"):
                print(file)
                nameXML = file.replace('.zip', '.xml')
                if os.path.isfile(pathNklDataOutputFolder+nameXML):
                    #suppression du .zip dans le dossier input 
                    os.remove(pathNklDataInputFolder+file)
                    
                else:
                    pass
            
    #envoi des .zip present dans le dossier data Input
    if len(os.listdir(pathNklDataInputFolder))>0:
        
        startNakalaConsoleV2(objConfigNakalaConsole.pathJava, objConfigNakalaConsole.pathNakalaConsole, objConfigNakalaConsole.nameConsoleJar, pathNklDataInputFolder, pathNklDataOutputFolder, objConfigNakalaConsole.email, objConfigNakalaConsole.handlePjt,objConfigNakalaConsole.pathNakalaConsole+"myNklKey.txt")
        #on attend pour etre sure de l'instanciation distante
        time.sleep(30)
    
    #on regarde si il y a eu des errors
    if os.path.exists(pathNklDataErrorFolder):
        for file in os.listdir(pathNklDataErrorFolder):
            if file.endswith(".zip"):
                raise ValueError("error nakala ", file)
            else:
                pass
        
    #bon en bonus ça serait cool de générer un tableur qui contient toutes les métas et les handles (data et collection) 
    #on ouvre encore ue fois le fichier d'entree
    #et le fichier de sortie
    with open(pathFileCSV_Source, encoding='utf-8') as csvfileSource:
        readerSource = csv.DictReader(csvfileSource, delimiter=';')
        
        #on ouvre le fichier Out
        with open(pathFileCSV_Out, 'w', encoding='utf-8') as csvfileOut:
            csvfileOut = csv.writer(csvfileOut, delimiter=';')
            
            listChampDistinctTriees= []
            listChampDistinct = set(dicSourceCur.keys())
            listChampDistinctTriees = sorted(listChampDistinct)
                

            countLine = 0
            for dicSourceCur in readerSource:
                countLine+=1
                #print(len(dicSourceCur))
                #cas particulier pour la premiere ligne
                # va reecrire les noms de column du fichier dentree plus le nom des columns additionnelles
                if countLine==1:
                    rowOut = list(listChampDistinctTriees)
                    rowOut.append('HandleCollecRoot')
                    rowOut.append('HandleCollecLeaf')
                    rowOut.append('HandleDataLeaf')
                    rowOut.append('HandleMetadataLeaf')
                    
                    csvfileOut.writerow(rowOut)
                   
                handleCollecRoot = None
                handleCollecLeaf = None
                handleDataLeaf = None
                
                #on va obtenir les différents handles nakala en lisant les fichiers retournées
                #on test si ya du linkedincollection pour obtenir un eventuel handle collection root
                if len(dicSourceCur['linkedInCollection'])>0:
                    nameXmlFileCur = dicSourceCur['linkedInCollection']+'.xml'
                    nameXmlFileCur = unidecode(nameXmlFileCur) 
                    pathOutputNameXml = pathNklCollecOutputFolder+"ok/"+nameXmlFileCur
                    handleCollecRoot = readReturnNakalaXml(pathOutputNameXml)
                    print('handleCollecRoot ',handleCollecRoot)
                 
                #on recupere le handle de la collec leaf
                if len(dicSourceCur['file'])>0:
                    head, tail = os.path.split(dicSourceCur['file']) 
                    nameXmlFileCur = tail+'.xml'
                    nameXmlFileCur = unidecode(nameXmlFileCur) 
                    pathOutputNameXml = pathNklCollecOutputFolder+"ok/"+nameXmlFileCur
                    handleCollecLeaf = readReturnNakalaXml(pathOutputNameXml)
                    print('handleCollecLeaf ', handleCollecLeaf)
                #on recupere le handle de la data leaf
                if len(dicSourceCur['file'])>0:
                    head, tail = os.path.split(dicSourceCur['file']) 
                    nameXmlFileCurData = tail+'.xml'
                    nameXmlFileCurData = unidecode(nameXmlFileCurData) 
                    pathOutputNameXmlData = pathNklDataOutputFolder+"ok/"+nameXmlFileCurData
                    handleDataLeaf = readReturnNakalaXml(pathOutputNameXmlData)
                    print('handleDataLeaf ', handleDataLeaf)
                    
                    #maintenant qu'on a tout on peut ecrire dans le fichier de sortie
                    rowOut = []
                    for columName in listChampDistinctTriees:
                        rowOut.append(dicSourceCur[columName])
                    if handleCollecRoot == None:
                        rowOut.append(None)
                    else:
                        rowOut.append('https://www.nakala.fr/page/collection/'+handleCollecRoot)
                        
                    rowOut.append('https://www.nakala.fr/page/collection/'+handleCollecLeaf)
                    rowOut.append('hdl.handle.net/'+handleDataLeaf)
                    rowOut.append('https://www.nakala.fr/metadata/'+handleDataLeaf)
                
                    csvfileOut.writerow(rowOut)
    
                
    print('-end export to Nakala-')
    

def metadataFileCSV2NakalaPush(pathFileCSV_Source, pathFileCSV_Out, pathFileCSV_Mapp, dicNakalaMetaMother, pathJava, pathNklConsoleJar, nameConsoleJar, pathNklCollecInputFolder, pathNklCollecOutputFolder, pathNklCollecErrorFolder, pathNklDataInputFolder, pathNklDataOutputFolder, pathNklDataErrorFolder, emailNkl, pathPassFileSHA, listColumnRejectedForCollectionRoot, listColumnRejectedForCollectionleaf, listColumnRejectedForDataleaf):
    """
    fonction permettant d'uploader les données et metadonnees locales
    vers nakala humanum.
    nous devons disposer en entree
    d'un fichier csv contenant une ligne par donnée (data) ainsi que les métas pour cette donnée
    il faut creer une nkl:colletion par donnée
    certaines données peuvent etre liées ensemble car appartenant à une meme oeuvre par exemple lorsque lon traite les différentes pages d'un ouvrage
    
    il faut donc éventuellement également créer des nkl:collection de nkl:collection de données.
    
    les .zip pour  couples fichiers (data) et xml (metadatas)
    dans un dossier .zip pour un import dans nakala
    
    :param pathFileCSV_Source : le chemin vers le fichier CSV contenant des métadonnées dont une column contenant le chemins vers le fichier de donnée
    :type pathFileCSV_Source : str.   
    
    :param pathFileCSV_Out : le chemin vers le fichier CSV de sortie ayant le même contenu que le FileCSV_Source mais dont des columns additionnels contiennent les 
    :type pathFileCSV_Out : str.   
    
    :param pathFileCSV_Mapp : le chemin vers le fichier csv contenant le mapping entre des champs de metas personnalisées des champs normalisées type DublinCore compatible avec nakala
    :type pathFileCSV_Mapp: str.  
    
    :param dicNakalaMetaMother : le dictionnaire contenant les infos sur la nkl collection mere
    :type dicNakalaMetaMother: dictionnary (devant contenir {'nkl:inCollection':'11280/2b561e65'} avec un collection handle existant)
    
    :returns: None
    :raises: IOerror (si probleme d'acces aux fichiers) 
    """
    
    print('-start export to Nakala-')
    
    separator = "|"
    #on commence par lire le fichier de mapping pour la conversion vers des champs accepté par Nakala
    #normalement il y a deux lignes
    #la ligne 0 contenant les champs source
    #la ligne 1 contenant les champs destination
    dicoMapp = {}
    
    with open(pathFileCSV_Mapp,encoding='utf-8') as csvfileMapp:
        readerMapp = csv.DictReader(csvfileMapp, delimiter=';')
        #on peut obtenir un dico par ligne
        countLine = 1
        for dicMappCur in readerMapp:

            countLine+=1
            
            if countLine==2:
                dicoMapp = dicMappCur
            
        if not(countLine==2):
            print("error [FileCSV_Mapp] : nb ligne presente : ",str(countLine)," attendu :2" )
            
    listChampDistinctTriees= []
    listChampDistinct = set(dicoMapp.values())
    listChampDistinctTriees = sorted(listChampDistinct)
            
    
    ##########################################################################
    ### gestion des collections mere/root (linkedInCollection)
    ##########################################################################
    #on traite ligne par ligne le pathFileCSV_Source
    with open(pathFileCSV_Source, encoding='utf-8') as csvfileSource:
        readerSource = csv.DictReader(csvfileSource, delimiter=';')
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
                    print("tableur metadatas keys",dicSourceCur.keys())
                    print("tableur mapping keys",dicoMapp.keys())
                    raise ValueError("error [FileCSV Source] : probleme de champs present dans pathFileCSV_Source et inexistants dans FileCSV_Mapp")
                else:
                    #on a egalite de champs ! youpi
                    print('Le fichier de mapping est compatible avec le tableur de metadatas ')
                        
            #nous traitons toutes les lignes de la meme facon  
            #on commence par tester l'existance de groupement par collection grace a la column linkedInCollection
            #si il y a bien du contenant ds cette column il faut creer le .zip pour cette collection (si il n'existe pas dejà)
            if len(dicSourceCur['linkedInCollection'])>0:
                #on test l'existance d'un .xml avec ce nom dans pathNklCollecOutputFolder
                #si il existe cela veut dire que cette collection a deja été envoyé avec succes 
                #lors d'un precedant appel a cette fonction (mais peut etre suite a probleme reseau l'envoi a etait interrompu en cous de route..)
                
                nameXmlFileCur = dicSourceCur['linkedInCollection']+'.xml'
                #patch Nakala ne gere pas bien les accents sur les noms de fichier 
                nameXmlFileCur = unidecode(nameXmlFileCur) 
                    
                pathOutputNameXml = pathNklCollecOutputFolder+nameXmlFileCur

                if not(os.path.isfile(pathOutputNameXml)):
                    #print("le fichier n'existe pas : ",pathOutputNameXml)
                    #on va le creer le .xml et le mettre dans un zip pour un envoi par lot qui va suivre
                    #on verifie que ça n'a aps deja été fait a un tour de boucle precedant (genre plusieurs pages d'un meme ouvrage ...)

                    
                    pathInputNameXml = pathNklCollecInputFolder+nameXmlFileCur
                    if not(os.path.isfile(pathInputNameXml)):
                        #print("le fichier n'existe pas : ",pathInputNameXml)
                        #ok ok on va le creer !
                        
                        #il faut recuperer toutes les métas données de cette ligne pour les mettre dans un .xml de collection puis zip ça
                        #on s'aide du fichier de mapping pour passer les métas personnalisées dans champs de métas standards
                        dicoMetaCur = {}
                        dicoMetaCur['dcterms:title'] = [dicSourceCur['linkedInCollection']]
                        for k in dicSourceCur.keys():
                            #verifie que cette méta doit etre integré 
                            if not(k in listColumnRejectedForCollectionRoot):
                                #on test dans le dico meta si cette key mapped exist deja
                                #si elle n'existe pas on creer une liste pour acceuillir cette premiere valeure
                                #si la cle existe deja, on complete la liste
                                if dicMappCur[k] in dicoMetaCur:
                                    dicoMetaCur[dicMappCur[k]].append(dicSourceCur[k])
                                else:
                                    dicoMetaCur[dicMappCur[k]]=[dicSourceCur[k]]
                                
                        #print('dico cur ',dicoMetaCur)
                        #on peut creer ce .xml
                        writeNakalaCollectionXml(pathInputNameXml, dicoMetaCur, dicNakalaMetaMother)
                        
                    #on met le xml courant dans un zip si ça n'a pas deja étét fait !
                    nameZipFileCur = dicSourceCur['linkedInCollection']+'.zip'
                    nameZipFileCur = unidecode(nameZipFileCur) 
                    pathInputNameZip = pathNklCollecInputFolder+nameZipFileCur
                    if not(os.path.isfile(pathInputNameZip)):
                        with zipfile.ZipFile(pathInputNameZip, 'w') as myzip:
                            myzip.write(pathInputNameXml,arcname=nameXmlFileCur)
                            
                else:
                    #normalement affiche au moins dexu fois chaque collection 
                    #car si il y a une collection, c'est qu'il y a 
                    #au moins deux fichiers à lier ensemble !!
                    print("root collec deja sur nakala ",nameXmlFileCur)      
            
    #a partir d'ici on peut disposer d'une liste de .zip de collection qui doivent etre envoyées sur nakala
    #on regarde chaque .zip present dans input et on verifie qu un xml du meme nom existe dans output 
    #si il existe on supprime le .zip pour eviter un envoi multiple de la meme collecton !
    for file in os.listdir(pathNklCollecInputFolder):
        if file.endswith(".zip"):
            print(file)
            nameXML = file.replace('.zip', '.xml')
            if os.path.isfile(pathNklCollecOutputFolder+nameXML):
                #suppression du .zip dans le dossier input 
                os.remove(pathNklCollecInputFolder+file)
                
            else:
                pass
            
    #envoi des .zip presentes dans le dossier Input (linkedInCollection)
    #si il y en a 
    if len(os.listdir(pathNklCollecInputFolder))>0:
        startNakalaConsole(pathJava, pathNklConsoleJar, nameConsoleJar, pathNklCollecInputFolder, pathNklCollecOutputFolder, pathNklCollecErrorFolder, emailNkl, pathPassFileSHA)
        #on attend pour etre sure de l'instanciation distante
        time.sleep(30)
    
    #on regarde si il y a eu des errors
    #si c'est le cas on lève une exception pour tt arreter !
    for file in os.listdir(pathNklCollecErrorFolder):
        if file.endswith(".zip"):
            raise ValueError("error nakala ", file)
        else:
            pass
    
    ##########################################################################
    ### gestion des collections filles/feuilles 
    ### on prend le parti d'avoir une collection par data
    ### cela permet de facilement aggréger de futurs données sur cette data
    ##########################################################################     

    #le code qui va suivre ressemble beaucoup au code precedent mais a quelques differences
    #mettre ça dans une fonction generique qui gere les cas particuliers risque defaire faire perdre en lisibilité
    #donc difficilement maitenable
    #cependant ça aurait été cool quand même d'avoir une belle fonction... bon on laisse ça comme ça
    #pour le moment on corrigera ce défaut pour une prochaine release ... peut etre 
    #aller on repart pour une lecture                        
    #on traite ligne par ligne le pathFileCSV_Source
    with open(pathFileCSV_Source, encoding='utf-8') as csvfileSource:
        readerSource = csv.DictReader(csvfileSource, delimiter=';')
        countLine = 0
        for dicSourceCur in readerSource:
            countLine+=1
            if countLine==1:                                                      
                if not(dicSourceCur.keys()==dicoMapp.keys()):
                    raise ValueError("error [FileCSV Source] : probleme de champs present dans pathFileCSV_Source et inexistants dans FileCSV_Mapp")
                else:
                    print('fichier de mapping acceptable')
                        
            #nous traitons toutes les lignes de la meme facon  
            #on s'interresse maitenant a la data/file/le fichier/ le file
            if len(dicSourceCur['file'])>0: 
                #on test l'existance d'un .xml avec ce nom dans pathNklCollecOutputFolder
                #si il existe cela veut dire que cette collection a deja été envoyé avec succes 
                #lors d'un precedant appel a cette fonction (mais peut etre suite a probleme reseau l'envoi a etait interrompu en cous de route..)

                head, tail = os.path.split(dicSourceCur['file']) 
                nameXmlFileCur = tail+'.xml'
                #patch Nakala ne gere pas bien les accents sur les noms de fichier 
                nameXmlFileCur = unidecode(nameXmlFileCur) 
                pathOutputNameXml = pathNklCollecOutputFolder+nameXmlFileCur

                if not(os.path.isfile(pathOutputNameXml)):
                    #print("le fichier n'existe pas : ",pathOutputNameXml)
                    #on va le creer le .xml et le mettre dans un zip pour un envoi par lot qui va suivre
                    #on verifie que ça n'a pas deja été fait a un tour de boucle precedant (genre plusieurs pages d'un meme ouvrage ...)
                    
                    pathInputNameXml = pathNklCollecInputFolder+nameXmlFileCur
                    if not(os.path.isfile(pathInputNameXml)):
                        #print("le fichier n'existe pas : ",pathInputNameXml)
                       
                        #il faut recuperer toutes les métas données de cette ligne pour les mettre dans un .xml de collection puis zip ça
                        #on s'aide du fichier de mapping pour passer les métas personnalisées dans champs de métas standards
                        dicoMetaCur = {}
                        
                        #on met le nom de fichier comme title de cette collection
                        #on sous entends que le nom de fichier est pertinent
                        nameFileSansExt = tail.split('.')
                        dicoMetaCur['dcterms:title'] = [nameFileSansExt[0]]
                        for k in dicSourceCur.keys():
                            #verifie que cette méta doit etre integré 
                            if not(k in listColumnRejectedForCollectionleaf):
                                #on test dans le dico meta si cette key mapped exist deja
                                #si elle n'existe pas on creer une liste pour acceuillir cette premiere valeure
                                #si la cle existe deja, on complete la liste
                                if dicMappCur[k] in dicoMetaCur:
                                    dicoMetaCur[dicMappCur[k]].append(dicSourceCur[k])
                                else:
                                    dicoMetaCur[dicMappCur[k]]=[dicSourceCur[k]]

                        #on peut creer ce .xml
                        #on doit ajouter la bonne nkl collection
                        #on test si ya du linkedincollection
                        if len(dicSourceCur['linkedInCollection'])>0:
                            #si oui on va chercher le handle nakala
                            #pour ca on va chercher le fichier .xml dans le dossier output
                            #le lire pour en extraire le handle
                            nameXmlFileCur = dicSourceCur['linkedInCollection']+'.xml'
                            nameXmlFileCur = unidecode(nameXmlFileCur) 
                            pathOutputNameXml = pathNklCollecOutputFolder+nameXmlFileCur
                            handle = readReturnNakalaXml(pathOutputNameXml)
                            dicNakalaMetaLeaf = {}
                            dicNakalaMetaLeaf.update({'nkl:inCollection':[handle]})
                            writeNakalaCollectionXml(pathInputNameXml, dicoMetaCur, dicNakalaMetaLeaf)
                       
                        else:
                            writeNakalaCollectionXml(pathInputNameXml, dicoMetaCur, dicNakalaMetaMother)
                        
                    #on met le xml courant dans un zip si ça n'a pas deja été fait !
                    nameZipFileCur = tail+'.zip'
                    nameZipFileCur = unidecode(nameZipFileCur) 
                    pathInputNameZip = pathNklCollecInputFolder+nameZipFileCur
                    if not(os.path.isfile(pathInputNameZip)):
                        with zipfile.ZipFile(pathInputNameZip, 'w') as myzip:
                            myzip.write(pathInputNameXml,arcname=nameXmlFileCur)
                            
                else:
                    #normalement affiche u'une fois chaque collection car elle fait referenece a une data unique 
                    print("leaf collec deja sur nakala ",nameXmlFileCur)                 
    
    #a partir d'ici on peut disposer d'une liste de .zip de collection leaf qui doivent etre envoyées sur nakala
    #on regarde chaque .zip present dans input et on verifie qu un xml du meme nom existe dans output 
    #si il existe on supprime le .zip pour eviter un envoi multiple de la meme collecton !
    for file in os.listdir(pathNklCollecInputFolder):
        if file.endswith(".zip"):
            print(file)
            nameXML = file.replace('.zip', '.xml')
            if os.path.isfile(pathNklCollecOutputFolder+nameXML):
                #suppression du .zip dans le dossier input 
                os.remove(pathNklCollecInputFolder+file)
                
            else:
                pass
            
    #envoi des .zip presentes dans le dossier Input (linkedInCollection)
    if len(os.listdir(pathNklCollecInputFolder))>0:
        startNakalaConsole(pathJava, pathNklConsoleJar, nameConsoleJar, pathNklCollecInputFolder, pathNklCollecOutputFolder, pathNklCollecErrorFolder, emailNkl, pathPassFileSHA)
        time.sleep(30)
    
    #on regarde si il y a eu des errors
    for file in os.listdir(pathNklCollecErrorFolder):
        if file.endswith(".zip"):
            raise ValueError("error nakala ", file)
        else:
            pass
    
    ##########################################################################
    ### gestion des DATA / envoi des images !!! 
    ##########################################################################     

    #le code qui va suivre ressemble beaucoup au code precedent mais a quelques differences
    #mettre ça dans une fonction generique qui gere les cas particuliers risque defaire faire perdre en lisibilité
    #donc difficilement maitenable
    #cependant ça aurait été cool quand même d'avoir une belle fonction... bon on laisse ça comme ça
    #pour le moment on corrigera ce défaut pour une prochaine release ... peut etre 
    #aller on repart encore pour une lecture                        
    #on traite ligne par ligne le pathFileCSV_Source
    with open(pathFileCSV_Source, encoding='utf-8') as csvfileSource:
        readerSource = csv.DictReader(csvfileSource, delimiter=';')
        countLine = 0
        for dicSourceCur in readerSource:
            countLine+=1
            if countLine==1:                                                      
                if not(dicSourceCur.keys()==dicoMapp.keys()):
                    raise ValueError("error [FileCSV Source] : probleme de champs present dans pathFileCSV_Source et inexistants dans FileCSV_Mapp")
                else:
                    print('fichier de mapping acceptable')
                        
            #nous traitons toutes les lignes de la meme facon  
            #on s'interresse maitenant a la data/file/le fichier
            if len(dicSourceCur['file'])>0: 
                #on test l'existance d'un .xml avec ce nom dans pathNklDataOutputFolder
                #si il existe cela veut dire que cette data a deja été envoyé avec succes 
                #lors d'un precedant appel a cette fonction (mais peut etre suite a probleme reseau l'envoi a etait interrompu en cous de route..)

                head, tail = os.path.split(dicSourceCur['file']) 
                nameXmlFileCur = tail+'.xml'
                #patch Nakala ne gere pas bien les accents sur les noms de fichier 
                nameXmlFileCur = unidecode(nameXmlFileCur) 
                pathOutputNameXml = pathNklDataOutputFolder+nameXmlFileCur
                
                pathInputNameXml = pathNklDataInputFolder+nameXmlFileCur

                if not(os.path.isfile(pathOutputNameXml)):
                    #on va le creer le .xml et le mettre dans un zip pour un envoi par lot qui va suivre
                    #on verifie que ça n'a pas deja été fait a un tour de boucle precedant
                    

                    if not(os.path.isfile(pathInputNameXml)):
                        #print("le fichier n'existe pas : ",pathInputNameXml)
                       
                        #il faut recuperer toutes les métas données de cette ligne pour les mettre dans un .xml de collection puis zip ça
                        #on s'aide du fichier de mapping pour passer les métas personnalisées dans champs de métas standards
                        dicoMetaCur = {}
                        dicNakalaMetaLeafData = {}
                        
                        #on met le nom de fichier comme title de cette data
                        #on sous entends que le nom de fichier est pertinent
                        nameFileSansExt = tail.split('.')
                        dicoMetaCur['dcterms:title'] = [nameFileSansExt[0]]
                        for k in dicSourceCur.keys():
                            #verifie que cette méta doit etre integré 
                            if not(k in listColumnRejectedForDataleaf):
                                #on test dans le dico meta si cette key mapped exist deja
                                #si elle n'existe pas on creer une liste pour acceuillir cette premiere valeure
                                #si la cle existe deja, on complete la liste
                                #on ajoute pas les key 'nkl'
                                if not('nkl' in dicMappCur[k]):
                                    if dicMappCur[k] in dicoMetaCur:
                                        dicoMetaCur[dicMappCur[k]].append(dicSourceCur[k])
                                    else:
                                        dicoMetaCur[dicMappCur[k]]=[dicSourceCur[k]]
                                else:
                                    #si ya du nkl on met ça dans le dico nkl
                                    if dicMappCur[k] in dicNakalaMetaLeafData:
                                        dicNakalaMetaLeafData[dicMappCur[k]].append(dicSourceCur[k])
                                    else:
                                        dicNakalaMetaLeafData[dicMappCur[k]]=[dicSourceCur[k]]

                        #on peut creer ce .xml
                        #on doit ajouter  le handle nkl collection leaf
                        #pour ca on va chercher le fichier .xml dans le dossier output collection 
                        #le lire pour en extraire le handle
                        #(pour le moment on garde une egalité de nom entre la collection leaf et la data)
                        pathOutputNameXml = pathNklCollecOutputFolder+nameXmlFileCur
                        handle = readReturnNakalaXml(pathOutputNameXml)
                        dicNakalaMetaLeafData.update({'nkl:inCollection':[handle]})
                        writeNakalaMetadataXml(pathInputNameXml, dicoMetaCur, dicNakalaMetaLeafData)
                   

                        
                #on met le xml courant dans un zip si ça n'a pas deja été fait !
                nameZipFileCur = tail+'.zip'
                nameZipFileCur = unidecode(nameZipFileCur) 
                pathInputNameZip = pathNklDataInputFolder+nameZipFileCur
                if not(os.path.isfile(pathInputNameZip)):
                    with zipfile.ZipFile(pathInputNameZip, 'w') as myzip:
                        myzip.write(pathInputNameXml,arcname=nameXmlFileCur)
                        #on ajoute aussi et surtout la data dans le zip 
                        cheminVersLaData = dicSourceCur['file'] 
                        head, tail = os.path.split(dicSourceCur['file']) 
                        nameData = tail
                        myzip.write(cheminVersLaData, arcname=nameData)
                            
                else:
                    #normalement affiche une fois par data
                    print("data deja sur nakala ",nameXmlFileCur)      
                    
    #a partir d'ici on peut disposer d'une liste de .zip de data qui doivent etre envoyées sur nakala
    #on regarde chaque .zip present dans data input et on verifie qu un xml du meme nom existe dans data output 
    #si il existe on supprime le .zip pour eviter un envoi multiple de la meme collection 
    #(la console nakala ne fait pas de controle basé sur les envois précédents) 
    for file in os.listdir(pathNklDataInputFolder):
        if file.endswith(".zip"):
            print(file)
            nameXML = file.replace('.zip', '.xml')
            if os.path.isfile(pathNklDataOutputFolder+nameXML):
                #suppression du .zip dans le dossier input 
                os.remove(pathNklDataInputFolder+file)
                
            else:
                pass
            
    #envoi des .zip presentes dans le dossier data Input
    if len(os.listdir(pathNklDataInputFolder))>0:
        startNakalaConsole(pathJava, pathNklConsoleJar, nameConsoleJar, pathNklDataInputFolder, pathNklDataOutputFolder, pathNklDataErrorFolder, emailNkl, pathPassFileSHA)
        time.sleep(30)
    
    #on regarde si il y a eu des errors
    for file in os.listdir(pathNklCollecErrorFolder):
        if file.endswith(".zip"):
            raise ValueError("error nakala ", file)
        else:
            pass
        
    #bon en bonus ça serait cool de générer un tableur qui contient toutes les métas et les handles (data et collection) 
    #on ouvre encore ue fois le fichier d'entree
    #et le fichier de sortie
    with open(pathFileCSV_Source, encoding='utf-8') as csvfileSource:
        readerSource = csv.DictReader(csvfileSource, delimiter=';')
        
        #on ouvre le fichier Out
        with open(pathFileCSV_Out, 'w', encoding='utf-8') as csvfileOut:
            csvfileOut = csv.writer(csvfileOut, delimiter=';')
            
            listChampDistinctTriees= []
            listChampDistinct = set(dicSourceCur.keys())
            listChampDistinctTriees = sorted(listChampDistinct)
                

            countLine = 0
            for dicSourceCur in readerSource:
                countLine+=1
                #print(len(dicSourceCur))
                #cas particulier pour la premiere ligne
                # va reecrire les noms de column du fichier dentree plus le nom des columns additionnelles
                if countLine==1:
                    rowOut = list(listChampDistinctTriees)
                    rowOut.append('HandleCollecRoot')
                    rowOut.append('HandleCollecLeaf')
                    rowOut.append('HandleDataLeaf')
                    rowOut.append('HandleMetadataLeaf')
                    
                    csvfileOut.writerow(rowOut)
                   
                handleCollecRoot = None
                handleCollecLeaf = None
                handleDataLeaf = None
                
                #on va obtenir les différents handles nakala en lisant les fichiers retournées
                #on test si ya du linkedincollection pour obtenir un eventuel handle collection root
                if len(dicSourceCur['linkedInCollection'])>0:
                    nameXmlFileCur = dicSourceCur['linkedInCollection']+'.xml'
                    nameXmlFileCur = unidecode(nameXmlFileCur) 
                    pathOutputNameXml = pathNklCollecOutputFolder+nameXmlFileCur
                    handleCollecRoot = readReturnNakalaXml(pathOutputNameXml)
                    print('handleCollecRoot ',handleCollecRoot)
                 
                #on recupere le handle de la collec leaf
                if len(dicSourceCur['file'])>0:
                    head, tail = os.path.split(dicSourceCur['file']) 
                    nameXmlFileCur = tail+'.xml'
                    nameXmlFileCur = unidecode(nameXmlFileCur) 
                    pathOutputNameXml = pathNklCollecOutputFolder+nameXmlFileCur
                    handleCollecLeaf = readReturnNakalaXml(pathOutputNameXml)
                    print('handleCollecLeaf ', handleCollecLeaf)
                #on recupere le handle de la data leaf
                if len(dicSourceCur['file'])>0:
                    head, tail = os.path.split(dicSourceCur['file']) 
                    nameXmlFileCurData = tail+'.xml'
                    nameXmlFileCurData = unidecode(nameXmlFileCurData) 
                    pathOutputNameXmlData = pathNklDataOutputFolder+nameXmlFileCurData
                    handleDataLeaf = readReturnNakalaXml(pathOutputNameXmlData)
                    print('handleDataLeaf ', handleDataLeaf)
                    
                    #maintenant qu'on a tout on peut ecrire dans le fichier de sortie
                    rowOut = []
                    for columName in listChampDistinctTriees:
                        rowOut.append(dicSourceCur[columName])
                    if handleCollecRoot == None:
                        rowOut.append(None)
                    else:
                        rowOut.append('https://www.nakala.fr/page/collection/'+handleCollecRoot)
                        
                    rowOut.append('https://www.nakala.fr/page/collection/'+handleCollecLeaf)
                    rowOut.append('hdl.handle.net/'+handleDataLeaf)
                    rowOut.append('https://www.nakala.fr/metadata/'+handleDataLeaf)
                
                    csvfileOut.writerow(rowOut)
                    
    print('-end export to Nakala-')
    
    
def metadataFileCSV2NakalaPushV2_test():
    """
    avec la nouvelle version de nakala console quelques modifications s'imposent !
    """
    
    #pathFileCSV_Source = "./../../Guarnido/03-metadatas/saisie/Guarnido_part1-12_pseudoManuel.csv"
    #pathFileCSV_Out = "./../../Guarnido/03-metadatas/saisie/Guarnido_part1-12_pm_nklHandled.csv"
    #pathFileCSV_Mapp = "./../../Guarnido/03-metadatas/nakala/mappingNakala.csv"
    
    #les infos sur la nkl collection mere
    dicNakalaMetaMother = {}
    dicNakalaMetaMother.update({'nkl:inCollection':['11280/1a40865d']})

    
    #pathJava = "C:\Program Files (x86)\Java\jre1.8.0_111\bin\java\"
    #pathNklConsoleJar = "C:\Users\Public\Documents\MSHS\workflow\code\python\packALA\nakalaConsoleV2\"

    
    #check_JavaAndNakalaConsolePath(pathJava, pathNklConsoleJar)
    
    pathNklCollecInputFolder = "C:/Users/Public/Documents/MSHS/workflow/Guarnido/03-metadatas/nakala/collection/input/"
    pathNklCollecOutputFolder = "C:/Users/Public/Documents/MSHS/workflow/Guarnido/03-metadatas/nakala/collection/output/"

    
        
    pathNklDataInputFolder = "C:/Users/Public/Documents/MSHS/workflow/Guarnido/03-metadatas/nakala/data/input/"
    pathNklDataOutputFolder = "C:/Users\Public/Documents/MSHS/workflow/Guarnido/03-metadatas/nakala/data/output/"
    
    #emailNkl = "michael.nauge01@univ-poitiers.fr"
    #pathPassFileSHA = "./../nakala-console/password_fileMichaelLocal.sha"
    #emailNkl = "fatihaimhand@yahoo.es"
    #pathPassFileSHA = "./../nakala-console/password_fileFatiha.sha"
    
    listColumnRejectedForCollectionRoot = ["linkedInCollection","file","dataFormat","type"]
    listColumnRejectedForCollectionleaf = ["linkedInCollection","file","dataFormat","type"]
    listColumnRejectedForDataleaf = ["linkedInCollection","file"]
        
    pathJava = "C:/Program Files (x86)/Java/jre1.8.0_111/bin/java"

    pathNklConsoleJar = "C:/Users/Public/Documents/MSHS/workflow/code/python/packALA/nakalaConsoleV2/"
    nameConsoleJar = "nakala-console.jar"
    keyApi = "a30fded9-XXXX-XXXX-8f62-e6c0f1f1f241"
    handlePjt = "11280/b28f4a83"
    email = "michael.nauge01@univ-poitiers.fr"
    
    confignkl = objConfigNakalaConsole(pathJava, pathNklConsoleJar,nameConsoleJar, keyApi, handlePjt, email)
    
    pathFileCSV_Source = "C:/Users/Public/Documents/MSHS/workflow/Guarnido/03-metadatas/test_1ressource.csv"
    pathFileCSV_Out = "C:/Users/Public/Documents/MSHS/workflow/Guarnido/03-metadatas/test_1ressource_pushNakala.csv"
    pathFileCSV_Mapp = "C:/Users/Public/Documents/MSHS/workflow/Guarnido/03-metadatas/nakala/mappingNkl.csv"

    metadataFileCSV2NakalaPushV2(confignkl, pathFileCSV_Source, pathFileCSV_Out, pathFileCSV_Mapp, dicNakalaMetaMother, pathNklCollecInputFolder, pathNklCollecOutputFolder, pathNklDataInputFolder, pathNklDataOutputFolder,listColumnRejectedForCollectionRoot, listColumnRejectedForCollectionleaf, listColumnRejectedForDataleaf)   
    
    
def metadataFileCSV2NakalaPush_test():
    """
    fonction de test de remappForXmlNakala
    """
    pathFileCSV_Source = "./../../Guarnido/03-metadatas/saisie/Guarnido_part1-12_pseudoManuel.csv"
    pathFileCSV_Out = "./../../Guarnido/03-metadatas/saisie/Guarnido_part1-12_pm_nklHandled.csv"
    pathFileCSV_Mapp = "./../../Guarnido/03-metadatas/nakala/mappingNakala.csv"
    
    #les infos sur la nkl collection mere
    dicNakalaMetaMother = {}
    dicNakalaMetaMother.update({'nkl:inCollection':['11280/1a40865d']})

    
    pathJava = "C:/Program Files (x86)/Java/jre1.8.0_111/bin/java"
    pathNklConsoleJar = "./../nakala-console/"
    nameConsoleJar = "nakala-console.jar"
    
    #check_JavaAndNakalaConsolePath(pathJava, pathNklConsoleJar)
    
    pathNklCollecInputFolder = "./../../Guarnido/03-metadatas/nakala/collection/input/"
    pathNklCollecOutputFolder = "./../../Guarnido/03-metadatas/nakala/collection/output/"
    pathNklCollecErrorFolder = "./../../Guarnido/03-metadatas/nakala/collection/error/"
    
        
    pathNklDataInputFolder = "./../../Guarnido/03-metadatas/nakala/data/input/"
    pathNklDataOutputFolder = "./../../Guarnido/03-metadatas/nakala/data/output/"
    pathNklDataErrorFolder = "./../../Guarnido/03-metadatas/nakala/data/error/"
    
    emailNkl = "michael.nauge01@univ-poitiers.fr"
    pathPassFileSHA = "./../nakala-console/password_fileMichaelLocal.sha"
    #emailNkl = "fatihaimhand@yahoo.es"
    #pathPassFileSHA = "./../nakala-console/password_fileFatiha.sha"
    
    
    listColumnRejectedForCollectionRoot = ["linkedInCollection","file","dataFormat","type"]
    listColumnRejectedForCollectionleaf = ["linkedInCollection","file","dataFormat","type"]
    listColumnRejectedForDataleaf = ["linkedInCollection","file"]
    
    metadataFileCSV2NakalaPush(pathFileCSV_Source, pathFileCSV_Out, pathFileCSV_Mapp, dicNakalaMetaMother, pathJava, pathNklConsoleJar, nameConsoleJar, pathNklCollecInputFolder, pathNklCollecOutputFolder, pathNklCollecErrorFolder, pathNklDataInputFolder, pathNklDataOutputFolder, pathNklDataErrorFolder, emailNkl, pathPassFileSHA, listColumnRejectedForCollectionRoot, listColumnRejectedForCollectionleaf, listColumnRejectedForDataleaf)   
    
   
def writeNakalaMetadataXml(pathfileOut, dicMeta, dicNakalaMeta):
    """
    fonction permettant de générer un fichier xml de metadata pour nakala
    
    :param pathfileOut: le chemin vers le fichier xml de sortie
    :type pathfileOut: chemin de fichier. 
    
    :param dicMeta: le dictionnaire des champs de meta data a mettre ds le xml. 
    Il est important de noter que pour nakala, 
    il faut au minimum les 4 champs DublinCore title,creator,type,created
    :type dicMeta: dictionnary de chaine de caractere (key nomchamps DC, valeur). 
    
    :param dicNakalaMeta: le dictionnaire des champs meta specifiques nakala
    :type dicNakalaMeta:   dictionnary de chaine de caractere (key nomchamps Nakala, valeur)

    :returns: None
    :raises: IOerror (si probleme  d'ouverture de fichier) 
    ValueError (pas les 4 champs dublin core mini, pas de presence collection nakala)
    """
    
    
    #le fichier xml de description pour nakala est particulier
    #il est conseillé de se référer au document Nakala-Documentation-Console.docx
    #disponible dans le zip : https://www.nakala.fr/nakala
    #mais voici un exemple de fichier valide:
        
        #<?xml version="1.0" encoding="UTF-8" standalone="no"?>
        #<nkl:Data xmlns:nkl="http://nakala.fr/schema#"
        #   xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        #   xmlns:dcterms="http://purl.org/dc/terms/"
        #   xsi:schemaLocation="http://purl.org/dc/terms/ http://dublincore.org/schemas/xmls/qdc/2008/02/11/dcterms.xsd">
    
        #<dcterms:title>Une image (jaune) et ses metas in ZIP via browser et sans valid FACILE</dcterms:title>
        #<dcterms:creator>NUAGE, Mariona</dcterms:creator>
        #<dcterms:type>Image</dcterms:type>
        #<dcterms:created>1933</dcterms:created>
        #<nkl:inCollection>11280/7c8a7a2d</nkl:inCollection>
        #<nkl:dataFormat>JPEG</nkl:dataFormat>
        #</nkl:Data>   
        
    #attention la nkl:collection doit deja exister a distance...
    #il est possible decrire une collection via xml 
    #se referer à la fonction writeNakalaCollectionXml

    
    #ouverture du fichier en ecriture
    with open(pathfileOut, 'w', encoding='utf-8') as f:
        #ecriture des premieres lignes standard pour nakala
        
        f.write('<?xml version="1.0" encoding="UTF-8" standalone="no"?> \n')
        f.write('<nkl:Data xmlns:nkl="http://nakala.fr/schema#"')
        f.write('   xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"')
        f.write('   xmlns:dcterms="http://purl.org/dc/terms/"')
        f.write('   xsi:schemaLocation="http://purl.org/dc/terms/ http://dublincore.org/schemas/xmls/qdc/2008/02/11/dcterms.xsd"> \n')
        
        #ecriture des champs dublincore
        ###################################
        
        #on teste la presence des 4 champs dublin core minimun
        if not( 'dcterms:title' in dicMeta):
            raise ValueError('Manque du champs DublinCore dcterms:title')
            
        if not( 'dcterms:creator' in dicMeta):
            raise ValueError('Manque du champs DublinCore dcterms:creator')
            
        if not( 'dcterms:type' in dicMeta):
            raise ValueError('Manque du champs DublinCore dcterms:type')
            
        if not( 'dcterms:created' in dicMeta):
            dicMeta['dcterms:created'] = [str(date.today().year)+'-'+str(date.today().month)+'-'+str(date.today().day)]
            #on l'ajoute !
            #raise ValueError('Manque du champs DublinCore dcterms:created')
            
        #les champs dublins ont l'air pas trop mal
        #on va les ecrire
        
        #attention cas particulier : il faut commencer par title puis creator  
        #puis type puis created sinon nakala n'est pas content
        #on considere 
        for v in dicMeta['dcterms:title']:
            v = getEncodedSpecialCharForXml(v)
            lineCur = '<dcterms:title>'+v+'</dcterms:title> \n'
            f.write(lineCur)
            
        for v in dicMeta['dcterms:creator']:   
            v = getEncodedSpecialCharForXml(v)
            lineCur = '<dcterms:creator>'+v+'</dcterms:creator> \n'
            f.write(lineCur)
            
        for v in dicMeta['dcterms:type']:
            v = getEncodedSpecialCharForXml(v)
            lineCur = '<dcterms:type>'+v+'</dcterms:type> \n'
            f.write(lineCur)
        
        for v in dicMeta['dcterms:created']:
            v = getEncodedSpecialCharForXml(v)
            lineCur = '<dcterms:created>'+v+'</dcterms:created> \n'
            f.write(lineCur)

        for k in dicMeta:
            #on ne retraite pas 'dcterms:title,creator,type,created', car c'est fait précedement.....
            if not(k == 'dcterms:title'or k == 'dcterms:creator'or k == 'dcterms:type'or k == 'dcterms:created' ):
                for value in dicMeta[k] :        
                    #clairement ca ne sert a rien d'envoyer du none...
                    if not(value == 'none'):
                        #gestion des caractere spéciaux en xml
                        value = getEncodedSpecialCharForXml(value)
                        lineCur = '<'+k+'>'+value+'</'+k+'> \n'
                        f.write(lineCur)
            else:
                #on va gérer pour traiter multiples instances (de title,creator...) sauf la case0 deja traité ...
                count = 0
                for value in dicMeta[k] :        
                    count+=1
                    if count>1:
                        #clairement ca ne sert a rien d'envoyer du none...
                        if not(value == 'none' or value == 'None'):
                            value = getEncodedSpecialCharForXml(value)
                            lineCur = '<'+k+'>'+value+'</'+k+'> \n'
                            f.write(lineCur)
                
        
        #ecriture des champs nakala
        ###################################           
        #on leve ue exception si il n'y a pas la presence des 2 champs nakala minimun nkl:inCollection et nkl:dataFormat
        if not( 'nkl:inCollection' in dicNakalaMeta):
            raise ValueError('Manque du champs nkl:inCollection')
        else:
            for value in dicNakalaMeta['nkl:inCollection']:
                if not(type(value)==type(None)):
                    if not(value == 'none'):
                        value = getEncodedSpecialCharForXml(value)
                        lineCur = '<'+'nkl:inCollection'+'>'+value+'</'+'nkl:inCollection'+'> \n'
                        f.write(lineCur) 
        
        if not('nkl:relation' in dicNakalaMeta):
            pass
        else:
            for value in dicNakalaMeta['nkl:relation']:
                if not(value == 'none'):
                    value = getEncodedSpecialCharForXml(value)
                    lineCur = '<'+'nkl:relation'+'>'+value+'</'+'nkl:relation'+'> \n'
                    f.write(lineCur)                    
        
        if not('nkl:accessEmail' in dicNakalaMeta):
            pass
        else:
            for value in dicNakalaMeta['nkl:accessEmail']:
                if not(value == 'none'):
                    value = getEncodedSpecialCharForXml(value)
                    lineCur = '<'+'nkl:accessEmail'+'>'+value+'</'+'nkl:accessEmail'+'> \n'
                    f.write(lineCur)             
        
        if not( 'nkl:dataFormat' in dicNakalaMeta):
            raise ValueError('Manque du champs nkl:dataFormat')
        else:
            for value in dicNakalaMeta['nkl:dataFormat']:
                if not(value == 'none'):
                    value = getEncodedSpecialCharForXml(value)
                    lineCur = '<'+'nkl:dataFormat'+'>'+value+'</'+'nkl:dataFormat'+'> \n'
                    f.write(lineCur) 
                    
        if not( 'nkl:identifier' in dicNakalaMeta):
            pass
        #pas obligatoire
            #raise ValueError('Manque du champs nkl:identifier')
        else:
            for value in dicNakalaMeta['nkl:identifier']:
                if not(value == 'none'):
                    value = getEncodedSpecialCharForXml(value)
                    lineCur = '<'+'nkl:identifier'+'>'+value+'</'+'nkl:identifier'+'> \n'
                    f.write(lineCur) 

        #####################################
                   
        f.write('</nkl:Data>')
    #fermeture du fichier auto par l'utilisation de with
    

def writeNakalaMetadataXml_test():
    pathfileOut = 'testWritePython.xml'
    dicMeta = {}
    dicMeta.update({'dcterms:title': 'mon petit title'})
    dicMeta.update({'dcterms:creator': 'Nuage, Mariona'})
    dicMeta.update({'dcterms:type': 'Image'})
    dicMeta.update({'dcterms:created': '2014-04-18'})
   
    dicNakalaMeta = {}
    dicNakalaMeta.update({'nkl:dataFormat':'JPEG'})
    dicNakalaMeta.update({'nkl:inCollection':'11280/7c8a7a2d'})
    writeNakalaMetadataXml(pathfileOut,dicMeta,dicNakalaMeta)
  
    
def writeNakalaCollectionXml(pathfileOut, dicMeta, dicNakalaMeta):
    """
    fonction permettant de générer un fichier xml de creation de collection pour nakala
    
    :param pathfileOut: le chemin vers le fichier xml de sortie
    :type pathfileOut: chemin de fichier. 
    
    :param dicMeta: le dictionnaire des champs de meta data a mettre ds le xml. 
    Il est important de noter que pour une nakala collection, 
    il faut au minimum les 1 champs DublinCore title
    :type dicMeta: dictionnary de chaine de caractere (key nomchamps DC, valeur). 
    
    :param dicNakalaMeta: le dictionnaire des champs meta specifiques nakala
    :type dicNakalaMeta:   dictionnary de chaine de caractere (key nomchamps Nakala, valeur)

    :returns: None
    :raises: IOerror (si probleme de d'ouverture de fichier) 
    ValueError (pas le champ dublin core mini)
    """
    
    #le fichier xml de description de collection pour nakala est particulier
    #il est conseillé de se référer au document Nakala-Documentation-Console.docx
    #disponible dans le zip : https://www.nakala.fr/nakala
    #mais voici un exemple de fichier valide:
        
        #<?xml version="1.0" encoding="UTF-8" standalone="no"?>
        #
        #<nkl:Collection xmlns:nkl="http://nakala.fr/schema#"
        #           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        #           xmlns:dcterms="http://purl.org/dc/terms/"
        #           xsi:schemaLocation="http://purl.org/dc/terms/ http://dublincore.org/schemas/xmls/qdc/2008/02/11/dcterms.xsd">
        #    
        #    <dcterms:title>Une collection par console</dcterms:title>
        #    <dcterms:creator>NUAGE, Mariona</dcterms:creator>
        #    <dcterms:created>2016</dcterms:created>
        #    <nkl:inCollection>11280/2b561e65</nkl:inCollection>
        #
        #</nkl:Collection>  
        
       #attention la nkl:collection doit deja exister a distance...
    #il est possible decrire une collection via xml 
    #se referer à la fonction writeNakalaCollectionXml

    #ouverture du fichier en ecriture
    with open(pathfileOut, 'w', encoding='utf-8') as f:
        #ecriture des premieres lignes standard pour nakala
        
        f.write('<?xml version="1.0" encoding="UTF-8" standalone="no"?> \n')
        f.write('<nkl:Collection xmlns:nkl="http://nakala.fr/schema#"')
        f.write('   xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"')
        f.write('   xmlns:dcterms="http://purl.org/dc/terms/"')
        f.write('   xsi:schemaLocation="http://purl.org/dc/terms/ http://dublincore.org/schemas/xmls/qdc/2008/02/11/dcterms.xsd"> \n')
        
        #ecriture des champs dublincore
        ###################################
        #on teste la presence des champs dublin core minimun
        if not( 'dcterms:title' in dicMeta):
            raise ValueError('Manque du champs DublinCore dcterms:title')
            

        #les champs dublins ont l'air pas trop mal
        #on va les ecrire

        #attention cas particulier : il faut commencer par title ! sinon nakala n'est pas content
        #on considere 
        for vtitle in dicMeta['dcterms:title']:
            vtitle = getEncodedSpecialCharForXml(vtitle)
            lineCur = '<dcterms:title>'+vtitle+'</dcterms:title> \n'
            f.write(lineCur)
                    
        for k in dicMeta.keys():

            #on ne retraite pas 'dcterms:title' car c'est fait précedement.....
            if not(k=='dcterms:title'):
            
                #il peut y avoir plusieurs values pour la meme key
                #et il est preconisé d ecrire plusieurs lignes dans ce cas
                for value in dicMeta[k] :        
                    #clairement ca ne sert a rien d'envoyer du none...
                    if not(value == 'none'):
                        #gestion des caractere spéciaux en xml
                        value = getEncodedSpecialCharForXml(value)
                        lineCur = '<'+k+'>'+value+'</'+k+'> \n'
                        f.write(lineCur)
        
        #ecriture des champs nakala
        ###################################           
        #on teste la presence du champs collection mere  nakala minimun et la seule acceptée

        if not( 'nkl:inCollection' in dicNakalaMeta):
            print('[error]')
            print('dicMeta', dicMeta)
            print('dicNakalaMeta', dicNakalaMeta)
            raise ValueError('Manque ddu champs nkl:inCollection')
        else:
            for value in dicNakalaMeta['nkl:inCollection']:
                if not(value == 'none'):
                    value = getEncodedSpecialCharForXml(value)
                    lineCur = '<'+'nkl:inCollection'+'>'+value+'</'+'nkl:inCollection'+'> \n'
                    f.write(lineCur) 
        #####################################         
            
        f.write('</nkl:Collection>')
    #fermeture du fichier auto par l'utilisation de with
    
   

def writeNakalaCollectionXml_test():
    pathfileOut = 'testWriteCollection.xml'
    dicMeta = {}
    dicMeta.update({'dcterms:title': ['ma collection par python']})
    dicMeta.update({'dcterms:creator': ['Nuage, Mariona','Blaise, Pascal']})
    dicMeta.update({'dcterms:created': ['2016-04-18']})
   
    dicNakalaMeta = {}
    #la collection mere
    dicNakalaMeta.update({'nkl:inCollection':['11280/2b561e65']})
    writeNakalaCollectionXml(pathfileOut, dicMeta, dicNakalaMeta)
    
            
def nakalaDataExists(handle):
    """
    Fonction permettant de tester l'existance d'une nkl:data
    par l'envoi d'une requete get sur nakala
    
    :param handle: le handle de la data cible
    :type handle: str
    :returns: true si ça a repondu a distance
    """    
    
    url = "https://www.nakala.fr/nakala/data/"+handle
    #url = "https://www.nakala.fr/page/data/"
    #https://www.nakala.fr/page/collection/11280/7c8a7a2d
    
    url = "https://www.nakala.fr/page/data/"+handle
    try : 
        with urllib.request.urlopen(url) as x:
            html = x.read()
            return True
    except :
        return False
    
    
def getEncodedSpecialCharForXml(value):
    """
    il existe certains caracteres speciaux en xml quil ne faut pas utilisées car reservés par le language.
    on va donc les remplcer par leurs versions codées... I love xml...
    """
    t = Text()
    t.data = value
    return t.toxml()

    
def getEncodedSpecialCharForXml_test():
    a = "N&B"
    
    print(getEncodedSpecialCharForXml(a))
        
def nakalaCollectionExists(handle):
    """
    Fonction permettant de tester l'existance d'une nkl:collection
    par l'envoi d'une requete get sur nakala
    
    :param handle: le handle de la data cible
    :type handle: str
    :returns: true si ça a repondu a distance
    """     
    
    url = "https://www.nakala.fr/page/collection/"+handle
    try : 
        with urllib.request.urlopen(url) as x:
            html = x.read()
            return True
    except :
        return False

    
def nakalaDataExists_test():
    datahandle = "11280/c571bc9d"
    print('nakalaDataExists ',datahandle,' : ', nakalaDataExists(datahandle))
    datahandle = "11280/c571bcff"
    print('nakalaDataExists ',datahandle,' : ', nakalaDataExists(datahandle))
    
def nakalaCollectionExists_test():
    collectionhandle = "11280/2b561e65"
    print('nakalaCollectionExists ',collectionhandle,' : ', nakalaCollectionExists(collectionhandle))    
    
    collectionhandle = "11280/2b561e77"
    print('nakalaCollectionExists ',collectionhandle,' : ', nakalaCollectionExists(collectionhandle))    
    
   
def startNakalaConsole(pathJava, pathNklConsoleJar, nameConsoleJar, pathNklInputFolder, pathNklOutputFolder, pathNklErrorFolder, emailNkl, pathPassFileSHA):
    """
    fonction permettant d'effectuer un envoi par lot via l'appli console nakala-console.jar.
    C'est une application java, il faut donc un interpreter java fonctionnel sur la machine
    Il faut aussi cette fameuse appli console !
    l'appli essai d'envoyer tous les .ZIP présents dans le dossier input spécifié
    l'appli fait un retour pour chaque envoi en remplissant les dossiers output et error
    
    :param pathJava: le chemin vers l'interpreter java (C:/Program Files (x86)/Java/jre1.8.0_111/bin/java)
    :type pathJava: str
    
    :param pathNklConsoleJar : le chemin vers le dossier contenant l'appli nakala-console ("./../nakala-console/")
    :type pathNklConsoleJar: str
    
    :param nameConsoleJar : le nom de l'appli (pouvant varier si humanum fait de nouvelles versions...)
    :type nameConsoleJar:str
    
    :param pathNklInputFolder : le chemin vers le dossier qui recevera les .zip pour l'envoi par lot dans nakala 
    :type pathNklInputFolder: str
    
    :param pathNklOutputFolder : le chemin vers le dossier qui recevera les .xml de retour d'envoi nakala (c'est en lisant les xml dans ce dossier que l'on peut obtenir le handle attribué ou des infos sur l'echec d'envoi)  
    :type pathNklInputFolder: str
    
    :param pathNklErrorFolder : le chemin vers le dossier qui recevera les .zip de retour d'échec d'envoi nakala. si ce répertoire est vide, tous les paquets ont été correctement envoyés. sinon il y a les .zip qui ont coincés..  
    :type pathNklErrorFolder: str
    
    :param emailNkl :  l'adresse email de l'envoyeur
    :type emailNkl: str
    
    :param pathPassFileSHA : le chemin  vers le fichier contenant le mot de pass (crypté en sha1) pour l'envoi nakala
    :type pathPassFileSHA : str
    
    :returns: None
    :raises: ValueError
    """
    
    
    pathAppliSender = pathNklConsoleJar+nameConsoleJar
    #avant de lancer l'appli on vérifie qu'il y a bien des zip a lancer
    if len(glob.glob(pathNklInputFolder+'*.zip'))>0:
        subprocess.check_output([pathJava, '-jar',pathAppliSender, '-email',emailNkl, '-inputFolder', pathNklInputFolder,'-outputFolder',pathNklOutputFolder, '-errorFolder',pathNklErrorFolder, '-passwordFile', pathPassFileSHA],cwd=pathNklConsoleJar) 
    
    
    
    
def startNakalaConsoleV2(pathJava, pathNklConsoleJar, nameConsoleJar, pathNklInputFolder, pathNklOutputFolder, emailNkl, handlePjt, pathNklApiKey, update=False):
    """
    fonction permettant d'effectuer un envoi par lot via l'appli console nakala-console.jar.
    C'est une application java, il faut donc un interpreter java fonctionnel sur la machine
    Il faut aussi cette fameuse appli console !
    l'appli essai d'envoyer tous les .ZIP présents dans le dossier input spécifié
    l'appli fait un retour pour chaque envoi en remplissant les dossiers output et error
    
    :param pathJava: le chemin vers l'interpreter java (C:/Program Files (x86)/Java/jre1.8.0_111/bin/java)
    :type pathJava: str
    
    :param pathNklConsoleJar : le chemin vers le dossier contenant l'appli nakala-console ("./../nakala-console/")
    :type pathNklConsoleJar: str
    
    :param nameConsoleJar : le nom de l'appli (pouvant varier si humanum fait de nouvelles versions...)
    :type nameConsoleJar:str
    
    :param pathNklInputFolder : le chemin vers le dossier qui recevera les .zip pour l'envoi par lot dans nakala 
    :type pathNklInputFolder: str
    
    :param pathNklOutputFolder : le chemin vers le dossier qui recevera les .xml de retour d'envoi nakala (c'est en lisant les xml dans ce dossier que l'on peut obtenir le handle attribué ou des infos sur l'echec d'envoi)  
    :type pathNklInputFolder: str
    
    :param emailNkl :  l'adresse email de l'envoyeur
    :type emailNkl: str
    
    :param handlePjt : le handle Nakala du projet (a obtenir via le backoffice nakala online)
    :type handlePjt : str
    
    :param pathNklApiKey : le chemin  vers le fichier contenant la API key (a generer via le backoffice nakala online)
    :type pathNklApiKey : str
    
    :returns: None
    :raises: ValueError
    """
    
    print("start startNakalaConsoleV2")
    pathAppliSender = pathNklConsoleJar+nameConsoleJar
    #avant de lancer l'appli on vérifie qu'il y a des zip a lancer
    nbZIPFinded = len(glob.glob(pathNklInputFolder+'/*.zip'))
    print("nb ZIP Finded : ",nbZIPFinded)
    
    
    #facileValidation 
    #-cleanOutput 
    #-replace
    #dataReplace
    
    if nbZIPFinded>0:
    
        cmdParams = [pathJava, '-jar', pathAppliSender, '-email', emailNkl, '-inputFolder', pathNklInputFolder,'-outputFolder', pathNklOutputFolder, '-projectId', handlePjt,'-apiKeyFile', pathNklApiKey, '-cleanOutput' ]
        
        if update:
            cmdParams.append('-replace')
        #cmdParams = [pathJava, "-version"]
        #print('cmdParams ',cmdParams)
        
        try:
            res = subprocess.check_output(cmdParams, stderr=subprocess.STDOUT, cwd=pathNklConsoleJar)
            #res = subprocess.check_output(["echo ", "Hello World!"])
            print(res)
            
            return True
            
        except subprocess.CalledProcessError as e:
            print(e.output)
            return False
        
    else:
        print("Nombre de ZIP in ",pathNklInputFolder, nbZIPFinded)
        return False
        
    print("end startNakalaConsoleV2")
    
    
def check_JavaAndNakalaConsolePath(pathJava, pathNklConsoleJar):
    #test java et sa version
    
    res = subprocess.check_output([pathJava,"-version"])
    print('path Java ',pathJava , ' : ', res)
    
    
    res = subprocess.check_output([pathJava, '-jar', pathNklConsoleJar,'-h'])
    print('path NakalaConsole.jar ',pathNklConsoleJar,' : ',res)
   
    
def check_JavaPath(pathJava) : 
    if os.path.isfile(pathJava+'java.exe'):
        return 0
    else: 
        return -1
    
    
def check_NakalaConsolePath(pathNakalaConsole) :
    if os.path.isfile(pathNakalaConsole+'nakala-console.jar'):
        return 0
    else: 
        return -1
    
def check_MetadataPathFile(pathMetadatasFile):
    if os.path.isfile(pathMetadatasFile):
        return 0
    else: 
        return -1
    
    

def startNakalaConsole_test():
    pathJava = "C:/Program Files (x86)/Java/jre1.8.0_111/bin/java"
    pathNklConsoleJar = "./../nakala-console/"
    nameConsoleJar = "nakala-console.jar"
    
    #check_JavaAndNakalaConsolePath(pathJava, pathNklConsoleJar)
    
    pathNklInputFolder = "./../../Guarnido/03-metadatas/nakala/collection/input"
    pathNklOutputFolder = "./../../Guarnido/03-metadatas/nakala/collection/output"
    pathNklErrorFolder = "./../../Guarnido/03-metadatas/nakala/collection/error"
    
    #emailNkl = "michael.nauge01@univ-poitiers.fr"
    #pathPassFileSHA = "./../nakala-console/password_file.sha"
    emailNkl = "fatihaimhand@yahoo.es"
    pathPassFileSHA = "./../nakala-console/password_fileFatiha.sha"
    
    startNakalaConsole(pathJava, pathNklConsoleJar, nameConsoleJar, pathNklInputFolder, pathNklOutputFolder, pathNklErrorFolder, emailNkl, pathPassFileSHA)
    
    
def startNakalaConsoleV2_test():
        
    pathJava = "C:/Program Files (x86)/Java/jre1.8.0_111/bin/java"
    #pathNklConsoleJar = "./nakalaConsoleV2/"
    pathNklConsoleJar = "C:/Users/Public/Documents/MSHS/workflow/code/python/packALA/nakalaConsoleV2/"
    nameConsoleJar = "nakala-console.jar"
    
    #check_JavaAndNakalaConsolePath(pathJava, pathNklConsoleJar+nameConsoleJar)
    
    
    
    #pathNklInputFolder = "./input"
    pathNklInputFolder = "C:/Users/Public/Documents/MSHS/workflow/code/python/packALA/nakalaConsoleV2/input"
    #pathNklOutputFolder = "./output"
    pathNklOutputFolder = "C:/Users/Public/Documents/MSHS/workflow/code/python/packALA/nakalaConsoleV2/output"
    
    
    emailNkl = "michael.nauge01@univ-poitiers.fr"
    #pathHandlePjt = "./../nakala-consoleV2/handleGuarnidopjt.txt"
    handlePjt = "11280/b28f4a83"
    pathNklApiKey = "nakala-key-guarnido.txt"
    #pathNklApiKey = "nakalaConsoleV2/nakala-key-guarnido.txt"
    
    startNakalaConsoleV2(pathJava, pathNklConsoleJar, nameConsoleJar, pathNklInputFolder, pathNklOutputFolder, emailNkl, handlePjt, pathNklApiKey)
    

def writeNakalaApiKeyTextFile(pathNakalaConsole, nameOutpuFile, key):
    file = open(pathNakalaConsole+nameOutpuFile,"w",encoding='utf-8')  
    file.write(key) 
    file.close() 
    

def writeNakalaApiKeyTextFile_test():
    pathNakalaConsole = "C:/Users/Public/Documents/MSHS/workflow/code/python/packALA/nakalaConsoleV2/"
    key = "a30fded9-3b7f-4c99-8f62-e6c0f1f1f241"
    writeNakalaApiKeyTextFile(pathNakalaConsole, key)
    
            
def readReturnNakalaXml(pathFileXmlIn):
    """
    fonction permettant de lire un fichier xml de retour de creation nakala par l'application console java
    
    :param pathFileXmlIn: le chemin vers le fichier xml de sortie generé par nakala
    :type pathFileXmlIn: chemin de fichier. 
    
    :returns: str : le handle (identifier) attribuée pour cet objet nakala
    :raises: IOerror (si probleme de d'ouverture de fichier) 
    """
    readedHandleNakala = None
    try :
        #les retours de paquet nakala sous windows c'est surement de l'encodage AINSI 
        #car quand on precise encoding='utf8' dans open ça pose probleme...
        with open(pathFileXmlIn, 'r') as f:
            xmlcontent = f.read()
            soup = BeautifulSoup(xmlcontent,'html.parser')
            #print(soup)
            if not(soup==None):
                
                listidentifier = soup.findAll('nkl:identifier')
                #bon il y a une difference en les retours Collection et les retours data
                #dans collection il n'y a pas nkl avant identifier...
                #sans doute un bug prochainement corrigé.
                
                if len(listidentifier) == 0:
                    #on test avec l'autre syntaxe
                    listidentifier = soup.findAll('identifier')
                    
                #on verifi qu il y a bien un et
                #un seul identifier handle en retour
                if len(listidentifier) == 1:
                    for idcur in listidentifier:
                        readedHandleNakala = idcur.text
                        #print('handle recup ',readedHandleNakala)
                else:
                    raise ValueError('nakala handle problem...')
                    
    except:
        return None
    return readedHandleNakala


def readReturnNakalaXml_test():
    #test sur collection
    #pathNakalaXml = './../../Guarnido/03-metadatas/nakala/collection/output/JMG-AA1-1942-01-22_2.jpg.xml'
    #pathNakalaXml ='./../../Guarnido/03-metadatas/nakala/data/output/test.xml'
    pathNakalaXml ='./../../Guarnido/03-metadatas/nakala/data/output/test2.xml'
    #pathNakalaXml ='./../../Guarnido/03-metadatas/nakala/data/output/JMG-AA1-1942-01-22_2.jpg.xml'
    res = readReturnNakalaXml(pathNakalaXml)
    print(res)
        
    
    
def scrapNakala():
    #via le end point : 
    #https://www.nakala.fr/oai/11280/b28f4a83?verb=ListRecords&metadataPrefix=oai_dc
    #https://www.nakala.fr/oai/11280/b28f4a83?verb=ListMetadataFormats
    pass
    
def main(argv):
    #writeNakalaMetadataXml_test()
    #writeNakalaCollectionXml_test()
    
    #nakalaDataExists_test()
    #nakalaCollectionExists_test())
    
    #startNakalaConsole_test()
    #readReturnNakalaXml_test()
    
    #metadataFileCSV2NakalaPush_test()
    

    #writeNakalaApiKeyTextFile_test()
    
    #startNakalaConsoleV2_test()
    
    #metadataFileCSV2NakalaPushV2_test()

    #checkMappingFileValues_test()
    #metadataFileCSV2NakalaPushV3_test()
    
    #getEncodedSpecialCharForXml_test()
    pass
    


if __name__ == "__main__":
    main(sys.argv) 