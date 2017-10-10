#!/usr/bin/env python
# -*- coding: utf-8 -*-
#python 3 

#author : Nauge Michael (Université de Poitiers)
#description : script permettant de gérer le dépot de données du projet Molina sur nakala


import sys
import csv
import re

import os
from os.path import basename

from shutil import copyfile

import toolsForCSV as toolCSV
import toolsForNakala as toolNakala


#pour outrepasser le nombre de caractere par cellule
csv.field_size_limit(sys.maxsize)


def transformDataFileForMolinaNakalaPush():
    listOutputFileTmp = []
    
    pathDataFile = "C:/Users/Public/Documents/MSHS/workflow/Molina/03-metadatas/nakala/"
    inputFile = pathDataFile+"Molina-AcRevisionGenetic.csv"
    
    fileCorrectionPath =  "Molina_reviseCorrFile.csv"
    correctionFileUrl(inputFile, pathDataFile,fileCorrectionPath)
    inputFile = pathDataFile+fileCorrectionPath
        
    dataV1 = "Molina_omkReadyP1.csv"
    pathDataV1 = pathDataFile+dataV1
    
    
    #suppression des column 'urlEditItem','urlShowItem'
    outputFileTmp = pathDataFile+"Molina_delCol.csv"
    listOutputFileTmp.append(outputFileTmp)
    
    listColumnNames = ['urlEditItem','urlShowItem']
    toolCSV.suppColumn(inputFile, outputFileTmp, listColumnNames)
    
    #ajout des column
    inputFile = outputFileTmp
    outputFileTmp = pathDataFile+"Molina_addCol.csv"
    listOutputFileTmp.append(outputFileTmp)
    
    listColumnNames = ['Linked in collection branch','Linked in collection leaf', 'Nkl dataType','Nkl dataFormat','Nkl hdl root collection','Nkl hdl branch collection','Nkl hdl leaf collection','Nkl hdl leaf data','Nkl statut']
    toolCSV.addColumnToFileWithoutValue(inputFile, outputFileTmp, listColumnNames)
    
    #duplication
    """
    inputFile = outputFileTmp
    outputFileTmp = pathDataFile+"Guarnido_dupCol.csv"
    listOutputFileTmp.append(outputFileTmp)
    
    columnNameIn = 'Date'
    columnNameDup = 'Created'
    toolCSV.duplicateColumn(inputFile, outputFileTmp, columnNameIn, columnNameDup)
    """
    
    #ajout des prefix
    dicPrefix = {}
    
    dicPrefix['Auteur analyse'] = 'Auteur analyse : '
    dicPrefix['Auteur description'] = 'Auteur description : '
    dicPrefix['Auteur révision'] = 'Auteur révision : '
    dicPrefix['Auteur transcription'] = 'Auteur transcription : '
    #dicPrefix['Contexte géographique']
    #dicPrefix['Coverage']
    #dicPrefix['Creator']
    #dicPrefix['Date']
    #dicPrefix['Description']
    dicPrefix['Destinataire'] = 'Destinataire : '
    dicPrefix['Directeur de la publication'] = 'Directeur publication : '
    dicPrefix['Etat général'] = 'Etat général : '
    dicPrefix['Etat génétique'] = 'Etat génétique : '
    dicPrefix['Format'] = 'Pagination et Dimension : '
    #dicPrefix['Language'] =
    dicPrefix["Lieu d'expédition"] = 'Lieu expédition : ' 
    dicPrefix['Localisation'] = 'Localisation du document : '
    dicPrefix['Nature du document'] = "Mode d'agencement : "
    dicPrefix['Notes'] = 'Notes : '
    dicPrefix['Numéro de la publication'] = 'Numéro publication : '
    dicPrefix['Publication'] = 'Date de publication : '
    #dicPrefix['Publisher'] = 
    dicPrefix['Périodicité'] = 'Périodicité : '
    dicPrefix['Collection'] = 'Recueil : '
    #dicPrefix['Relation'] = 
    dicPrefix['Relations Génétiques'] = 'Relations Génétiques : '
    dicPrefix['Autres ressources en relations'] = 'Autres ressources en relations : '
    
    dicPrefix['Représentation'] = 'Représentation : '
    #dicPrefix['Rights'] = 
    #dicPrefix['Source'] = 'Cote du document : '
    dicPrefix['Sous-titre'] = 'Sous-titre : '
    #dicPrefix['Subject'] = 
    dicPrefix['Support'] = 'Support : ' 
    #dicPrefix['Title'] = 
    dicPrefix['Titre de la publication'] = 'Titre publication : '
    dicPrefix['Type'] = 'Genre : ' 
    dicPrefix['Type de publication'] = 'Type de publication : '

    dicPrefix['Lieu de publication'] = 'Lieu de publication : '

    #dicPrefix['Ville'] = 
    #dicPrefix['file'] = 
    #dicPrefix['tag'] = 
    #dicPrefix['Linked in collection branch'] = 
    #dicPrefix['Linked in collection leaf'] = 
    #dicPrefix['Nkl dataType'] = 
    #dicPrefix['Nkl dataFormat'] = 
    #dicPrefix['Nkl hdl root collection'] = 
    #dicPrefix['Nkl hdl root collection'] = 
    #dicPrefix['Nkl hdl branch collection'] = 
    #dicPrefix[' Nkl hdl leaf collection'] = 
    #dicPrefix['Nkl hdl leaf data'] = 
    #dicPrefix['Nkl statut'] = 
    
    inputFile = outputFileTmp
    outputFileTmp = pathDataFile+"Molina_prefixDic.csv"
    
    toolCSV.addPrefixValueFixedFromDic(inputFile,outputFileTmp, dicPrefix)

    #remplir collection root
    inputFile = outputFileTmp
    outputFileTmp = pathDataFile+"Molina_nklroot.csv"
    ColumnName = 'Nkl hdl root collection'
    listOutputFileTmp.append(outputFileTmp)
    

    #le handle de la collection root (a obtenir via le site web https://www.nakala.fr/nakala/#CollectionsPlace:view )
    value = '11280/1a877ef6'
    toolCSV.addValueFixed(inputFile, outputFileTmp, ColumnName, value)


    #remplir la column 'Nkl dataType' (par text ou image)
    inputFile = outputFileTmp
    outputFileTmp = pathDataFile+"Molina_nkldataType.csv"
    listOutputFileTmp.append(outputFileTmp)
    
    columnNameTest = 'Type'
    columnNameOut = 'Nkl dataType'
    contentValue = 'Iconographie'
    suffixValueTrue = 'Image'
    suffixValueFalse = 'Text'
    
    toolCSV.addSuffixValueConditional(inputFile, outputFileTmp, columnNameTest, columnNameOut, contentValue, suffixValueTrue, suffixValueFalse)
    

    #decoupage 1 ligne par fichier
    inputFile = outputFileTmp
    outputFileTmp = pathDataFile+"Molina_splitByFile.csv"
    
    columnName = "file"
    separator = " | "
    columnNameNumbered = "Page"
    
    toolCSV.creatLineFromMultiValuesInColumn(inputFile, outputFileTmp, columnName, separator, columnNameNumbered)  


    #remplir la column 'Nkl dataFormat' (par JPG)
    inputFile = outputFileTmp
    outputFileTmp = pathDataFile+"Molina_nkldataFormat.csv"
    listOutputFileTmp.append(outputFileTmp)
    
    """
    columnNameTest = 'file'
    columnNameOut = 'Nkl dataFormat'
    contentValue = '.jpg'
    suffixValueTrue = 'JPG'
    suffixValueFalse = ''
    
    toolCSV.addSuffixValueConditional(inputFile, outputFileTmp, columnNameTest, columnNameOut, contentValue, suffixValueTrue, suffixValueFalse)
    """
    
    #on a que du JPG (quand on en a pas c'est qu'on a pas encore le scan)
    
    columnNameOut = 'Nkl dataFormat'
    value = 'JPG'
    toolCSV.addValueFixed(inputFile,outputFileTmp,columnNameOut,value)
    
    #remplir les colonnes 'Linked in collection branch','Linked in collection leaf' et 'Title'

    #'Title' nkldata               >>> La rueda de la fortuna | Shelfnum : JMG-AA-0000 | Page : 12 | Content : facsimile   
    #'Linked in collection leaf'   >>> La rueda de la fortuna | Shelfnum : JMG-AA-0000 | Page : 12 
    #'Linked in collection branch' >>> La rueda de la fortuna | Shelfnum : JMG-AA-0000 ou rien si pas de Page
    #---------------------------------------------------------------------------------
    #'Linked in collection branch' >>> La rueda de la fortuna | Shelfnum : JMG-AA-0000
    inputFile = outputFileTmp
    outputFileTmp = pathDataFile+"Molina_nklcollBranch1.csv"
    listOutputFileTmp.append(outputFileTmp)
    
    columnNameIn = 'Title'
    columnNameOut = 'Linked in collection branch'
    appendMode = False
    separator = None
    columnNameTest = 'Page'
    
    toolCSV.duplicateColumnIfOtherColumnIsnotEmpty(inputFile, outputFileTmp, columnNameIn, columnNameOut, appendMode, separator, columnNameTest)
    
    inputFile = outputFileTmp
    outputFileTmp = pathDataFile+"Molina_nklcollBranch2.csv"
    listOutputFileTmp.append(outputFileTmp)
    
    columnNameIn = 'Source'
    columnNameDup = 'Linked in collection branch'
    appendMode = True
    separator = ' | Shelfnum : '
    
    toolCSV.duplicateColumnIfOtherColumnIsnotEmpty(inputFile, outputFileTmp, columnNameIn, columnNameDup, appendMode, separator, columnNameTest)   
    
    #---------------------------------------------------------------------------------    
    #---------------------------------------------------------------------------------
    #'Linked in collection leaf'   >>> La rueda de la fortuna | Shelfnum : JMG-AA-0000 | Page : 12  
    inputFile = outputFileTmp
    outputFileTmp = pathDataFile+"Molina_nklcollLeaf1.csv"
    listOutputFileTmp.append(outputFileTmp)
    
    columnNameIn = 'Title'
    columnNameDup = 'Linked in collection leaf'
    appendMode = False
    separator = None
    toolCSV.duplicateColumn(inputFile, outputFileTmp, columnNameIn, columnNameDup, appendMode, separator)
    
    inputFile = outputFileTmp
    outputFileTmp = pathDataFile+"Molina_nklcollLeaf2.csv"
    listOutputFileTmp.append(outputFileTmp)
    
    columnNameIn = 'Source'
    columnNameDup = 'Linked in collection leaf'
    appendMode = True
    separator = ' | Shelfnum : '
    toolCSV.duplicateColumn(inputFile, outputFileTmp, columnNameIn, columnNameDup, appendMode, separator)   
    
    inputFile = outputFileTmp
    outputFileTmp = pathDataFile+"Molina_nklcollLeaf3.csv"
    listOutputFileTmp.append(outputFileTmp)
    
    columnNameIn = 'Page'
    columnNameDup = 'Linked in collection leaf'
    appendMode = True
    separator = ' | Page : '
    toolCSV.duplicateColumn(inputFile, outputFileTmp, columnNameIn, columnNameDup, appendMode, separator)   
    #---------------------------------------------------------------------------------
    
    #'Title'   >>> La rueda de la fortuna | Shelfnum : JMG-AA-0000 | Page : 12 | Content : facsimile 
    
    inputFile = outputFileTmp
    outputFileTmp = pathDataFile+"Molina_NewTitle2.csv"
    listOutputFileTmp.append(outputFileTmp)
    
    columnNameIn = 'Source'
    columnNameDup = 'Title'
    appendMode = True
    separator = ' | Shelfnum : '
    toolCSV.duplicateColumn(inputFile, outputFileTmp, columnNameIn, columnNameDup, appendMode, separator)   
    
    inputFile = outputFileTmp
    outputFileTmp = pathDataFile+"Molina_NewTitle3.csv"
    listOutputFileTmp.append(outputFileTmp)
    
    columnNameIn = 'Page'
    columnNameDup = 'Title'
    appendMode = True
    separator = ' | Page : '
    toolCSV.duplicateColumn(inputFile, outputFileTmp, columnNameIn, columnNameDup, appendMode, separator)   
    
    inputFile = outputFileTmp
    outputFileTmp = pathDataFile+"Molina_NewTitle4.csv"
    listOutputFileTmp.append(outputFileTmp)
    
    columnNameTest = 'Nkl dataFormat'
    columnNameOut = 'Title'
    contentValue = 'JPG'
    suffixValueTrue = ' | Content : facsimile'
    suffixValueFalse = ''
    
    toolCSV.addSuffixValueConditional(inputFile, outputFileTmp, columnNameTest, columnNameOut, contentValue, suffixValueTrue, suffixValueFalse)
    
    #---------------------------------------------------------------------------------
    #changement de path dans la column file
        
    inputFile = outputFileTmp
    outputFileTmp = pathDataFile+"Molina_LocalPath.csv"
    listOutputFileTmp.append(outputFileTmp)
    columnNameTest = 'file'
    valueTest = 'http://eman-archives.org/hispanique/files/original/'
    valueReplace = 'C:/Users/Public/Documents/MSHS/workflow/Molina/02-numerisation/compress-Jpg/'
    
    toolCSV.replaceValueWithOther(inputFile, outputFileTmp, columnNameTest, valueTest, valueReplace)
    
        
    
    
    
    
     
def renameMolinaFile(inputCsvFile, outputCsvFile):
    """
    change les nom de fichier generés par omeka en nom de fichier intelligible
    """

    countFileFind = 0
    
    writerOut = None
    outFile = None
    with open(inputCsvFile, encoding='utf-8', newline="\n") as dataFile:
        reader = csv.DictReader(dataFile, delimiter=';')
        countLine = 2 #on commence a 2 pour etre cohérent avec les lignes du tableau de donnée quand on l'ouvre sous Excell/libreOffice
        for dicDataCur in reader:
            #on gere l'ouverture et l'ecriture de la permiere ligne du fichier de sortie
            #---------------------------
            if countLine== 2 :
                outFile = open(outputCsvFile, 'w', encoding='utf-8', newline="\n") 
                writerOut = csv.DictWriter(outFile, delimiter=';', fieldnames = reader.fieldnames)
                writerOut.writeheader()
                
            valueCur = dicDataCur['file']

            if not(valueCur==None) and not(valueCur=='') and not(valueCur.lower()=='none'):
                #on creer le nouveau nom 

                newNameFile = dicDataCur['Source']
                
                if dicDataCur['Page'] == None or dicDataCur['Page']=='':
                    pass
                else:
                    newNameFile = newNameFile+"_"+"{0:03}".format(int(dicDataCur['Page']))
                
                (head, tail) = os.path.split(valueCur)
                leFileNameCur = os.path.basename(tail) 
                
                (leFileNameCurSansExtension,extension) = os.path.splitext(leFileNameCur)
                
                #print(leFileNameCurSansExtension)
                dicDataCur['file'] = head+'/'+newNameFile+extension
                
                          
                #si le fichier existe!
                if os.path.isfile(valueCur):
                    countFileFind+=1
                                       
                    try:
                            pathFileCur = valueCur
                            os.rename(pathFileCur, dicDataCur['file'])
                            
                    except:
                        print('pas pu renommer', pathFileCur)
                    
                else:
                    #print('pas trouve',valueCur )
                    pass  
                                
            else:
                pass
            
            writerOut.writerow(dicDataCur)
            countLine+=1
            
            
    outFile.close()    
    print('nb fichiers trouvées a renommer', str(countFileFind))   
    
def suffixPageNumber():
    #maintenant qu'on a un champs Page on peut le prefixer
    dicPrefix = {}
    dicPrefix['Page'] = 'Page : '
    
    pathDataFile = "C:/Users/Public/Documents/MSHS/workflow/Molina/03-metadatas/nakala/"
    dataFilename = "Molina_RenamedFiles.csv"
    inputFile = pathDataFile+dataFilename
    
    outputFileTmp = pathDataFile+"Molina_prefixPage.csv"
    
    toolCSV.addPrefixValueFixedFromDic(inputFile,outputFileTmp, dicPrefix)
    
        
    #fait une copie du last file 
    outputFileFinal = pathDataFile+"Molina_nklReadyPush.csv"
    copyfile(outputFileTmp, outputFileFinal)
 

def renameMolinaFile_test():
    
    pathDataFile = "C:/Users/Public/Documents/MSHS/workflow/Molina/03-metadatas/nakala/"
    inputCsvFile = pathDataFile+"Molina_LocalPath.csv"
    
    outputCsvFile = pathDataFile+"Molina_RenamedFiles.csv"
    
    renameMolinaFile(inputCsvFile, outputCsvFile)

def correctionFileUrl(inputFile,pathOut,filenameOut):
    #il y a des  file du genre
    #http: //eman-archives. org/hispanique/files/original/dceb0a3ff7051393d151a15d2e97bdaf. | jpg |
    #donc des espaces en trop et des | dans le nom d'extension...

    columnNameTest = 'file'
    valueTest = '. | jpg'
    valueReplace = '.jpg'
    pathdatafileTmp = pathOut+'MolinaTmp1.csv'
    toolCSV.replaceValueWithOther(inputFile, pathdatafileTmp, columnNameTest, valueTest, valueReplace)
    
    columnNameTest = 'file'
    valueTest = ' '
    valueReplace = ''
    pathdatafileTmp2 = pathOut+'MolinaTmp2.csv'
    toolCSV.replaceValueWithOther(pathdatafileTmp, pathdatafileTmp2, columnNameTest, valueTest, valueReplace)
    
    columnNameTest = 'file'
    valueTest = '|'
    valueReplace = ' | '
    pathdatafileTmp3 = pathOut+filenameOut
    toolCSV.replaceValueWithOther(pathdatafileTmp2, pathdatafileTmp3, columnNameTest, valueTest, valueReplace)
    

def pushMolina():
    """
    fonction permettant d'envoyer les données de Guarnido sur Nakala V.2
    """
    
    pathNklCollecInputFolder = "C:/Users/Public/Documents/MSHS/workflow/Molina/03-metadatas/nakala/collection/input/"
    pathNklCollecOutputFolder = "C:/Users/Public/Documents/MSHS/workflow/Molina/03-metadatas/nakala/collection/output/"
  
    pathNklDataInputFolder = "C:/Users/Public/Documents/MSHS/workflow/Molina/03-metadatas/nakala/data/input/"
    pathNklDataOutputFolder = "C:/Users\Public/Documents/MSHS/workflow/Molina/03-metadatas/nakala/data/output/"
    
    listColumnRejectedForCollectionBranch = ["Linked in collection leaf","Nkl dataFormat","Nkl dataType","file"]
    listColumnRejectedForCollectionleaf = ["Linked in collection branch","Nkl dataFormat","Nkl dataType","file"]
    listColumnRejectedForDataleaf = ["file"]
        
    pathJava = "C:/Program Files (x86)/Java/jre1.8.0_111/bin/java"

    pathNklConsoleJar = "C:/Users/Public/Documents/MSHS/workflow/code/python/packALA/nakala-console/"
    nameConsoleJar = "nakala-console.jar"
    keyApi = "a30fded9-XXXX-XXXX-8f62-e6c0f1f1f241"
    #attention le handle du projet n'est pas la meme chose que le handle de la collection root!
    handlePjt = "11280/1cea02b6"
    email = "michael.nauge01@univ-poitiers.fr"
    
    confignkl = toolNakala.objConfigNakalaConsole(pathJava, pathNklConsoleJar, nameConsoleJar, keyApi, handlePjt, email)
    
    pathDataFile = "C:/Users/Public/Documents/MSHS/workflow/Molina/03-metadatas/nakala/"
    dataFilename = "Molina_nklReadyPush.csv"
    pathDataFile = pathDataFile+dataFilename
    
    pathOutFile = "C:/Users/Public/Documents/MSHS/workflow/Molina/03-metadatas/nakala/"
    outFilename = "Molina_NklHandled.csv"
    pathOutFile = pathOutFile+outFilename
    
    pathMappingFile = "C:/Users/Public/Documents/MSHS/workflow/Molina/03-metadatas/nakala/"
    mappingFilename = "mappingNkl.csv"
    pathMappingFile = pathMappingFile+mappingFilename
    
    updateMetas = False
    updateDatas = False
    
    resPushUpdate = toolNakala.metadataFileCSV2NakalaPushV3(confignkl, pathDataFile, pathMappingFile, pathOutFile, pathNklCollecInputFolder, pathNklCollecOutputFolder, pathNklDataInputFolder, pathNklDataOutputFolder, listColumnRejectedForCollectionBranch, listColumnRejectedForCollectionleaf, listColumnRejectedForDataleaf, updateMetas, updateDatas)   
    
    if resPushUpdate:
        print('Tout est PUSH et MAJ !')
    else:
        print('[error] : il y a eu des soucis dans le push et MAJ')
    

    
def main(argv):
    
    """
    transformDataFileForMolinaNakalaPush()
    
    renameMolinaFile_test()
    
    suffixPageNumber()
    """
    
    #Attention j'ai fait des modifications manuelles sur le fichichier readypush en utilisant readyforpushtmp
    #pour ajouter les chemins fichiers audios 
    #et corriger les type et format
    
    #--------------------------------------------
    
    pushMolina()
    #pass




if __name__ == "__main__":
    main(sys.argv) 