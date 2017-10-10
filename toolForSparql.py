#!/usr/bin/env python
# -*- coding: utf-8 -*-
#python 3 

#author : Nauge Michael (Université de Poitiers)
#description : script contenant plusieurs fonctions permettant de manipuler des données dans nakala via le endpoint sparql

#pour faire des requetes get sur le server nakala
import urllib.request

#pour parser des input xml ou html
from bs4 import BeautifulSoup

#pour l'encodage en html et xml
from xml.dom.minidom import Text, Element

import sys


def nakalaCollectionExistByHandle(handleCollectionRoot, handleCollectionSearch):
    """Cette fonction permet de tester l'existance d'une collection nakala par son handle
    si il existe une collection portant ce handle la fonction retourne True
    si il n'existe pas de collection portant ce handle fonction retourne False

    Concretement, cette fonction effectue une requeque GET Sparql sur un endpoint spaqrl nakala
    
    :param handleCollectionRoot: le handle de collection racine du projet
    :type hdlCollection: str. 
    
    :param handleCollectionSearch: le handle de collection à rechercher
    :type handleCollectionSearch: str.
    
    :returns: str contenant le handle ou None
    :raises: urllib.URLError, BeautifulSoup.AttributeError
    """

    """
    exemple de requete
    
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX ore: <http://www.openarchives.org/ore/terms/>
    
    SELECT distinct ?collection
    WHERE {
    ?collection dcterms:identifier '11280/99618bd0'.
    ?collection ore:isAggregatedBy+ <http://www.nakala.fr/collection/11280/1a40865d>.
    }
    
    """
    
    url = "https://www.nakala.fr/sparql/?default-graph-uri=&query="
    url += "PREFIX+foaf:<http://xmlns.com/foaf/0.1/>%0D%0A"
    url += "PREFIX+dcterms:<http://purl.org/dc/terms/>%0D%0A"
    url += "PREFIX+rdfs:<http://www.w3.org/2000/01/rdf-schema%23>%0D%0A"
    url += "PREFIX+skos:<http://www.w3.org/2004/02/skos/core%23>%0D%0A"
    url += "PREFIX+ore:<http://www.openarchives.org/ore/terms/>%0D%0A"
    url += "SELECT+distinct+?idHdl%0D%0A"
    url += "WHERE+{%0D%0A"
    url += "?collection+dcterms%3Aidentifier+%27"+handleCollectionSearch+"%27.%0D%0A"
    url += "?collection+ore%3AisAggregatedBy%2B+%3Chttp%3A%2F%2Fwww.nakala.fr%2Fcollection%2F"+handleCollectionRoot+"%3E.+%0D%0A"
    url += "?collection+dcterms%3Aidentifier+%3FidHdl.%0D%0A"
    url += "}"
    url += "&format=application%2Fsparql-results%2Bxml&timeout=0&debug=on"
    
    #print(url)
    
    
    with urllib.request.urlopen(url) as x:
    
        html = x.read()
        soup = BeautifulSoup(html,'html.parser')
    
        if not(soup==None):
            #print(soup)
            listitem = soup.findAll("binding",attrs={"name":"idHdl"})
            
            if not(listitem == None):
                for item in listitem:
                    #print(item)
                    if not(item==None):
                        #print(item)
                        souplette = BeautifulSoup(str(item),'html.parser')
                        #print(souplette)
                        Handle = souplette.find("literal").text
                        print(Handle)
                        
                        return True
                    else:
                        return False
                            
            else:
                return False
            
        else:
            return False            
    
    return False


def nakalaCollectionExistByHandle_test():
    #ressource Existante :
    hdlSearch = "11280/99618bd0"
    handleCollectionRoot = "11280/1a40865d"

    res = nakalaCollectionExistByHandle(handleCollectionRoot,hdlSearch)

    if res==False:
        print("il n'y a pas de collection nakala portant ce handle", hdlSearch)
    else:
        print("il existe une collection nakala portant ce handle ", hdlSearch )


    #ressource Inexistancte :
    hdlSearch = "unFauxHandle"
    handleCollectionRoot = "11280/1a40865d"

    res = nakalaCollectionExistByHandle(handleCollectionRoot,hdlSearch)
    
    if res==False:
        print("il n'y a pas de collection nakala portant ce handle", hdlSearch)
    else:
        print("il existe une collection nakala portant ce handle ", hdlSearch )


def nakalaDataExistByHandle(handleCollectionRoot, handleDataSearch):
    """Cette fonction permet de tester l'existance d'une data nakala par son handle
    si il existe une data portant ce handle la fonction retourne True
    si il n'existe pas de data portant ce handle fonction retourne False

    Concretement, cette fonction effectue une requeque GET Sparql sur un endpoint spaqrl nakala
    
    :param handleCollectionRoot: le handle de collection racine du projet
    :type hdlCollection: str. 
    
    :param handleDataSearch: le handle de la data à rechercher
    :type handleDataSearch: str.
    
    :returns: str contenant le handle ou None
    :raises: urllib.URLError, BeautifulSoup.AttributeError
    
    """
    
    """
    exemple de requete
    
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX ore: <http://www.openarchives.org/ore/terms/>
    
    SELECT distinct ?handle
    WHERE {
    ?doc ore:isAggregatedBy+ <http://www.nakala.fr/collection/11280/1a40865d>.
    ?doc foaf:primaryTopic ?data.
    ?doc dcterms:identifier ?handle.
    ?doc dcterms:identifier '11280/750ef980'.
    ?doc a foaf:Document.
    }
    """
    
    #requeteTest = "https://www.nakala.fr/sparql/?default-graph-uri=&query=PREFIX+foaf%3A+%3Chttp%3A%2F%2Fxmlns.com%2Ffoaf%2F0.1%2F%3E%0D%0APREFIX+dcterms%3A+%3Chttp%3A%2F%2Fpurl.org%2Fdc%2Fterms%2F%3E%0D%0APREFIX+rdfs%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2000%2F01%2Frdf-schema%23%3E%0D%0APREFIX+skos%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2004%2F02%2Fskos%2Fcore%23%3E%0D%0APREFIX+ore%3A+%3Chttp%3A%2F%2Fwww.openarchives.org%2Fore%2Fterms%2F%3E%0D%0A++++%0D%0ASELECT+distinct+%3Fhandle%0D%0AWHERE+%7B%0D%0A%3Fdoc+ore%3AisAggregatedBy%2B+%3Chttp%3A%2F%2Fwww.nakala.fr%2Fcollection%2F11280%2F1a40865d%3E.%0D%0A%3Fdoc+foaf%3AprimaryTopic+%3Fdata.%0D%0A%3Fdoc+dcterms%3Aidentifier+%3Fhandle.%0D%0A%3Fdoc+dcterms%3Aidentifier+%2711280%2F750ef980%27.%0D%0A%3Fdoc+a+foaf%3ADocument.%0D%0A%7D&format=text%2Fhtml&timeout=0&debug=on"
    
    url = "https://www.nakala.fr/sparql/?default-graph-uri=&query="
    url += "PREFIX+foaf:<http://xmlns.com/foaf/0.1/>%0D%0A"
    url += "PREFIX+dcterms:<http://purl.org/dc/terms/>%0D%0A"
    url += "PREFIX+rdfs:<http://www.w3.org/2000/01/rdf-schema%23>%0D%0A"
    url += "PREFIX+skos:<http://www.w3.org/2004/02/skos/core%23>%0D%0A"
    url += "PREFIX+ore:<http://www.openarchives.org/ore/terms/>%0D%0A"
    url += "SELECT+distinct+%3FidHdl%0D%0AWHERE+%7B%0D%0A%3Fdoc+ore%3AisAggregatedBy%2B+%3Chttp%3A%2F%2Fwww.nakala.fr%2Fcollection%2F11280%2F1a40865d%3E.%0D%0A%3Fdoc+foaf%3AprimaryTopic+%3Fdata.%0D%0A%3Fdoc+dcterms%3Aidentifier+%3FidHdl.%0D%0A%3Fdoc+dcterms%3Aidentifier+%2711280%2F750ef980%27.%0D%0A%3Fdoc+a+foaf%3ADocument.%0D%0A%7D"
    url += "&format=application%2Fsparql-results%2Bxml&timeout=0&debug=on"
    
    #print(url)
    
    with urllib.request.urlopen(url) as x:
    
        html = x.read()
        soup = BeautifulSoup(html,'html.parser')
    
        if not(soup==None):
            #print(soup)
            listitem = soup.findAll("binding",attrs={"name":"idHdl"})
            
            if not(listitem == None):
                for item in listitem:
                    #print(item)
                    if not(item==None):
                        #print(item)
                        souplette = BeautifulSoup(str(item),'html.parser')
                        #print(souplette)
                        Handle = souplette.find("literal").text
                        print(Handle)
                        
                        return True
                    else:
                        return False
                            
            else:
                return False
            
        else:
            return False            
    
    return False    

def nakalaDataExistByHandle_test():
    #ressource Existante :
    hdlSearch = "11280/80d6548d"
    handleCollectionRoot = "11280/1a40865d"

    res = nakalaDataExistByHandle(handleCollectionRoot,hdlSearch)

    if res==False:
        print("il n'y a pas de data nakala portant ce handle", hdlSearch)
    else:
        print("il existe une data nakala portant ce handle ", hdlSearch )


    #ressource Inexistancte :
    hdlSearch = "unFauxHandle"
    handleCollectionRoot = "11280/1a40865d"

    res = nakalaDataExistByHandle(handleCollectionRoot,hdlSearch)
    
    if res==False:
        print("il n'y a pas de data nakala portant ce handle", hdlSearch)
    else:
        print("il existe une data nakala portant ce handle ", hdlSearch) 
        

def nakalaCollectionExistByTitle(handleCollectionRoot, title):
    """Cette fonction permet d'obtenir le handle d'une collection via son attribut title
    si il existe une collection portant ce titre dans le champs dcterms title le handle est renvoyé.
    si il n'existe pas de collection portant ce titre la valeur None est renvoyé

    Concretement, cette fonction effectue une requeque GET Sparql sur un endpoint spaqrl nakala
    
    :param handleCollectionRoot: le handle de collection racine du projet
    :type hdlCollection: str. 
    
    :param title: le titre de la collection recherché
    :type title: str. 
    
    :returns: str contenant le handle ou None
    :raises: urllib.URLError, BeautifulSoup.AttributeError
    """

    """
    exemple de requete
    
    
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX ore: <http://www.openarchives.org/ore/terms/>
    
    SELECT distinct ?collection
    WHERE {
    ?collection dcterms:title 'JMG-AA1-1924-03-00a_1'.
    ?collection ore:isAggregatedBy+ <http://www.nakala.fr/collection/11280/1a40865d>.
    }
    
    """
    """
    pour la requete Get il faut remplacer les espaces par des +
    When data that has been entered into HTML forms is submitted, 
    the form field names and values are encoded and sent to the server in an HTTP request message using method GET or POST, 
    or, historically, via email. 
    The encoding used by default is based on a very early version of the general URI percent-encoding rules,
    with a number of modifications such as newline normalization and replacing spaces with "+" 
    instead of "%20". 
    The MIME type of data encoded this way is application/x-www-form-urlencoded, and it is currently defined (still in a very outdated manner) in the HTML and XForms specifications.
    """
                                                                                                                                

    #title=title.replace(' ','+')--> utilisation de urllib.parse.urlencode

    
    
    """
    url = "https://www.nakala.fr/sparql/?default-graph-uri=&query="
    url += "PREFIX+foaf:<http://xmlns.com/foaf/0.1/>%0D%0A"
    url += "PREFIX+dcterms:<http://purl.org/dc/terms/>%0D%0A"
    url += "PREFIX+rdfs:<http://www.w3.org/2000/01/rdf-schema%23>%0D%0A"
    url += "PREFIX+skos:<http://www.w3.org/2004/02/skos/core%23>%0D%0A"
    url += "PREFIX+ore:<http://www.openarchives.org/ore/terms/>%0D%0A"
    url += "SELECT+distinct+?idHdl%0D%0A"
    url += "WHERE+{%0D%0A"
    url += "?collection+dcterms%3Atitle+%27"+title+"%27.%0D%0A"
    url += "?collection+ore%3AisAggregatedBy%2B+%3Chttp%3A%2F%2Fwww.nakala.fr%2Fcollection%2F"+handleCollectionRoot+"%3E.+%0D%0A"
    url += "?collection+dcterms%3Aidentifier+%3FidHdl.%0D%0A"
    url += "}"
    url += "&format=application%2Fsparql-results%2Bxml&timeout=0&debug=on"
    
    """

    #gestion des quote double quote dans les requetes
    title = title.replace('"','\\"').replace("'","\\'")
    
    requet = ""
    requet += "PREFIX foaf: <http://xmlns.com/foaf/0.1/>"
    requet += "PREFIX dcterms: <http://purl.org/dc/terms/>"
    requet += "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
    requet += "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
    requet += "PREFIX ore: <http://www.openarchives.org/ore/terms/>"
    
    requet += "SELECT distinct ?idHdl "
    requet += "WHERE {"
    requet += "?collection dcterms:title '"+title+"'."
    requet += "?collection ore:isAggregatedBy+ "+"<http://www.nakala.fr/collection/"+handleCollectionRoot+">."
    requet += "?collection dcterms:identifier ?idHdl."
    requet += "}"
    
                      
    params = {}
    params['query']= requet

    
    suffix = "&format=application/sparql-results+xml&timeout=0&debug=on"
    
    url = "https://www.nakala.fr/sparql/?default-graph-uri=&"+urllib.parse.urlencode(params)+suffix
                                                                                    
    #print(url)

    
    with urllib.request.urlopen(url) as x:
    
        html = x.read()
        soup = BeautifulSoup(html,'html.parser')
    
        if not(soup==None):
            #print(soup)
            listitem = soup.findAll("binding",attrs={"name":"idHdl"})
            
            if not(listitem == None):
                for item in listitem:
                    #print(item)
                    if not(item==None):
                        #print(item)
                        souplette = BeautifulSoup(str(item),'html.parser')
                        #print(souplette)
                        Handle = souplette.find("literal").text
                        #print(Handle)
                        return Handle
                    else:
                        return None
                            
            else:
                return None
            
        else:
            return None            
    
    return None




def nakalaCollectionExistByTitle_test():
    
    #ressource Existancte :
    title = "JMG-AA1-1929-06-00_2"
    handleCollectionRoot = "11280/1a40865d"

    res = nakalaCollectionExistByTitle(handleCollectionRoot,title)

    if res==None:
        print("il n'y a pas de collection nakala portant ce titre", title)
    else:
        print("il existe une collection nakala portant le titre ", title, " son handle est : ",res )

    #ressource Inexistante :
    
    title = "collection MORA GUARNIDO José"

    handleCollectionRoot = "11280/1a40865d"

    res = nakalaCollectionExistByTitle(handleCollectionRoot,title)

    if res==None:
        print("il n'y a pas de collection nakala portant ce titre : ", title)
    else:
        print("il existe une collection nakala portant le titre ", title, " son handle est : ",res )



def nakalaDataExistByTitle(handleCollectionRoot, title):
    """Cette fonction permet d'obtenir le handle d'une data via son attribut title
    si il existe une data portant ce titre dans le champs dcterms title le handle est renvoyé.
    si il n'existe pas de data portant ce titre la valeur None est renvoyé

    Concretement, cette fonction effectue une requeque GET Sparql sur un endpoint spaqrl nakala
    
    :param handleCollectionRoot: le handle de collection racine du projet
    :type hdlCollection: str. 
    
    :param title: le titre de la data recherché
    :type title: str. 
    
    :returns: str contenant le handle ou None
    :raises: urllib.URLError, BeautifulSoup.AttributeError
    """

    """
    exemple de requete
    
    
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX ore: <http://www.openarchives.org/ore/terms/>
    
    SELECT distinct ?idHdl
    WHERE {
    ?doc ore:isAggregatedBy+ <http://www.nakala.fr/collection/11280/1a40865d>.
    ?doc foaf:primaryTopic ?data.
    ?doc dcterms:identifier ?idHdl.
    ?data dcterms:title 'JMG-AA1-1924-01-00b_1'.
    ?data a foaf:Document.
    }
    
    """
    
    #urlTmp = "https://www.nakala.fr/sparql/?default-graph-uri=&query=PREFIX+foaf%3A+%3Chttp%3A%2F%2Fxmlns.com%2Ffoaf%2F0.1%2F%3E%0D%0APREFIX+dcterms%3A+%3Chttp%3A%2F%2Fpurl.org%2Fdc%2Fterms%2F%3E%0D%0APREFIX+rdfs%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2000%2F01%2Frdf-schema%23%3E%0D%0APREFIX+skos%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2004%2F02%2Fskos%2Fcore%23%3E%0D%0APREFIX+ore%3A+%3Chttp%3A%2F%2Fwww.openarchives.org%2Fore%2Fterms%2F%3E%0D%0A++++%0D%0ASELECT+distinct+%3FidHdl%0D%0AWHERE+%7B%0D%0A%3Fdoc+ore%3AisAggregatedBy%2B+%3Chttp%3A%2F%2Fwww.nakala.fr%2Fcollection%2F11280%2F1a40865d%3E.%0D%0A%3Fdoc+foaf%3AprimaryTopic+%3Fdata.%0D%0A%3Fdoc+dcterms%3Aidentifier+%3FidHdl.%0D%0A%3Fdata+dcterms%3Atitle+%27JMG-AA1-1924-01-00b_1%27.%0D%0A%3Fdata+a+foaf%3ADocument.%0D%0A%7D&format=application%2Fsparql-results%2Bxml&timeout=0&debug=on"
    
    """
    url = "https://www.nakala.fr/sparql/?default-graph-uri=&query="
    url += "PREFIX+foaf:<http://xmlns.com/foaf/0.1/>%0D%0A"
    url += "PREFIX+dcterms:<http://purl.org/dc/terms/>%0D%0A"
    url += "PREFIX+rdfs:<http://www.w3.org/2000/01/rdf-schema%23>%0D%0A"
    url += "PREFIX+skos:<http://www.w3.org/2004/02/skos/core%23>%0D%0A"
    url += "PREFIX+ore:<http://www.openarchives.org/ore/terms/>%0D%0A"
    url += "SELECT+distinct+%3FidHdl%0D%0A"
    url += "WHERE+%7B%0D%0A"
    url += "?doc+ore:isAggregatedBy%2B+%3Chttp%3A%2F%2Fwww.nakala.fr%2Fcollection%2F"+handleCollectionRoot+"%3E.%0D%0A"
    url += "?doc+foaf:primaryTopic+?data.%0D%0A"
    url += "?doc+dcterms:identifier+?idHdl.%0D%0A"
    url += "?data+dcterms:title+%27"+title+"%27.%0D%0A"
    url += "?data+a+foaf:Document.%0D%0A%7D"
    url += "&format=application%2Fsparql-results%2Bxml&timeout=0&debug=on"
    
    #url = urlTmp
    #print(url)
    
    """

    #gestion des quote double quote dans les requetes
    title = title.replace('"','\\"').replace("'","\\'")
    
    requet = ""
    requet += "PREFIX foaf: <http://xmlns.com/foaf/0.1/>"
    requet += "PREFIX dcterms: <http://purl.org/dc/terms/>"
    requet += "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
    requet += "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
    requet += "PREFIX ore: <http://www.openarchives.org/ore/terms/>"
    
    requet += "SELECT distinct ?idHdl "
    requet += "WHERE {"
    requet += "?doc ore:isAggregatedBy+ "+"<http://www.nakala.fr/collection/"+handleCollectionRoot+">."
    requet += "?doc foaf:primaryTopic ?data."
    requet += "?doc dcterms:identifier ?idHdl."
    requet += "?data dcterms:title '"+title+"'."
    requet += "?data a foaf:Document."
    requet += "}"
    
                      
    params = {}
    params['query']= requet

    
    suffix = "&format=application/sparql-results+xml&timeout=0&debug=on"
    
    url = "https://www.nakala.fr/sparql/?default-graph-uri=&"+urllib.parse.urlencode(params)+suffix
    #print(url)
    
    with urllib.request.urlopen(url) as x:
    
        html = x.read()
        soup = BeautifulSoup(html,'html.parser')
    
        if not(soup==None):
            #print(soup)
            listitem = soup.findAll("binding",attrs={"name":"idHdl"})
            
            if not(listitem == None):
                for item in listitem:
                    #print(item)
                    if not(item==None):
                        #print(item)
                        souplette = BeautifulSoup(str(item),'html.parser')
                        #print(souplette)
                        Handle = souplette.find("literal").text
                        #print(Handle)
                        return Handle
                    else:
                        return None
                            
            else:
                return None
            
        else:
            return None            
    
    return None

def nakalaDataExistByTitle_test():
    
    #ressource Existancte :
    title = '[Décor de théâtre pour marionnettes - "La niña que riega la albahaca y el Príncipe preguntón\\" - Federico García Lorca [6]] | Shelfnum : JMG-DC-302 | Content : facsimile'

    handleCollectionRoot = "11280/1a40865d"

    res = nakalaDataExistByTitle(handleCollectionRoot,title)

    if res==None:
        print("il n'y a pas de data nakala portant ce titre", title)
    else:
        print("il existe une data nakala portant le titre ", title, " son handle est : ",res )




def main(argv):
    
    
    #nakalaCollectionExistByTitle_test()
    #nakalaCollectionExistByHandle_test()
    
    #nakalaDataExistByHandle_test()
    nakalaDataExistByTitle_test()
    

    


if __name__ == "__main__":
    main(sys.argv) 



