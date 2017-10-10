# -*- coding: utf-8 -*-
"""
Created on Wed Aug 16 10:13:39 2017

@author: Michael Nauge
"""
#unit test for nakalaRestConnector functions

import sys

import nakalaRestConnector as nklCon

from random import choice
from string import ascii_uppercase


#Repository de Fernando  Aínsa  (handle 11280/7c0135eb)
baseUrl='https://www.nakala.fr/nakala/api/v1/'
#baseUrl='http://localhost:1234/'
email='michael.nauge01@univ-poitiers.fr'
keyApi='a30fded9-XXXX-XXXX-8f62-e6c0f1f1f241'
project='11280/7c0135eb'

objConfNkl = nklCon.objConfigNakalaRestApi(baseUrl,email,keyApi,project)
    
def createCollection_test():
    pathCollectionZip = 'tmpCollection.zip'
    idCollectionRes = nklCon.createCollection(objConfNkl, pathCollectionZip)
    print('id collection res', idCollectionRes)
    
    
    
def updateCollection_test():
    handleCollection = '11280/8d974106'
    pathCollectionZip = 'tmpCollectionUpdate.zip'
    nklCon.updateCollection(objConfNkl, handleCollection, pathCollectionZip)
    
    
def main(argv):
    #createCollection_test()
    updateCollection_test()
    pass

if __name__ == "__main__":
    main(sys.argv) 





