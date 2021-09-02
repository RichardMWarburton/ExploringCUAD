# -*- coding: utf-8 -*-
import requests
import os
from zipfile import ZipFile
import json
from collections import defaultdict
import re
import string
import pandas as pd
import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin
import nltk
from nltk.corpus import stopwords
nltk.download('stopwords')



def download_url(url, save_path, chunk_size=128):
    """
    Function to download zip file from web
    ref: https://stackoverflow.com/questions/9419162/download-returned-zip-file-from-url
    
    """
    r = requests.get(url, stream=True)
    with open(save_path, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)
            

def extract_zip(pth,data_pth = None):
    """Function to extract contents of a zip file to a specified location (wd if data_pth not passed)"""
    with ZipFile(pth, 'r') as zipObj:
       # Extract all the contents of zip file in different directory
       zipObj.extractall(data_pth)
            
            
class DownloadCUAD():
    """
    Class Definition to download and process CUAD data
    """
    
    def __init__(self):
        self.download_pth = 'https://github.com/TheAtticusProject/cuad/archive/refs/heads/main.zip'
    
        
    def setup(self):
        """Function to download main CUAD repository"""
        
     
        if not os.path.isdir('cuad-main'):
        
            #UI initation confirmation
            print('CUAD data downloading...')
            
            #Download repo from GitHub            
            download_url(url = self.download_pth,
                         save_path = os.getcwd()+'\\CUAD-main.zip')
                  
            #Extract Files
            extract_zip('CUAD-main.zip',os.getcwd())
            extract_zip('cuad-main/data.zip','cuad-main/data/')        
            
            #Remove CUAD-main.zip (repo) file
            os.remove('CUAD-main.zip')
            
            #UI completion confirmation
            print('CUAD data downloaded & extracted!')
        else:
            print('CUAD data already downloaded & extracted!')
            


    def extract_contracts(self, return_df = False):
    
        #Load CUADv1 JSON to contract_data
        with open('cuad-main/data/CUADv1.json','r') as infile:
            for line in infile:
                contract_data = json.loads(line)
        
        #ser reg ex expression for characters to remove from contract contest
        spec_chars = '\\n|\\t|\\t'
        
        #Set number of contracts in data
        num_contracts = len(contract_data['data'])
        
        #Initate dictionary to store raw contract data
        raw_contracts = defaultdict(list)
        
        #for each contract
        for i in range(num_contracts):
          #Append the title, contract text and character length of text to the raw_contracts dictionary
          raw_contracts['contract title'].append(contract_data['data'][i]['title'])
          #raw_contracts['label'].append(labels_LU[contract_data['data'][i]['title']] if contract_data['data'][i]['title'] in labels_LU else 'marketing agreement' ) #<- manual error trap applied here (see below)
          
          #Parse raw text and process to remove breaks
          raw_text = contract_data['data'][i]['paragraphs'][0]['context']
          clean_text = re.sub(spec_chars,'',raw_text)
        
          #Split clean text in to sentances and tokens
          sentance_text = clean_text.split(sep = '. ')
          token_text = clean_text.split(sep = ' ')
        
          #Append text to the respective key in the raw_contracts dictionary
          raw_contracts['raw text'].append(raw_text)
          raw_contracts['clean text'].append(clean_text)
          raw_contracts['sentance text'].append(sentance_text)
          raw_contracts['token text'].append(token_text)
          
          #Add character, sentance and token counts to raw_contracts dictionary
          raw_contracts['character count'].append(len(raw_text))
          raw_contracts['sentance count'].append(len(sentance_text))
          raw_contracts['token count'].append(len(token_text))

        #Define the number of clauses (NEED TO REMOVE THIS!!!)
        num_clauses = 41
        
        #initate dictioanry to store caluse data
        clause_data = defaultdict(list)
        
        #For each contract
        for i in range(num_contracts):
          #for each clause
          for j in range(num_clauses):
            #for each found clause annotation
            for k in range(len(contract_data['data'][i]['paragraphs'][0]['qas'][j]['answers'])): 
              #Add the contract title
              clause_data['contract title'].append(contract_data['data'][i]['title'])
              #clause_data['label'].append(labels_LU[contract_data['data'][i]['title']] if contract_data['data'][i]['title'] in labels_LU else 'marketing agreement' )  #<- manual error trap applied here
              clause_data['clause'].append(contract_data['data'][i]['paragraphs'][0]['qas'][j]['id'].split(sep='__')[1])
              clause_data['annotation'].append(contract_data['data'][i]['paragraphs'][0]['qas'][j]['answers'][k]['text'])
              clause_data['annotation start'].append(contract_data['data'][i]['paragraphs'][0]['qas'][j]['answers'][k]['answer_start'])
              clause_data['annotation length'].append(len(contract_data['data'][i]['paragraphs'][0]['qas'][j]['answers'][k]['text']))
                
    
        #If return_df == True retrun data as a DataFrame
        if return_df:
            return pd.DataFrame(clause_data),pd.DataFrame(raw_contracts)
        #Else export the data dictionary
        else:
            return clause_data,raw_contracts
            

    def extract_clause(self, clause_of_interest = '', concat = False, clean = True):
        """
        Method to extract specific clause data
        """
        CUAD_df, _ = self.extract_contracts(True)
        clause_df = CUAD_df[CUAD_df['clause'] == clause_of_interest]
        
        #Get volume of contract titles found in total (use in % calc)
        cont_vol = np.unique(CUAD_df['contract title']).shape[0]

        #If concat, combine annotations where contracts have more than one
        if concat:
            #Limit df to clause of interest and extract annotations of itnerest
            of_interest_data = clause_df[clause_df['clause'] == clause_of_interest]
            
            #Identify where there are multiple annotations per contract
            titles,counts = np.unique(of_interest_data['contract title'],return_counts =True)
            dups = titles[counts >= 2]
            
            #Output Analysis
            print('There are {} contracts ({:.2%}) with \'{}\' annotations'.format(*(titles.shape[0],titles.shape[0]/cont_vol,clause_of_interest)))
            print('There are {} contracts with more than one annotation'.format(dups.shape[0]))
                     
            #Initate memory for annotations within contracts
            combined_annotations_list = defaultdict(list)
            combined_annotations_string = {}
            
            #For each annotation of interest found in the contract, 
            #append annotation to a default dict list with contract as key
            for i in of_interest_data.index:
              name = of_interest_data.loc[i,['contract title']].values[0]
              annotation = of_interest_data.loc[i,['annotation']].values[0]
              combined_annotations_list[name].append(annotation)
            
            #Produce a single string of all annotations found in specific contracts
            for key in combined_annotations_list.keys():
              combined_annotations_string[key] = ' '.join(combined_annotations_list[key])
            
            #Build array of contract names and concatenated annotations
            contracts_title = np.array(list(combined_annotations_string.keys()))
            clause_annotations = np.array(list(combined_annotations_string.values()))
        else:
            clause_annotations = np.array(clause_df['annotation'].values)
            contracts_title= np.array(clause_df['contract title'].values)
    
        
        if clean:
            clause_annotations = np.array(list(map(lambda x: re.sub('\\t|\\r|\\n|[^\S]{2,}',' ',x),clause_annotations)))
            #clause_annotations = np.array(list(map(lambda x: x.translate(str.maketrans('', '', string.punctuation)),clause_annotations)))
    
    
        return contracts_title, clause_annotations
    
    
    
class clean_clause(BaseEstimator, TransformerMixin):
    
    def __init__(self,punct=False, lower=False, stop_wd = False, double_spaces= False,strip=False,additional_stop_words = [],token_tidy ={}):        
        self.punct = punct
        self.lower = lower
        self.stop_wd = stop_wd
        self.double_spaces = double_spaces
        self.stop_words = stopwords.words('english') + additional_stop_words
        self.strip = strip
        self.token_tidy = token_tidy
    
    def fit(self,X,y=None):
        return self

    def transform(self,X,y = None):
        
        return_X = X.copy()
        
        #If self.punt, remove punctuation from features
        if self.punct:
            return_X = np.array(list(map(lambda x: x.translate(str.maketrans('', '', string.punctuation)),return_X)))
            
        if self.lower:
            return_X = np.array(list(map(lambda x: x.lower(),return_X)))
            
        if self.stop_wd:
            return_X = np.array(list(map(lambda x:' '.join([token for token in x.split(sep=' ') if token not in self.stop_words]),return_X)))
            
            
        if self.strip:
            return_X = np.array(list(map(lambda x: x.strip(),return_X)))
            
        
        if self.token_tidy != {}:
            for key in self.token_tidy.keys():
                return_X = np.array(list(map(lambda x: x.replace(key,self.token_tidy[key]),return_X)))
                
        if self.double_spaces:
            return_X = np.array(list(map(lambda x: re.sub('\\t|\\r|\\n|[^\S]{2,}',' ',x),return_X)))
               
        return return_X

        
        
        