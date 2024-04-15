# -*- coding: utf-8 -*-

import gspread
from gspread_dataframe import set_with_dataframe
# from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account

SCOPES = ['https://www.googleapis.com/auth/spreadsheets'] 

class TVCLoad:
    def __init__(self, credentials_file):
        self.gc = self.init_gsheets_client(credentials_file, SCOPES)



    def init_gsheets_client(self, credentials_file, scopes,  **kwargs):
        # creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, 
        #                                                          scopes)
        creds = service_account.Credentials.from_service_account_file(
           credentials_file, scopes=scopes
        )
        client = gspread.authorize(creds)
        
        return client

    
    
    def write_to_google_sheet(self, dataframe, worksheet_name, spreadsheet_key):
      
      sh = self.gc.open_by_key(spreadsheet_key)
      
      ws = None
      worksheet_list = sh.worksheets()
      for worksheet in worksheet_list:
        if worksheet.title == worksheet_name:
          ws = worksheet
      if ws is None:
        ws = sh.add_worksheet(title = worksheet_name, rows="1", cols = "1")
        
      set_with_dataframe(ws, 
                         dataframe, 
                         row=1, 
                         col=1,
                         include_index=False, 
                         include_column_header=True, 
                         resize=True, 
                         allow_formulas=True
                         )    
    

    
