# -*- coding: utf-8 -*-


from google.colab import auth
from oauth2client.client import GoogleCredentials
import gspread
from gspread_dataframe import set_with_dataframe



def google_authenticate():
    auth.authenticate_user()
    gc = gspread.authorize(GoogleCredentials.get_application_default())
    return gc



def write_to_google_sheet(dataframe, spreadsheet_key, worksheet_name, goog_creds = gc):
    sh = goog_creds.open_by_key(spreadsheet_key)
    ws = None
    worksheet_list = sh.worksheets()
    for worksheet in worksheet_list:
        if worksheet.title == worksheet_name:
            ws = worksheet
            if ws is None:
                ws = sh.add_worksheet(title = worksheet_name, rows="1", cols = "1")

    set_with_dataframe(ws, dataframe, row=1, col=1, include_index=False, 
                       include_column_header=True, resize=True, 
                       allow_formulas=True)

