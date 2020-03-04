import pickle
import os
import json
import pandas as pd
from googleapiclient.discovery import build
from google.oauth2 import service_account


class Sheet:

    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
    AUTH_REDIRECT_URL = os.getenv("GOOGLE_REDIRECT_URI", "http://localhost")
    STORAGE_ROOT = os.getenv("STORAGE_ROOT", './static')

    def __init__(self, spreadsheet_id):
        # Create credentials.json file (if not there)
        creds_fp = "{}/credentials.json".format(self.STORAGE_ROOT)
        if not os.path.exists(creds_fp):
            self.createCredentialsFile(creds_fp)

        # build creds from service account json (generated above)
        creds = service_account.Credentials.from_service_account_file(creds_fp, scopes=self.SCOPES)

        # build service
        service = build("sheets", "v4", credentials=creds)
        self.sheet = service.spreadsheets()
        self.spreadsheet_id = spreadsheet_id

    def read(self, sheet_range):
        """
        Returns google sheet as pandas dataframe
        """
        sheet_data = self.sheet.values().get(spreadsheetId=self.spreadsheet_id,
                                             range=sheet_range).execute()

        return self.sheetToDataframe(sheet_data)

    def write(self, sheet_range, df, clear=True):
        data = self.dataframeToSheetData(df)

        try:
            if (clear):
                self.sheet.values().clear(spreadsheetId=self.spreadsheet_id,
                                          range=sheet_range).execute()
                print("Sheet cleared!")

            result = self.sheet.values().update(
                spreadsheetId=self.spreadsheet_id,
                range=sheet_range,
                valueInputOption="RAW",
                body={"majorDimension": "ROWS", "values": data}).execute()

            print("{0} cells updated.".format(result.get("updatedCells")))
        except Exception as ex:
            print("Unable to write to spreadsheet: {}".format(ex))

    def dataframeToSheetData(self, df):
        return df.T.reset_index().T.values.tolist()

    def sheetToDataframe(self, sheet):
        data = sheet.get("values", [])
        header = data[0]  # Assumes first line is header
        values = data[1:]

        if not values:
            print("No data found!")
        else:
            data = []
            for col_id, col_name in enumerate(header):
                column_data = []
                for row in values:
                    try:
                        column_data.append(row[col_id])
                    except IndexError:
                        column_data.append(None)
                ds = pd.Series(data=column_data, name=col_name)
                data.append(ds)
            df = pd.concat(data, axis=1)
            return df

    def createCredentialsFile(self, path):
        with open(path, "w") as fp:
            creds = {
                "type": "service_account",
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                "project_id": os.getenv("GOOGLE_PROJECT_ID"),
                "private_key_id": os.getenv("GOOGLE_PRIVATE_KEY_ID"),
                "private_key": os.getenv("GOOGLE_PRIVATE_KEY"),
                "client_email": os.getenv("GOOGLE_CLIENT_EMAIL"),
                "client_id": os.getenv("GOOGLE_CLIENT_ID"),
                "client_x509_cert_url": os.getenv("GOOGLE_CLIENT_X509_CERT_URL")
            }

            json.dump(creds, fp)
