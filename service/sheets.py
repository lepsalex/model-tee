import pickle
import os.path
import pandas as pd
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


class Sheet:

    # If modifying these scopes, delete the file token.pickle.
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

    def __init__(self, spreadsheet_id):
        """Shows basic usage of the Sheets API.
        Prints values from a sample spreadsheet.
        """
        creds = None
        # The file token.pickle stores the user"s access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists("token.pickle"):
            with open("token.pickle", "rb") as token:
                creds = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    "credentials.json", self.SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open("token.pickle", "wb") as token:
                pickle.dump(creds, token)

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
