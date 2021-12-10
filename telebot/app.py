import os
import sqlalchemy
from sqlalchemy.dialects.mssql import pymssql

import datetime
import urllib.parse

import numpy as np
import pandas as pd

hostname = os.environ.get("HOSTNAME")
username = os.environ.get("USERNAME")
password = os.environ.get("PASSWORD")

engine = sqlalchemy.create_engine(f'mssql+pymssql://{username}:{password}@{hostname}')

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from df2gspread import df2gspread as d2g

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(
    os.environ.get("KEYFILE"), scopes=scope)
gc = gspread.authorize(credentials)
spreadsheet_key = "19Lt8Bf0xglLDNcEjMBLV08r6UNmvAlRC3Z_tsOV1OUQ"

selects = ["""
    select
        count(case
            when convert(date, CreateDate) = dateadd(day, -1, convert(date, getdate()))
                then CustomerID
        end) as value
        ,count(CustomerID) as cumulative
    from
            card.dbo.Wallet wt
    where 1 = 1
        and convert(date, CreateDate) >= dateadd(day, - datepart(day, dateadd(day, -1, getdate())), convert(date, getdate()))
        and wt.WalletTypeID = 9
        and wt.IsHold = 0
    ;""",
    """
    select count(CustomerID) as value
    from (
        select se.CustomerID
            ,convert(date, min(TransactionDate)) as first_transaction_dt
        from
                card.dbo.Wallet wt
        left join
                statement.dbo.StatementEntry as se
            on wt.CustomerID = se.CustomerID
        where 1 = 1
            and wt.IsHold = 0
            and wt.WalletTypeID = 9
            and se.StateID in (0, 1)
            and se.TransactionAmount > 0
         group by se.CustomerID
         having convert(date, min(TransactionDate)) = dateadd(day, -1, convert(date, getdate()))
     ) temp
    ;""",
    """
    select count(*) as value
    from
            statement.dbo.StatementEntry as se
    inner join
            statement.dbo.Payin pi
        on se.ExternalTransactionID = pi.ExternalPayinID
    where 1 = 1
        and convert(date, se.TransactionDate) = dateadd(day, -1, convert(date, getdate()))
        and se.StateID in (0, 1)
        and se.FeeAmount > 0
    group by convert(date, se.TransactionDate)
    ;""",
    """
    select count(AccountAmount) as value
    from
            statement.dbo.StatementEntry
    where 1 = 1
        and StateID in (0, 1)
        and Direction = -1
        and TypeID = 3
        and TransactionAmount > 0
        and convert(date, TransactionDate) = dateadd(day, -1, convert(date, getdate()))
    group by convert(date, TransactionDate)
    ;""",
    """
    select count(distinct cd.CardID) as cards
        ,count(distinct wt.WalletID) as wallets
        ,count(distinct ct.CustomerID) - count(distinct wt.CustomerID) as preorders
        ,count(distinct ct.CustomerID) as total
    from
            customer.dbo.Contact ct
    left join
            card.dbo.Wallet wt
        on ct.CustomerID = wt.CustomerID
        and wt.WalletTypeID = 9
        and wt.IsHold = 0
    left join
            card.dbo.Card cd
        on wt.WalletID = cd.WalletID
        and cd.CardStateID = 0
    ;"""]

for idx, wks_name in enumerate(["card", "activation", "topup", "purchase", "lifetime"]):
    # print(idx, wks_name)
    # print(pd.read_sql(selects[idx], engine))
    d2g.upload(pd.read_sql(selects[idx], engine),
               spreadsheet_key,
               wks_name=wks_name,
               col_names=False,
               row_names=False,
               credentials=credentials,
               df_size=True)