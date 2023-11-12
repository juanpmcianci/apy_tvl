#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 11 19:30:06 2023
@author: juanpablomadrigalcianci
"""

import pandas as pd
import requests
from datetime import datetime, timedelta
from typing import List, Dict
from tqdm import tqdm

def generate_daily_timestamps(start_date: str, end_date: str, stepsize:int) -> List[int]:
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    return [int(start.timestamp() + i * 86400) for i in range((end - start).days + stepsize)]

def run_query(url: str, query: str) -> Dict:
    response = requests.post(url, json={'query': query})
    response.raise_for_status()
    return response.json()

def data_to_dataframe(data: Dict) -> pd.DataFrame:
    return pd.DataFrame(data)

def create_query(timestamp: int, reserve_id: str) -> str:
    reserve_id = reserve_id.lower()
    return f'''
    {{
      reserve (id: "{reserve_id}") {{
        paramsHistory(where: {{timestamp_gte: {timestamp} }}, first: 1) {{
          variableBorrowRate
          utilizationRate
          liquidityRate
          timestamp
          stableBorrowRate
          totalLiquidity
          totalATokenSupply
          availableLiquidity
        }}
      }}
    }}
    '''

def build_df(token_address: str, start_date: str, end_date: str, decimals: int = 18,stepsize: int=1) -> pd.DataFrame:
    RAY = 10**27 
    SECONDS_PER_YEAR = 31536000
    api_url = 'https://api.thegraph.com/subgraphs/name/aave/protocol-v3'
    reserve_id = f'{token_address}0x2f39d218133afab8f2b819b1066c7e434ad94e9e'

    timestamps = generate_daily_timestamps(start_date, end_date,stepsize)
    dataframes = [data_to_dataframe(run_query(api_url, create_query(ts, reserve_id))['data']['reserve']['paramsHistory']) for ts in tqdm(timestamps)]

    final_df = pd.concat(dataframes, ignore_index=True)
    cols=['variableBorrowRate', 'liquidityRate', 'stableBorrowRate']
    
    for c in cols: 
        final_df[c]=final_df[c].aply(lambda x: float(x))/ RAY

    final_df['depositAPY'] = ((1 + final_df.liquidityRate / SECONDS_PER_YEAR) ** SECONDS_PER_YEAR) - 1
    final_df['variableBorrowAPY'] = ((1 + final_df.variableBorrowRate / SECONDS_PER_YEAR) ** SECONDS_PER_YEAR) - 1
    final_df['stableBorrowAPY'] = ((1 + final_df.stableBorrowRate / SECONDS_PER_YEAR) ** SECONDS_PER_YEAR) - 1
    final_df['ReserveSizeToken'] = final_df['totalATokenSupply'] / 10**decimals
    final_df['AvailableLiquidityToken'] = final_df['availableLiquidity'] / 10**decimals
    final_df['time'] = pd.to_datetime(final_df['timestamp'], unit='s')
    final_df.sort_values(by='timestamp', inplace=True)

    print("Data successfully processed")
    return final_df

token_id = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
start_date = '2023-11-10'
end_date = '2023-11-11'

df = build_df(token_id, start_date, end_date)
