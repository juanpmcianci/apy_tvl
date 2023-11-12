#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 11 22:33:55 2023

@author: juanpablomadrigalcianci
"""

from web3 import Web3
import json
from datetime import datetime,timedelta
import pandas as pd
from typing import List, Dict
from tqdm import tqdm

# Connect to Ethereum node
NODE='https://mainnet.infura.io/v3/1e3a4cec87af41de9030f2be1f7a77f8'
CONTRACT_ADDRESS='0x4Ddc2D193948926D02f9B1fE9e1daa0718270ED5'
CONTRACT_ADDRESSV3='0xA17581A9E3356d9A858b789D68B4d866e593aE94'
Blocks_Per_Day = 7200 
Days_Per_Year = 365.25
Mantissa=1e18
SecondsPerYear=365*3600*24
V3_start=16983730


def generate_daily_timestamps(start_date: str, end_date: str) -> List[str]:
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    return [(start + timedelta(days=i)).strftime('%Y-%m-%d %H:%M:%S') for i in range((end - start).days + 1)]



def data_to_dataframe(data: Dict) -> pd.DataFrame:
    return pd.DataFrame(data)


def date_to_ethereum_block_estimate(date_str,w3):
    # Convert date string to timestamp
    target_timestamp = int(datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S').timestamp())

    # Get the latest block
    latest_block = w3.eth.get_block('latest')
    latest_block_timestamp = latest_block.timestamp
    latest_block_number = latest_block.number

    # Average Ethereum block time (in seconds)
    average_block_time = 13  # Adjust this based on the current network conditions

    # Estimate the block number
    seconds_difference = target_timestamp - latest_block_timestamp
    block_difference = seconds_difference / average_block_time
    estimated_block_number = int(latest_block_number + block_difference)

    return estimated_block_number


def block_number_to_date(block_number,w3):
    # Retrieve the block
    block = w3.eth.get_block(block_number)
    timestamp = block['timestamp']

    # Convert timestamp to date
    return datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')







def _computeAPY(date_str):
        
    with open('../ABI/Compound_ABIV3.json', 'r') as abi_file:
            contract_abi_V3 = json.load(abi_file)
    
    
    with open('../ABI/Compound_ABIV2.json', 'r') as abi_file:
            contract_abi_V2 = json.load(abi_file)
    
    w3 = Web3(Web3.HTTPProvider(NODE))
    
    
    # Initialize the contract
    contractV3 = w3.eth.contract(address=CONTRACT_ADDRESSV3, 
                               abi=contract_abi_V3)
    
    contractV2 = w3.eth.contract(address=CONTRACT_ADDRESS, 
                               abi=contract_abi_V2)
    block_number=date_to_ethereum_block_estimate(date_str,w3)
    

    
    
    if block_number<V3_start:
        total_supply=contractV2.functions.totalSupply().call(block_identifier=block_number)
        rate=contractV2.functions.supplyRatePerBlock().call(block_identifier=block_number)
        rateB=contractV2.functions.borrowRatePerBlock().call(block_identifier=block_number)
        total_borrow=contractV2.functions.totalBorrows().call(block_identifier=block_number)
        
        SupplyAPR = rate / Mantissa * Blocks_Per_Day*Days_Per_Year 
        BorrowAPR = rateB / Mantissa * Blocks_Per_Day*Days_Per_Year 
    
    
    
    else:
    
        
        total_supply=contractV3.functions.totalSupply().call(block_identifier=block_number)
        gu=contractV3.functions.getUtilization().call(block_identifier=block_number)
        rate=contractV3.functions.getSupplyRate(gu).call(block_identifier=block_number)
        rateB=contractV3.functions.getBorrowRate(gu).call(block_identifier=block_number)    
        total_borrow=contractV3.functions.totalBorrow().call(block_identifier=block_number)
    
        SupplyAPR = rate / Mantissa * SecondsPerYear 
        BorrowAPR = rateB / Mantissa * SecondsPerYear 
    
    
    
    SupplyAPY= (1+SupplyAPR/(Blocks_Per_Day*Days_Per_Year))**(Blocks_Per_Day*Days_Per_Year)-1
    
    BorrowAPY= (1+BorrowAPR/(Blocks_Per_Day*Days_Per_Year))**(Blocks_Per_Day*Days_Per_Year)-1
    
    
    
    
    aux={
         'SupplyAPR':SupplyAPR,
         'SupplyAPY':SupplyAPY,
         'BorrowAPR':BorrowAPR,
         'BorrowAPY':BorrowAPY,
         'TotalSupply':total_supply/Mantissa,
         'TotalBorrow':total_borrow/Mantissa,
         'timestamp':block_number,
         'date':block_number_to_date(block_number,w3)
         }
    return aux




def build_df(start_date: str, end_date: str, decimals: int = 18):
    timestamps = generate_daily_timestamps(start_date, end_date)
    
    res=[]
    for t in tqdm(timestamps):
        res.append(_computeAPY(t))
    df=pd.DataFrame(res)
    df.to_csv('compound_df.csv')
    
    return pd.DataFrame(res)
    
   

a=build_df(start_date='2022-11-11', end_date='2023-11-11')
    
