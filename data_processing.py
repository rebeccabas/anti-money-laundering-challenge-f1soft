import pandas as pd
import logging
import sys
import re
from datetime import datetime
import os

# Setup logging
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Constants
INPUT_CSV = "Txn_data.csv"
OUTPUT_CSV = "processed_txn_data.csv"
ID_MAPPING_CSV = "id_mapping.csv"
MONTHLY_THRESHOLD = 100000  # 1 lakh
CHUNK_SIZE = 5000000
AMOUNT_THRESHOLD = 2000  # New constant for amount filtering

class AccountMapper:
    def __init__(self):
        self.str_to_int = {}
        self.next_id = 1
    
    def get_id(self, account_str):
        """Get integer ID for account string, creating new mapping if needed."""
        if account_str not in self.str_to_int:
            self.str_to_int[account_str] = self.next_id
            self.next_id += 1
        return self.str_to_int[account_str]
    
    def save_mapping(self, filepath):
        """Save ID mapping to CSV."""
        mapping_df = pd.DataFrame(
            [(k, v) for k, v in self.str_to_int.items()],
            columns=['original_id', 'mapped_id']
        )
        mapping_df.to_csv(filepath, index=False)
        logging.info(f"Saved {len(mapping_df)} ID mappings to {filepath}")

def is_synthetic_account(account_str: str) -> bool:
    """Check if account ID contains non-numeric characters."""
    return not bool(re.match(r'^\d+$', str(account_str)))

def process_chunk(chunk, account_mapper):
    """Process a single chunk of data."""
    # Convert datetime and ensure account IDs are strings
    chunk['Date/Time'] = pd.to_datetime(chunk['Date/Time'])
    chunk['From_Account_id'] = chunk['From_Account_id'].astype(str)
    chunk['To_Account_id'] = chunk['To_Account_id'].astype(str)
    
    # Map string IDs to integers
    chunk['From_Account_id'] = chunk['From_Account_id'].apply(account_mapper.get_id)
    chunk['To_Account_id'] = chunk['To_Account_id'].apply(account_mapper.get_id)
    
    # Drop T_id column if it exists
    if 'T_id' in chunk.columns:
        chunk = chunk.drop('T_id', axis=1)
    
    # Create month-year column for grouping
    chunk['month_year'] = chunk['Date/Time'].dt.to_period('M')
    
    return chunk

def get_high_volume_accounts(df):
    """
    Identify sending accounts with total transactions >= threshold.
    Only considers outgoing transactions (From_Account_id).
    """
    # Group by From_Account_id only and sum their sent amounts
    sent_totals = df.groupby('From_Account_id')['amount'].sum().reset_index()
    
    # Find accounts that exceed threshold
    high_volume_accounts = sent_totals[sent_totals['amount'] >= MONTHLY_THRESHOLD]['From_Account_id'].unique()
    print(high_volume_accounts)
    print(sent_totals[sent_totals['amount'] >= MONTHLY_THRESHOLD])
    
    logging.info(f"Found {len(high_volume_accounts)} high-volume sending accounts")
    
    return set(high_volume_accounts)

def get_synthetic_accounts(df, account_mapper):
    """Get all synthetic account IDs based on original IDs."""
    original_synthetic = set()
    
    # Reverse mapping for checking
    int_to_str = {v: k for k, v in account_mapper.str_to_int.items()}
    
    # Check both From and To accounts
    for acc_id in pd.concat([df['From_Account_id'], df['To_Account_id']]).unique():
        original_id = int_to_str[acc_id]
        if is_synthetic_account(original_id):
            original_synthetic.add(acc_id)
    
    return original_synthetic

def process_transactions():
    """Main function to process transaction data."""
    if os.path.exists(OUTPUT_CSV):
        os.remove(OUTPUT_CSV)
    
    logging.info(f"Starting transaction data processing from {INPUT_CSV}")
    
    account_mapper = AccountMapper()
    high_volume_accounts = set()
    synthetic_accounts = set()
    all_chunks = []
    
    # First pass: Identify suspicious accounts
    logging.info("First pass: Identifying suspicious accounts...")
    for chunk_idx, chunk in enumerate(pd.read_csv(INPUT_CSV, chunksize=CHUNK_SIZE, low_memory=False)):
        processed_chunk = process_chunk(chunk, account_mapper)
        all_chunks.append(processed_chunk)
        
        # Update sets of suspicious accounts
        high_volume_accounts.update(get_high_volume_accounts(processed_chunk))
        synthetic_accounts.update(get_synthetic_accounts(processed_chunk, account_mapper))
        
        if chunk_idx % 10 == 0:
            logging.info(f"Processed chunk #{chunk_idx} for suspicious accounts")
    
    suspicious_accounts = high_volume_accounts | synthetic_accounts
    logging.info(f"Found {len(suspicious_accounts)} suspicious accounts "
                f"({len(high_volume_accounts)} high volume, {len(synthetic_accounts)} synthetic)")
    
    # Save ID mapping
    account_mapper.save_mapping(ID_MAPPING_CSV)
    
    # Second pass: Filter and save relevant transactions
    logging.info("Second pass: Filtering relevant transactions...")
    header = True
    
    for chunk_idx, chunk in enumerate(all_chunks):
        # Keep transactions where either account is suspicious AND amount is greater than threshold
        filtered_chunk = chunk[
            ((chunk['From_Account_id'].isin(suspicious_accounts)) |
             (chunk['To_Account_id'].isin(suspicious_accounts))) &
            (chunk['amount'] > 2000)  # New condition for amount filtering
        ]
        
        # Drop month_year column before saving
        filtered_chunk = filtered_chunk.drop('month_year', axis=1)
        
        if not filtered_chunk.empty:
            filtered_chunk.to_csv(
                OUTPUT_CSV,
                mode='a',
                index=False,
                header=header
            )
            header = False
        
        if chunk_idx % 10 == 0:
            logging.info(f"Saved filtered chunk #{chunk_idx}")
    
    logging.info(f"Processing complete. Output saved to {OUTPUT_CSV}")

if __name__ == "__main__":
    process_transactions()