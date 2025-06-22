import asyncio
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, AuthKeyUnregisteredError
import os
import re
import boto3
import pandas as pd
from datetime import datetime, date, timedelta
from io import BytesIO
import logging
import traceback

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration from environment variables
API_ID = os.getenv('API_ID')
API_HASH = os.getenv('API_HASH')
PHONE_NUMBER = os.getenv('PHONE_NUMBER')

# AWS S3 Configuration from environment variables
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION', 'eu-north-1')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
S3_FILE_KEY = os.getenv('S3_FILE_KEY', 'trans.csv')
S3_SESSION_KEY = os.getenv('S3_SESSION_KEY', 'telegram_session.session')

# Validate required environment variables
required_vars = ['API_ID', 'API_HASH', 'PHONE_NUMBER', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'S3_BUCKET_NAME']
missing_vars = [var for var in required_vars if not os.getenv(var)]

if missing_vars:
    logger.error(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
    exit(1)


class TelegramTransactionReader:
    def __init__(self, api_id, api_hash, phone_number):
        self.api_id = api_id
        self.api_hash = api_hash
        self.phone_number = phone_number
        
        # Try to download session from S3 first
        self.session_file = self.download_session_from_s3()
        self.client = TelegramClient(self.session_file, api_id, api_hash)
        
        # Initialize S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
    
    def download_session_from_s3(self):
        """Download session file from S3 if it exists"""
        try:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name=AWS_REGION
            )
            
            response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=S3_SESSION_KEY)
            session_data = response['Body'].read()
            
            with open('session.session', 'wb') as f:
                f.write(session_data)
            
            logger.info("✅ Session file downloaded from S3")
            return 'session'
            
        except Exception as e:
            logger.info(f"No session file in S3 or error downloading: {e}")
            return 'session'
    
    def upload_session_to_s3(self):
        """Upload session file to S3 for backup"""
        try:
            if os.path.exists('session.session'):
                with open('session.session', 'rb') as f:
                    session_data = f.read()
                
                self.s3_client.put_object(
                    Bucket=S3_BUCKET_NAME,
                    Key=S3_SESSION_KEY,
                    Body=session_data
                )
                logger.info("✅ Session file backed up to S3")
                return True
        except Exception as e:
            logger.error(f"❌ Failed to backup session to S3: {e}")
            return False
    
    async def authenticate(self):
        """Simplified authentication"""
        try:
            await self.client.connect()
            
            if await self.client.is_user_authorized():
                logger.info("✅ Already authenticated")
                return True
            
            logger.error("❌ Authentication required but running in automated mode")
            return False
                
        except AuthKeyUnregisteredError:
            logger.error("❌ Session expired, need re-authentication")
            return False
                
        except Exception as e:
            logger.error(f"❌ Authentication error: {e}")
            return False
    
    def read_csv_from_s3(self):
        """Read ALL existing transactions from CSV file in S3 - reads everything as strings first"""
        try:
            response = self.s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=S3_FILE_KEY)
            csv_data = response['Body'].read()
            
            # Read CSV as all strings to avoid any data type conversion issues
            df = pd.read_csv(BytesIO(csv_data), dtype=str, keep_default_na=False)
            
            logger.info(f"📁 Raw CSV loaded with {len(df)} rows")
            logger.info(f"📋 Columns found: {list(df.columns)}")
            
            # Print first few rows for debugging
            if not df.empty:
                logger.info("🔍 First 3 rows of raw data:")
                for i in range(min(3, len(df))):
                    logger.info(f"   Row {i}: {df.iloc[i].to_dict()}")
            
            # Clean up column names (remove extra spaces)
            df.columns = df.columns.str.strip()
            
            # Expected columns
            expected_columns = ['Date', 'Amount', 'C/D', 'From/To', 'Reason', 'message id', 'Balance']
            
            # Ensure all expected columns exist
            for col in expected_columns:
                if col not in df.columns:
                    df[col] = 'nill'
                    logger.warning(f"Added missing column: {col}")
            
            # Reorder columns to match expected order
            df = df[expected_columns]
            
            # Clean up the data - keep everything as strings initially
            # Replace empty strings and 'nan' with 'nill' for consistency
            df = df.replace(['', 'nan', 'NaN', 'None'], 'nill')
            
            # Filter out completely invalid rows
            original_count = len(df)
            
            # Remove rows where BOTH Date and Amount are 'nill' (completely invalid rows)
            invalid_mask = (df['Date'] == 'nill') & (df['Amount'] == 'nill')
            df = df[~invalid_mask]
            
            # Remove rows where Date is still showing as ###### (display issue but no actual data)
            # But keep rows where Date might be a valid date string
            df = df[df['Date'] != '#######']
            
            if len(df) < original_count:
                logger.info(f"🧹 Filtered out {original_count - len(df)} completely invalid rows")
            
            if df.empty:
                logger.warning("⚠️ No valid existing data found after filtering")
                df = pd.DataFrame(columns=expected_columns)
                return df
            
            # Now convert data types carefully while preserving the data
            
            # Convert Amount to float (but keep 'nill' as 0.0)
            def safe_float_convert(val):
                if val == 'nill' or val == '':
                    return 0.0
                try:
                    return float(val)
                except (ValueError, TypeError):
                    logger.warning(f"Could not convert amount '{val}' to float, using 0.0")
                    return 0.0
            
            df['Amount'] = df['Amount'].apply(safe_float_convert)
            
            # Convert Balance to float
            df['Balance'] = df['Balance'].apply(safe_float_convert)
            
            # Keep message id as string but clean it up
            df['message id'] = df['message id'].astype(str)
            df['message id'] = df['message id'].replace('nill', 'nill')  # Keep 'nill' as is
            
            # Clean up other string columns
            df['C/D'] = df['C/D'].replace('nill', 'UNKNOWN')
            df['From/To'] = df['From/To'].replace('nill', 'UNKNOWN')  
            df['Reason'] = df['Reason'].replace('nill', 'No reason provided')
            
            # Handle Date column - assume it's already in DD-MM-YYYY format
            # Just validate that dates are reasonable
            def validate_date(date_str):
                if date_str == 'nill' or date_str == '':
                    return None
                # If it looks like a valid date pattern, keep it as-is
                # DD-MM-YYYY pattern check
                if re.match(r'\d{1,2}-\d{1,2}-\d{4}', str(date_str)):
                    return str(date_str)
                # Try to parse and reformat if needed
                try:
                    for fmt in ['%d-%m-%Y', '%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y']:
                        try:
                            parsed_date = pd.to_datetime(date_str, format=fmt)
                            return parsed_date.strftime('%d-%m-%Y')
                        except:
                            continue
                    # Last resort - pandas auto parsing
                    parsed_date = pd.to_datetime(date_str)
                    return parsed_date.strftime('%d-%m-%Y')
                except:
                    logger.warning(f"Could not parse date '{date_str}', keeping as-is")
                    return str(date_str)
            
            df['Date'] = df['Date'].apply(validate_date)
            
            # Remove rows where date validation completely failed
            df = df[df['Date'].notna()]
            df = df[df['Date'] != 'None']
            
            logger.info(f"✅ CSV file processed successfully: {len(df)} valid rows")
            if not df.empty:
                # Find the last valid balance
                last_balance = df.iloc[-1]['Balance']
                logger.info(f"💰 Latest balance from existing data: ₹{last_balance:.2f}")
                logger.info(f"📅 Date range: {df['Date'].iloc[0]} to {df['Date'].iloc[-1]}")
                logger.info(f"🆔 Sample message IDs: {df['message id'].iloc[:3].tolist()}...")
                
                # Show some statistics
                credit_count = len(df[df['C/D'] == 'CREDIT'])
                debit_count = len(df[df['C/D'] == 'DEBIT'])
                logger.info(f"📊 Transaction breakdown: {credit_count} CREDIT, {debit_count} DEBIT")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Error reading CSV from S3: {e}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
            # Return empty DataFrame if file doesn't exist or can't be read
            df = pd.DataFrame(columns=['Date', 'Amount', 'C/D', 'From/To', 'Reason', 'message id', 'Balance'])
            logger.info("🆕 Created new empty DataFrame due to error")
            return df

    def debug_existing_data(self):
        """Debug method to check what's in your S3 CSV file"""
        try:
            response = self.s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=S3_FILE_KEY)
            csv_data = response['Body'].read().decode('utf-8')
            
            logger.info("🔍 RAW CSV CONTENT (first 1000 characters):")
            logger.info(csv_data[:1000])
            
            lines = csv_data.split('\n')
            logger.info(f"📊 Total lines in CSV: {len(lines)}")
            
            if lines:
                logger.info(f"📋 Header line: {lines[0]}")
                if len(lines) > 1:
                    logger.info(f"📄 First data line: {lines[1]}")
                if len(lines) > 2:
                    logger.info(f"📄 Second data line: {lines[2]}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error reading raw CSV: {e}")
            return False
    
    def upload_csv_to_s3(self, df):
        """Upload CSV file to S3"""
        try:
            csv_buffer = BytesIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            
            self.s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=S3_FILE_KEY,
                Body=csv_buffer.getvalue(),
                ContentType='text/csv'
            )
            logger.info("✅ CSV file updated successfully in S3")
            return True
        except Exception as e:
            logger.error(f"❌ Error uploading CSV to S3: {e}")
            return False
    
    def calculate_new_balance(self, current_balance, amount, transaction_type):
        """Calculate new balance based on current balance and transaction"""
        try:
            current_balance = float(current_balance)
            amount = float(amount)
        except (ValueError, TypeError):
            logger.warning(f"Invalid balance or amount values: balance={current_balance}, amount={amount}")
            current_balance = 0.0
            amount = 0.0
        
        if transaction_type == 'CREDIT':
            new_balance = current_balance + amount
        else:  # DEBIT
            new_balance = current_balance - amount
        
        return new_balance
    
    def is_valid_transaction_format(self, message_text):
    
        if not message_text:
            return False
        
        message_lower = message_text.lower()
        
        # Check for the exact reason prompt text
        has_reason_prompt = (
            "would you like to add reason for this transaction?" in message_lower and
            "if yes, tag the corresponding message and reply the reason." in message_lower
        )
        
        # Check for amount indicator
        has_amount = "rs" in message_lower or "rs." in message_lower
        
        # Check for transaction type
        has_transaction_type = "debited" in message_lower or "credited" in message_lower
        
        is_valid = has_reason_prompt and has_amount and has_transaction_type
        
        logger.debug(f"Transaction validation - Reason prompt: {has_reason_prompt}, Amount: {has_amount}, Type: {has_transaction_type}, Valid: {is_valid}")
        
        return is_valid

    def extract_transaction_data(self, message_text, message_date, message_id):
        """
        Extract transaction data from messages with the standard reason prompt
        Enhanced to properly prioritize names over UPI IDs
        """
        if not self.is_valid_transaction_format(message_text):
            logger.debug(f"Message {message_id} is not a valid transaction, skipping")
            return None
        
        transaction_data = {
            'message_id': message_id,
            'date': message_date,
            'amount': None,
            'type': None,
            'from_to': None
        }
        
        # Extract amount - look for Rs/Rs. followed by numbers
        amount_patterns = [
            r'Rs\.?\s*(\d+(?:\.\d{2})?)',  # Rs.2000.00 or Rs 250.00
            r'₹\s*(\d+(?:\.\d{2})?)'       # ₹2000.00 (just in case)
        ]
        
        for pattern in amount_patterns:
            amount_match = re.search(pattern, message_text, re.IGNORECASE)
            if amount_match:
                transaction_data['amount'] = amount_match.group(1)
                break
        
        # Extract transaction type
        message_lower = message_text.lower()
        if 'debited' in message_lower:
            transaction_data['type'] = 'DEBIT'
        elif 'credited' in message_lower:
            transaction_data['type'] = 'CREDIT'
        
        # Enhanced name extraction - PRIORITIZE NAMES over UPI IDs
        from_to_patterns = [
            # Pattern 1: Look for "Mr/Mrs/Ms NAME" first (highest priority)
            r'((?:Mr\.?\s+|Mrs\.?\s+|Ms\.?\s+)[A-Z][a-zA-Z\s]+?)(?:\s+UPI|\s+on|\s*$)',
            
            # Pattern 2: "to your account from NAME" - capture proper names
            r'to\s+your\s+account\s+from\s+([A-Z][a-zA-Z\s]+?)(?:\s+UPI|\s+on|\s*$)',
            
            # Pattern 3: "from your account to NAME" - capture proper names
            r'from\s+your\s+account\s+to\s+([A-Z][a-zA-Z\s]+?)(?:\s+UPI|\s+on|\s*$)',
            
            # Pattern 4: "to/from NAME" - capture proper names
            r'to/from\s+([A-Z][a-zA-Z\s]+?)(?:\s+UPI|\s+on|\s*$)',
            
            # Pattern 5: Look for capitalized words (likely names) before UPI
            r'(?:to|from|to/from)\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*)\s+UPI',
            
            # Pattern 6: Business/Shop names (often in caps or title case)
            r'(?:to|from|to/from)\s+([A-Z][A-Z\s]+(?:[A-Z][a-z\s]*)*?)(?:\s+UPI|\s+on)',
            
            # Pattern 7: Fallback - any text between prepositions and UPI
            r'(?:to|from|to/from)\s+([^@\n]+?)\s+UPI:'
        ]
        
        # Try to extract name using patterns (prioritizing actual names)
        for i, pattern in enumerate(from_to_patterns):
            name_match = re.search(pattern, message_text, re.IGNORECASE)
            if name_match:
                extracted_name = name_match.group(1).strip()
                
                # Validate that it's a proper name (not a UPI ID or random text)
                if extracted_name and len(extracted_name) > 2:
                    # Check if it's NOT a UPI ID pattern
                    if not re.match(r'^\w+@\w+', extracted_name) and not re.match(r'^\d+$', extracted_name):
                        # Clean up the name
                        extracted_name = ' '.join(extracted_name.split())  # Clean multiple spaces
                        extracted_name = extracted_name.title()  # Proper case
                        transaction_data['from_to'] = extracted_name
                        logger.debug(f"Extracted name using pattern {i+1}: '{extracted_name}'")
                        break
        
        # Only if no proper name found, then look for UPI IDs as fallback
        if not transaction_data['from_to']:
            upi_patterns = [
                # UPI ID patterns
                r'UPI:\s*(\w+@\w+[\w\.]*)',
                r'(\w+@\w+[\w\.]*)',
                # UPI transaction ID patterns
                r'UPI:\s*(\d+)',
                r'UPI\s+(?:ID|Ref):\s*(\w+)',
            ]
            
            for pattern in upi_patterns:
                upi_match = re.search(pattern, message_text)
                if upi_match:
                    extracted_upi = upi_match.group(1).strip()
                    if extracted_upi:
                        transaction_data['from_to'] = f"UPI:{extracted_upi}"
                        logger.debug(f"Fallback: Using UPI ID: '{extracted_upi}'")
                        break
        
        # Last resort fallback
        if not transaction_data['from_to']:
            transaction_data['from_to'] = 'UNKNOWN'
            logger.warning(f"Could not extract name or UPI ID from message {message_id}")
        
        # Log the extraction with more detail
        logger.info(f"✅ Extracted transaction {message_id}: ₹{transaction_data['amount']} {transaction_data['type']} - {transaction_data['from_to']}")
        
        return transaction_data
    
    async def process_and_update_transactions(self):
        """Main processing function - STRICTLY APPENDS new transactions to existing data"""
        try:
            # Authenticate
            if not await self.authenticate():
                logger.error("❌ Authentication failed, cannot proceed")
                return False
            
            # Read ALL existing data (this preserves historical transactions)
            existing_df = self.read_csv_from_s3()
            # Changed from today to yesterday
            yesterday = date.today() - timedelta(days=1)
            
            logger.info(f"📋 EXISTING DATA: {len(existing_df)} rows")
            
            # Get current balance from existing data
            if existing_df.empty:
                current_balance = 0.0
                logger.info("💰 Starting with balance: ₹0.00 (no existing data)")
            else:
                current_balance = float(existing_df.iloc[-1]['Balance'])
                logger.info(f"💰 Current balance from existing data: ₹{current_balance:.2f}")
            
            # Get existing message IDs to prevent duplicates
            existing_msg_ids = set(existing_df['message id'].astype(str).values) if not existing_df.empty else set()
            logger.info(f"🔍 Existing message IDs: {len(existing_msg_ids)} transactions to check for duplicates")
            
            # Collect YESTERDAY's bot messages that contain transaction patterns
            bot_messages = {}
            processed_count = 0
            skipped_count = 0
            
            async for message in self.client.iter_messages(8056410079, limit=1000):
                if (message.date.date() == yesterday and message.sender_id == 8056410079):
                    processed_count += 1
                    transaction_data = self.extract_transaction_data(message.text, message.date, message.id)
                    
                    if transaction_data:  # Only add if transaction pattern is found
                        bot_messages[message.id] = {
                            'transaction': transaction_data,
                            'user_reason': None
                        }
                        logger.info(f"✅ Valid transaction found: Message {message.id}")
                    else:
                        skipped_count += 1
            
            logger.info(f"📊 PROCESSED {processed_count} messages from yesterday ({yesterday})")
            logger.info(f"✅ FOUND {len(bot_messages)} valid transaction messages")
            logger.info(f"⏭️ SKIPPED {skipped_count} messages (no transaction pattern)")
            
            # Find user replies to YESTERDAY's valid bot messages
            async for message in self.client.iter_messages(8056410079, limit=1000):
                if (message.date.date() == yesterday and 
                    message.reply_to and 
                    message.reply_to.reply_to_msg_id in bot_messages and
                    message.sender_id != 8056410079):
                    
                    bot_messages[message.reply_to.reply_to_msg_id]['user_reason'] = {
                        'date': message.date,
                        'text': message.text or '[Media/File]'
                    }
            
            if not bot_messages:
                logger.info(f"ℹ️ No valid transactions found for yesterday ({yesterday})")
                return True
            
            # Process and add ONLY NEW transactions (strict duplicate prevention)
            new_transactions = []
            duplicate_count = 0
            
            for msg_id, data in bot_messages.items():
                transaction = data['transaction']
                reason = data['user_reason']
                
                # STRICT duplicate check
                if str(msg_id) in existing_msg_ids:
                    logger.info(f"🔄 DUPLICATE FOUND: Transaction {msg_id} already exists, skipping")
                    duplicate_count += 1
                    continue
                
                amount = float(transaction['amount']) if transaction['amount'] else 0.0
                transaction_type = transaction['type'] or 'UNKNOWN'
                from_to = transaction['from_to'] or 'UNKNOWN'
                reason_text = reason['text'] if reason else 'No reason provided'
                
                # Calculate new balance based on CURRENT balance (not from empty DataFrame)
                new_balance = self.calculate_new_balance(current_balance, amount, transaction_type)
                
                new_transaction = {
                    'Date': transaction['date'].strftime('%d-%m-%Y'),
                    'Amount': amount,
                    'C/D': transaction_type,
                    'From/To': from_to,
                    'Reason': reason_text,
                    'message id': str(msg_id),
                    'Balance': round(new_balance, 2)
                }
                
                new_transactions.append(new_transaction)
                current_balance = new_balance  # Update current balance for next transaction
                
                logger.info(f"➕ NEW TRANSACTION: ₹{amount} {transaction_type} - {from_to} - New Balance: ₹{new_balance:.2f}")
            
            # Summary before updating
            logger.info(f"📈 SUMMARY:")
            logger.info(f"   📋 Existing transactions: {len(existing_df)}")
            logger.info(f"   ➕ New transactions: {len(new_transactions)}")
            logger.info(f"   🔄 Duplicates skipped: {duplicate_count}")
            
            if new_transactions:
                # Create DataFrame for new transactions
                new_df = pd.DataFrame(new_transactions)
                
                # APPEND new transactions to existing data
                updated_df = pd.concat([existing_df, new_df], ignore_index=True)
                
                logger.info(f"📊 FINAL DATA: {len(updated_df)} total rows (was {len(existing_df)}, added {len(new_transactions)})")
                logger.info(f"💰 FINAL BALANCE: ₹{updated_df.iloc[-1]['Balance']:.2f}")
                
                # Upload the combined data
                if self.upload_csv_to_s3(updated_df):
                    logger.info(f"✅ SUCCESS: Added {len(new_transactions)} new transactions to existing data")
                    logger.info(f"📁 Total transactions in file: {len(updated_df)}")
                    return True
                else:
                    logger.error("❌ FAILED: Could not upload to S3")
                    return False
            else:
                logger.info("ℹ️ No new transactions to add yesterday (all were duplicates or none found)")
                return True
                
        except Exception as e:
            logger.error(f"❌ CRITICAL ERROR in main processing: {e}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
            return False
    
    async def close(self):
        """Close connections"""
        try:
            await self.client.disconnect()
            logger.info("✅ Client disconnected")
        except Exception as e:
            logger.error(f"Error closing client: {e}")


# Main execution
async def main():
    logger.info("🚀 Starting daily transaction processor - STRICT APPEND MODE (YESTERDAY'S DATA)")
    logger.info("📋 This will PRESERVE all existing data and only ADD yesterday's transactions")
    
    reader = TelegramTransactionReader(API_ID, API_HASH, PHONE_NUMBER)
    
    try:
        # Optional: Debug existing data first
        # logger.info("🔍 Debugging existing data...")
        # reader.debug_existing_data()
        
        success = await reader.process_and_update_transactions()
        if success:
            logger.info("✅ DAILY PROCESSING COMPLETED SUCCESSFULLY")
        else:
            logger.error("❌ DAILY PROCESSING FAILED")
    except Exception as e:
        logger.error(f"❌ UNEXPECTED ERROR: {e}")
        logger.error(f"Stack trace: {traceback.format_exc()}")
    finally:
        await reader.close()


if __name__ == "__main__":
    asyncio.run(main())
