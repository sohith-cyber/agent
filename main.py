import asyncio
import os
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, AuthKeyUnregisteredError
import boto3
import pandas as pd
from datetime import datetime, date
from io import BytesIO
import logging
import time
import re

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
S3_FILE_KEY = os.getenv('S3_FILE_KEY', 'transactions.xlsx')
S3_SESSION_KEY = os.getenv('S3_SESSION_KEY', 'telegram_session.session')

# Bot ID and check interval
BOT_ID = int(os.getenv('BOT_ID', '8056410079'))
CHECK_INTERVAL = int(os.getenv('CHECK_INTERVAL', '300'))  # 5 minutes default

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
    
    def read_excel_from_s3(self):
        """Read ALL existing transactions from Excel file in S3"""
        try:
            response = self.s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=S3_FILE_KEY)
            excel_data = response['Body'].read()
            df = pd.read_excel(BytesIO(excel_data))
            logger.info(f"✅ Excel file loaded from S3 with {len(df)} existing transactions")
            return df
        except Exception as e:
            logger.warning(f"Excel file not found in S3, creating new: {e}")
            return pd.DataFrame(columns=['Date', 'Amount', 'C/D', 'From/To', 'Reason', 'message id', 'Balance'])
    
    def upload_excel_to_s3(self, df):
        """Upload Excel file to S3"""
        try:
            excel_buffer = BytesIO()
            df.to_excel(excel_buffer, index=False)
            excel_buffer.seek(0)
            
            self.s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=S3_FILE_KEY,
                Body=excel_buffer.getvalue(),
                ContentType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
            logger.info("✅ Excel file updated successfully in S3")
            return True
        except Exception as e:
            logger.error(f"❌ Error uploading Excel to S3: {e}")
            return False
    
    def calculate_new_balance(self, df, amount, transaction_type):
        """Calculate new balance based on previous balance and transaction"""
        if df.empty:
            previous_balance = 0.0
        else:
            previous_balance = float(df.iloc[-1]['Balance'])
        
        if transaction_type == 'CREDIT':
            new_balance = previous_balance + float(amount)
        else:
            new_balance = previous_balance - float(amount)
        
        return new_balance
    
    def extract_transaction_data(self, message_text, message_date, message_id):
        """Extract transaction details from bot message"""
        transaction_data = {
            'message_id': message_id,
            'date': message_date,
            'amount': None,
            'type': None,
            'from_to': None
        }
        
        # Amount patterns
        amount_patterns = [
            r'₹\s*(\d+(?:,\d+)*(?:\.\d{2})?)',
            r'Rs\.?\s*(\d+(?:,\d+)*(?:\.\d{2})?)',
            r'INR\s*(\d+(?:,\d+)*(?:\.\d{2})?)',
            r'(\d+(?:,\d+)*(?:\.\d{2})?)\s*(?:₹|Rs|INR)',
        ]
        
        for pattern in amount_patterns:
            match = re.search(pattern, message_text, re.IGNORECASE)
            if match:
                transaction_data['amount'] = match.group(1).replace(',', '')
                break
        
        # Transaction type
        text_lower = message_text.lower()
        if any(keyword in text_lower for keyword in ['credited', 'received', 'deposit', 'added', 'refund', 'cashback']):
            transaction_data['type'] = 'CREDIT'
        elif any(keyword in text_lower for keyword in ['debited', 'paid', 'withdrawn', 'sent', 'transfer', 'spent', 'charged']):
            transaction_data['type'] = 'DEBIT'
        
        # From/To patterns
        from_to_patterns = [
            r'(?:from|to|at)\s+([A-Z\s]+(?:[A-Z]{2,})?)',
            r'([A-Z][A-Z\s]+[A-Z])\s+(?:UPI|NEFT|IMPS)',
            r'UPI-([A-Z\s]+)',
            r'VPA:\s*([^\s]+)',
        ]
        
        for pattern in from_to_patterns:
            match = re.search(pattern, message_text, re.IGNORECASE)
            if match:
                transaction_data['from_to'] = match.group(1).strip()
                break
        
        return transaction_data
    
    async def process_and_update_transactions(self):
        """Main processing function - adds only TODAY's new transactions to ALL existing data"""
        try:
            # Authenticate
            if not await self.authenticate():
                logger.error("❌ Authentication failed, cannot proceed")
                return False
            
            # Read ALL existing data (preserves all historical transactions)
            df = self.read_excel_from_s3()
            today = date.today()
            
            # Collect only TODAY's bot messages
            bot_messages = {}
            async for message in self.client.iter_messages(BOT_ID, limit=1000):
                if (message.date.date() == today and message.sender_id == BOT_ID):
                    transaction_data = self.extract_transaction_data(message.text, message.date, message.id)
                    bot_messages[message.id] = {
                        'transaction': transaction_data,
                        'user_reason': None
                    }
            
            logger.info(f"Found {len(bot_messages)} bot messages from today ({today})")
            
            # Find user replies to TODAY's bot messages
            async for message in self.client.iter_messages(BOT_ID, limit=1000):
                if (message.date.date() == today and 
                    message.reply_to and 
                    message.reply_to.reply_to_msg_id in bot_messages and
                    message.sender_id != BOT_ID):
                    
                    bot_messages[message.reply_to.reply_to_msg_id]['user_reason'] = {
                        'date': message.date,
                        'text': message.text or '[Media/File]'
                    }
            
            if not bot_messages:
                logger.info(f"No transactions found for today ({today})")
                return True
            
            # Process and add only NEW transactions to existing data
            new_rows = []
            for msg_id, data in bot_messages.items():
                transaction = data['transaction']
                reason = data['user_reason']
                
                # Check for duplicates (transaction already exists in historical data)
                if not df.empty and msg_id in df['message id'].values:
                    logger.info(f"Transaction {msg_id} already exists, skipping")
                    continue
                
                amount = float(transaction['amount']) if transaction['amount'] else 0.0
                transaction_type = transaction['type'] or 'UNKNOWN'
                from_to = transaction['from_to'] or 'UNKNOWN'
                reason_text = reason['text'] if reason else 'No reason provided'
                
                new_balance = self.calculate_new_balance(df, amount, transaction_type)
                
                new_row = {
                    'Date': transaction['date'].strftime('%d-%m-%Y'),
                    'Amount': amount,
                    'C/D': transaction_type,
                    'From/To': from_to,
                    'Reason': reason_text,
                    'message id': msg_id,
                    'Balance': round(new_balance, 2)
                }
                
                new_rows.append(new_row)
                df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                
                logger.info(f"Added: ₹{amount} {transaction_type} - {from_to} - Balance: ₹{new_balance:.2f}")
            
            if new_rows:
                if self.upload_excel_to_s3(df):
                    logger.info(f"✅ Successfully added {len(new_rows)} new transactions")
                    logger.info(f"Total transactions in file: {len(df)}")
                    logger.info(f"Current Balance: ₹{df.iloc[-1]['Balance']:.2f}")
                    return True
                else:
                    logger.error("❌ Failed to upload to S3")
                    return False
            else:
                logger.info("No new transactions to add today")
                return True
                
        except Exception as e:
            logger.error(f"❌ Error in main processing: {e}")
            return False
    
    async def close(self):
        """Close connections"""
        try:
            await self.client.disconnect()
            logger.info("✅ Client disconnected")
        except Exception as e:
            logger.error(f"Error closing client: {e}")

# Continuous runner function
async def run_continuously():
    """Run the transaction processor continuously"""
    reader = TelegramTransactionReader(API_ID, API_HASH, PHONE_NUMBER)
    
    logger.info("🚀 Starting continuous transaction processor")
    logger.info(f"Check interval: {CHECK_INTERVAL} seconds")
    
    while True:
        try:
            logger.info("🔄 Starting transaction check cycle...")
            success = await reader.process_and_update_transactions()
            
            if success:
                logger.info("✅ Transaction check completed successfully")
            else:
                logger.error("❌ Transaction check failed")
                
        except Exception as e:
            logger.error(f"❌ Unexpected error in continuous loop: {e}")
        
        # Wait before next check
        logger.info(f"⏳ Waiting {CHECK_INTERVAL} seconds before next check...")
        await asyncio.sleep(CHECK_INTERVAL)

# Main execution
async def main():
    # Validate environment variables
    required_vars = ['API_ID', 'API_HASH', 'PHONE_NUMBER', 'AWS_ACCESS_KEY_ID', 
                    'AWS_SECRET_ACCESS_KEY', 'S3_BUCKET_NAME']
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"❌ Missing required environment variables: {missing_vars}")
        return
    
    logger.info("✅ All required environment variables found")
    
    try:
        await run_continuously()
    except KeyboardInterrupt:
        logger.info("👋 Shutting down gracefully...")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")

if __name__ == "__main__":
    asyncio.run(main())