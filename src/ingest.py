import asyncio
import sys
import duckdb
import pandas as pd
import botocore.exceptions
from aiobotocore.session import get_session
from datetime import datetime
from loguru import logger

class ingest:
    def __init__(self) -> None:
        self.queue_name = 'test-queue'
        self.endpoint_url = 'http://interview-localstack:4566'
        self.con = duckdb.connect(database='/home/app/duckdb_artifacts/bayzat')

    async def go(self):
        session = get_session()
        async with session.create_client('sqs', endpoint_url = self.endpoint_url) as client:
            try:
                response = await client.get_queue_url(QueueName=self.queue_name)
            except botocore.exceptions.ClientError as err:
                if (
                    err.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue'
                ):
                    logger.error(f"Queue {self.queue_name} does not exist")
                    sys.exit(1)
                else:
                    raise

            queue_url = response['QueueUrl']
            li=[]
            pd_df=pd.DataFrame()
            count = 0
            logger.info('Pulling messages off the queue...')
            start = datetime.now()
            while True:

                try:
                    # poll for messages from the queue at a 2 sec wait
                    response = await client.receive_message(
                        QueueUrl=queue_url,
                        WaitTimeSeconds=2,
                    )

                    if 'Messages' in response:
                        for msg in response['Messages']:
                            li.append(msg)
                            count += 1
                            # Need to remove msg from queue or else it'll reappear
                            await client.delete_message(
                                QueueUrl=queue_url,
                                ReceiptHandle=msg['ReceiptHandle'],
                            )
                            # Flushing messages to database at a 50k record count 
                            if count == 50000:
                                pd_df = pd.DataFrame(li)
                                self.con.execute('insert into message_landing select *, current_timestamp from pd_df')
                                logger.info(f'Fetched and flushed {count} messages in {datetime.now()-start}')
                                count = 0
                                li=[]
                                pd_df=pd.DataFrame()
                                start = datetime.now()
                    else:
                        if li:
                            pd_df = pd.DataFrame(li)
                            self.con.execute('insert into message_landing select *, current_timestamp from pd_df')
                            logger.info(f'Fetched and flushed {count} messages in {datetime.now()-start}')
                            count = 0
                            li = []
                            pd_df = pd.DataFrame()
                        logger.info('No messages in queue')
                        break       # remove if more messages are expected
                except KeyboardInterrupt:
                    if li:
                            pd_df = pd.DataFrame(li)
                            self.con.execute('insert into message_landing select *, current_timestamp from pd_df')
                            logger.info(f'Flushed {count} messages...')
                    break

            logger.info('Finished')

    def create_table(self):
        logger.info('Creating landing table in duckdb...')
        self.con.execute("""create table if not exists message_landing (
            message_id varchar,
            receipt_handle varchar,
            md5_of_body varchar,
            body varchar,
            _insert_ts timestamptz
        )""")

if __name__ == '__main__':
    ing = ingest()
    ing.create_table()
    asyncio.run(ing.go())