import sys
import boto3
import time

""" Program to get DDL of all tables in an Athena DB """


def waitForQuery(client, query_execution_response):
    time.sleep(10)
    while (client.get_query_execution(QueryExecutionId=query_execution_response['QueryExecutionId'])['QueryExecution'][
               'Status']['State'] in ('QUEUED', 'RUNNING')):
        time.sleep(10)


def main(argv):
    db_name = argv[1]
    s3_output_loc_dict = {'OutputLocation': argv[2]}
    client = boto3.client('athena')

    query_execution_response1 = client.start_query_execution(QueryString=''' SELECT table_name
                                                                             FROM INFORMATION_SCHEMA.TABLES
                                                                             WHERE table_schema=\'''' + db_name + '''\'''',
                                                             ResultConfiguration=s3_output_loc_dict)
    waitForQuery(client, query_execution_response1)
    query_results1 = client.get_query_results(QueryExecutionId=query_execution_response1['QueryExecutionId'])

    for Row in query_results1['ResultSet']['Rows']:
        table_name = Row['Data'][0]['VarCharValue']
        query_execution_response2 = client.start_query_execution(
            QueryString='show create table ' + db_name + '.' + table_name,
            ResultConfiguration=s3_output_loc_dict)
        waitForQuery(client, query_execution_response2)
        query_results2 = client.get_query_results(QueryExecutionId=query_execution_response2['QueryExecutionId'])
        if ('ResultSet' in query_results2):
            print()
            for Row in query_results2['ResultSet']['Rows']:
                print(Row['Data'][0]['VarCharValue'])


main(sys.argv)
