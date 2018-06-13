# local_dynamodb
This repo is example of using local dynamodb

## Setup local dynamodb on your development machine

Run local dynamodb as follows:
`java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -sharedDb`

Reference for more information: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html

## AWS CLI can be used to connect to local dynamodb

Local dynamodb usually runs on port `8000` unless you change it while running it locally.

You can use aws cli to make queries, you have to use additional option `--endpoint-url http://localhost:8000`

i.e: `aws dynamodb list-tables --endpoint-url http://localhost:8000`