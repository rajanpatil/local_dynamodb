package com.local.dynamodb;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;

public class App {
    private static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "eu-west-1"))
            .build();
    private static DynamoDB dynamoDB = new DynamoDB(client);

    private static String productCatalogTableName = "ProductCatalog";

    public static void main(String[] args) {

        LocalDynamodbUtils localDynamodbUtils = new LocalDynamodbUtils(dynamoDB);

        try {
            localDynamodbUtils.deleteTable(productCatalogTableName);
            localDynamodbUtils.createTable(productCatalogTableName, 10L, 5L, "Id", "N");
            localDynamodbUtils.loadSampleProducts(productCatalogTableName);

        } catch (Exception e) {
            System.err.println("Program failed:");
            System.err.println(e.getMessage());
        }
        System.out.println("Success.");
    }

}
