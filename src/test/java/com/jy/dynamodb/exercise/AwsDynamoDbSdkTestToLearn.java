package com.jy.dynamodb.exercise;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.util.TableUtils;

import com.jy.dynamodb.exercise.domain.Comment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;


public class AwsDynamoDbSdkTestToLearn {

    private AmazonDynamoDB amazonDynamoDb;

    private DynamoDBMapper dynamoDbMapper;

    private Comment comment;

    @BeforeEach
    void setUp() {
        AWSCredentials awsCredentials = new BasicAWSCredentials("key1", "key2");
        AWSCredentialsProvider awsCredentialsProvider = new AWSStaticCredentialsProvider(awsCredentials);
        AwsClientBuilder.EndpointConfiguration endpointConfiguration = new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "ap-northeast-2");

        amazonDynamoDb = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(awsCredentialsProvider)
                .withEndpointConfiguration(endpointConfiguration).build();

        dynamoDbMapper = new DynamoDBMapper(amazonDynamoDb, DynamoDBMapperConfig.DEFAULT);

        comment = Comment.builder()
                .name("name")
                .mentionId(1)
                .content("content")
                .build();
    }


    @Test
    @Disabled
    public void createTable_ValidInput_TableHasBeenCreated() {
        CreateTableRequest createTableRequest = (new CreateTableRequest())
                .withAttributeDefinitions(
                        new AttributeDefinition("id", ScalarAttributeType.S),
                        new AttributeDefinition("mentionId", ScalarAttributeType.N),
                        new AttributeDefinition("createdAt", ScalarAttributeType.S)
                ).withTableName("Comment")
                .withKeySchema(new KeySchemaElement("id", KeyType.HASH))
                .withGlobalSecondaryIndexes(
                        (new GlobalSecondaryIndex())
                        .withIndexName("byMentionId")
                        .withKeySchema(
                                new KeySchemaElement("mentionId",  KeyType.HASH),
                                new KeySchemaElement("createdAt", KeyType.RANGE)
                        ).withProjection(
                                (new Projection()).withProjectionType(ProjectionType.ALL)
                        ).withProvisionedThroughput(
                                new ProvisionedThroughput(1L, 1L))
                ).withProvisionedThroughput(
                        new ProvisionedThroughput(1L, 1L)
                );

        TableUtils.createTableIfNotExists(amazonDynamoDb, createTableRequest);
    }

    @Test
    @Disabled
    public void putItem_ShouldBeCalledAfterTableCreation_StatusOk() {
        Map<String, AttributeValue> item = new HashMap<>();

        item.put("id", (new AttributeValue()).withS("uuid"));
        item.put("mentionId", (new AttributeValue()).withN("1"));
        item.put("content", (new AttributeValue().withS("comment content")));
        item.put("deleted", (new AttributeValue().withBOOL(false)));
        item.put("createdAt", (new AttributeValue().withS("1836-03-07T02:21:30.536Z")));

        PutItemRequest putItemRequest = (new PutItemRequest())
                                        .withTableName("Comment")
                                        .withItem(item);

        PutItemResult putItemResult = amazonDynamoDb.putItem(putItemRequest);

        System.out.println(putItemResult.getSdkHttpMetadata().getHttpStatusCode());


    }

    @Test
    @Disabled
    void getItem_ShouldBeCalledAfterPuttingItem_FoundItem() {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", (new AttributeValue()).withS("uuid"));

        GetItemRequest getItemRequest = (new GetItemRequest())
                                        .withTableName("Comment")
                                        .withKey(key);

        GetItemResult getItemResult = amazonDynamoDb.getItem(getItemRequest);

        System.out.println(getItemResult.getItem().get("id"));
    }

    @Test
    @Disabled
    void deleteItem_ShouldBeCalledAfterPuttingItem_StatusOk() {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", (new AttributeValue()).withS("uuid"));

        DeleteItemRequest deleteItemRequest = (new DeleteItemRequest())
                .withTableName("Comment")
                .withKey(key);

        DeleteItemResult deleteItemResult = amazonDynamoDb.deleteItem(deleteItemRequest);

        System.out.println(deleteItemResult.getSdkHttpMetadata().getHttpStatusCode());
    }

    @Test
    @Disabled
    void createTable_ValidInput_TableHasBeenCreated_2() {
        CreateTableRequest createTableRequest = dynamoDbMapper.generateCreateTableRequest(Comment.class)
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L));

        createTableRequest.getGlobalSecondaryIndexes().forEach(
                idx -> idx.withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                          .withProjection(new Projection().withProjectionType("ALL"))
        );

        System.out.println(TableUtils.createTableIfNotExists(amazonDynamoDb, createTableRequest));
    }

    @Test
    @Disabled
    void saveItem_ShouldBeCalledAfterTableCreation_IdIsNotNull() {


        System.out.println(comment.getId());

        dynamoDbMapper.save(comment);

        System.out.println(comment.getId());

    }

    @Test
    @Disabled
    void deleteTable_ShouldBeCalledAfterTableCreation_TableHasBeenCreated() {
        DeleteTableRequest deletetableRequest = dynamoDbMapper.generateDeleteTableRequest(Comment.class);
        TableUtils.deleteTableIfExists(amazonDynamoDb, deletetableRequest);

    }

    @Test
    @Disabled
    void saveAndLoadItem_ShouldBeCalledAfterTableCreation_FoundItem() {
        dynamoDbMapper.save(comment);

        Comment foundComment = dynamoDbMapper.load(Comment.class, comment.getId());

        System.out.println(foundComment.getContent());

    }

}
