package com.jy.dynamodb.exercise;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.jy.dynamodb.exercise.config.DynamoDBConfig;
import com.jy.dynamodb.exercise.domain.CommentRepository;
import com.jy.dynamodb.exercise.domain.Comment;
import com.jy.dynamodb.exercise.exception.CommentNotFoundException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.BDDAssertions.*;

@SpringBootTest(classes = {DynamoDBConfig.class})
public class CommentRepositoryTest {

    @Autowired
    private CommentRepository commentRepository;

    @Autowired
    private AmazonDynamoDB amazonDynamoDb;

    @Autowired
    private DynamoDBMapper dynamoDbMapper;

    // TODO: It is weired to create and delete table for each test. Refactor it somehow...
    @BeforeEach
    void createTable() {
        CreateTableRequest createTableRequest = dynamoDbMapper.generateCreateTableRequest(Comment.class)
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L));

        createTableRequest.getGlobalSecondaryIndexes().forEach(
                idx -> idx
                        .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                        .withProjection(new Projection().withProjectionType("ALL"))
        );
        TableUtils.createTableIfNotExists(amazonDynamoDb, createTableRequest);
    }

    @AfterEach
    void deleteTable() {
        DeleteTableRequest deleteTableRequest = dynamoDbMapper.generateDeleteTableRequest(Comment.class);
        TableUtils.deleteTableIfExists(amazonDynamoDb, deleteTableRequest);
    }

    @Test
    void findCreatedComment_ById_FoundComment() {
        // given
        String id = commentRepository.save(Comment.builder()
                .mentionId(1)
                .name("name")
                .content("content").build()
        ).getId();

        // when
        Comment createdComment = commentRepository.findById(id)
                .orElseThrow(() -> new CommentNotFoundException(id));

        // then
        then(createdComment)
                .hasNoNullFieldsOrPropertiesExcept("deletedAt")
                .hasFieldOrPropertyWithValue("mentionId", 1)
                .hasFieldOrPropertyWithValue("name", "name")
                .hasFieldOrPropertyWithValue("content", "content");
    }

    @Test
    void updateComment_ValidInput_UpdatedComment() {
        // given
        String id = commentRepository.save(Comment.builder()
                .mentionId(2)
                .name("name2")
                .content("content2").build()
        ).getId();

        // when
        Comment foundComment = commentRepository.findById(id)
                .orElseThrow(() -> new CommentNotFoundException(id));

        foundComment.update("updated content");
        Comment modifiedComment = commentRepository.save(foundComment);

        //then

        // then
        then(modifiedComment)
                .hasNoNullFieldsOrPropertiesExcept("deletedAt")
                .hasFieldOrPropertyWithValue("mentionId", 2)
                .hasFieldOrPropertyWithValue("name", "name2")
                .hasFieldOrPropertyWithValue("content", "updated content");

    }

    @Test
    void deleteCreatedComment_TryToFindDeletedComment_ThrowCommentNotFoundException() {
        // given
        Comment createdComment = commentRepository.save(Comment.builder()
                .mentionId(1)
                .name("name")
                .content("content").build());

        // when
        commentRepository.delete(createdComment);

        // then
        thenThrownBy(() -> commentRepository.findById(createdComment.getId())
                .orElseThrow(() -> new CommentNotFoundException(createdComment.getId())))
                .isExactlyInstanceOf(CommentNotFoundException.class);
    }

    @Test
    void findComments_ByMentionIdAndOrderByCreatedAtDescDeletedFalse_FoundCommentsInDesignatedOrder() {
        // given
        int size = 10;

        IntStream.range(0, size).forEach(i -> commentRepository.save(Comment.builder()
                .mentionId(1)
                .name("name " + i)
                .content("content " + i).build()));

        // when
        List<Comment> foundComment = commentRepository
                .findAllByMentionIdOrderByCreatedAtAsc(1);

        // then
        then(foundComment.size()).isEqualTo(size);

        IntStream.range(1, size).forEach(i -> {
            Comment prev = foundComment.get(i - 1);
            Comment next = foundComment.get(i);
            then(prev.getCreatedAt().isBefore(next.getCreatedAt())).isTrue();
        });
    }

}
