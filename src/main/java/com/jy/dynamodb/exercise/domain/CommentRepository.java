package com.jy.dynamodb.exercise.domain;

import org.springframework.data.repository.PagingAndSortingRepository;

import java.util.List;

public interface CommentRepository extends PagingAndSortingRepository<Comment, String> {

    List<Comment> findAllByMentionIdOrderByCreatedAtAsc(Integer mentionId);
}
