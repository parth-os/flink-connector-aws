package org.apache.flink.connector.dynamodb.source.proxy;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.connector.dynamodb.source.config.ConsumerConfigConstants;
import org.apache.flink.connector.dynamodb.source.split.StartingPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DynamoDbStreamsProxy implements StreamProxy {

    private final DynamoDbStreamsClient dynamoDbStreamsClient;
    private final SdkHttpClient httpClient;
    private final Map<String, String> shardIdToIteratorStore;

    private static final FullJitterBackoff BACKOFF = new FullJitterBackoff();

    private static final Logger LOG = LoggerFactory.getLogger(DynamoDbStreamsProxy.class);

    public DynamoDbStreamsProxy(DynamoDbStreamsClient dynamoDbStreamsClient, SdkHttpClient httpClient) {
        this.dynamoDbStreamsClient = dynamoDbStreamsClient;
        this.httpClient = httpClient;
        this.shardIdToIteratorStore = new ConcurrentHashMap<>();
    }

    @Override
    public List<Shard> listShards(String streamArn, @Nullable String lastSeenShardId) {
        return this.getShardsOfStream(streamArn, lastSeenShardId);
    }

    @Override
    public GetRecordsResponse getRecords(String streamArn, String shardId, StartingPosition startingPosition) {
        String shardIterator =
                shardIdToIteratorStore.computeIfAbsent(
                        shardId, (s) -> getShardIterator(streamArn, s, startingPosition));

        try {
            GetRecordsResponse getRecordsResponse = getRecords(shardIterator);
            if (getRecordsResponse.nextShardIterator() != null) {
                shardIdToIteratorStore.put(shardId, getRecordsResponse.nextShardIterator());
            }
            return getRecordsResponse;
        } catch (ExpiredIteratorException e) {
            // Eagerly retry getRecords() if the iterator is expired
            shardIterator = getShardIterator(streamArn, shardId, startingPosition);
            GetRecordsResponse getRecordsResponse = getRecords(shardIterator);
            if (getRecordsResponse.nextShardIterator() != null) {
                shardIdToIteratorStore.put(shardId, getRecordsResponse.nextShardIterator());
            }
            return getRecordsResponse;
        }
    }

    @Override
    public void close() throws IOException {
        dynamoDbStreamsClient.close();
        httpClient.close();
    }

    private List<Shard> getShardsOfStream(
            String streamName, @Nullable String lastSeenShardId) {
        List<Shard> shardsOfStream = new ArrayList<>();

        DescribeStreamResponse describeStreamResponse;
        do {
            describeStreamResponse = this.describeStream(streamName, lastSeenShardId);
            List<Shard> shards = describeStreamResponse.streamDescription().shards();
            for (Shard shard : shards) {
                shardsOfStream.add(shard);
            }

            if (shards.size() != 0) {
                lastSeenShardId = shards.get(shards.size() - 1).shardId();
            }
        } while (!CollectionUtils.isEmpty(describeStreamResponse.streamDescription().shards()));

        return shardsOfStream;
    }

    private DescribeStreamResponse describeStream(String streamArn, @Nullable String startShardId) {
        final DescribeStreamRequest describeStreamRequest = DescribeStreamRequest.builder()
                .streamArn(streamArn)
                .exclusiveStartShardId(startShardId)
                .build();;

        DescribeStreamResponse describeStreamResponse = null;

        // Call DescribeStream, with full-jitter backoff (if we get LimitExceededException).
        int attemptCount = 0;
        while (describeStreamResponse == null) { // retry until we get a result
            try {
                describeStreamResponse = dynamoDbStreamsClient.describeStream(describeStreamRequest);
            } catch (LimitExceededException le) {
                long backoffMillis =
                        BACKOFF.calculateFullJitterBackoff(
                                // TODO: Make this configurable by client
                                ConsumerConfigConstants.DEFAULT_STREAM_DESCRIBE_BACKOFF_BASE,
                                ConsumerConfigConstants.DEFAULT_STREAM_DESCRIBE_BACKOFF_MAX,
                                ConsumerConfigConstants.DEFAULT_STREAM_DESCRIBE_BACKOFF_EXPONENTIAL_CONSTANT,
                                attemptCount++);
                LOG.warn(
                        String.format(
                                "Got LimitExceededException when describing stream %s. "
                                        + "Backing off for %d millis.",
                                streamArn, backoffMillis));
                BACKOFF.sleep(backoffMillis);
            } catch (ResourceNotFoundException re) {
                throw new RuntimeException("Error while getting stream details", re);
            }
        }

        StreamStatus streamStatus = describeStreamResponse.streamDescription().streamStatus();
        if (!(streamStatus.equals(StreamStatus.DISABLED)
                || streamStatus.equals(StreamStatus.ENABLING.toString()))) {
            if (LOG.isWarnEnabled()) {
                LOG.warn(
                        String.format(
                                "The status of stream %s is %s ; result of the current "
                                        + "describeStream operation will not contain any shard information.",
                                streamArn, streamStatus));
            }
        }

        return describeStreamResponse;
    }

    private String getShardIterator(String streamArn, String shardId, StartingPosition startingPosition) {
        GetShardIteratorRequest.Builder requestBuilder =
                GetShardIteratorRequest.builder()
                        .streamArn(streamArn)
                        .shardId(shardId)
                        .shardIteratorType(startingPosition.getShardIteratorType());

        switch (startingPosition.getShardIteratorType()) {
            case TRIM_HORIZON:
            case LATEST:
                break;
            case AT_SEQUENCE_NUMBER:
            case AFTER_SEQUENCE_NUMBER:
                if (startingPosition.getStartingMarker() instanceof String) {
                    requestBuilder =
                            requestBuilder.sequenceNumber(
                                    (String) startingPosition.getStartingMarker());
                } else {
                    throw new IllegalArgumentException(
                            "Invalid object given for GetShardIteratorRequest() when ShardIteratorType is AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER. Must be a String.");
                }
        }

        return dynamoDbStreamsClient.getShardIterator(requestBuilder.build()).shardIterator();
    }

    private GetRecordsResponse getRecords(String shardIterator) {
        return dynamoDbStreamsClient.getRecords(
                GetRecordsRequest.builder()
                        .shardIterator(shardIterator)
                        .build());
    }
}
