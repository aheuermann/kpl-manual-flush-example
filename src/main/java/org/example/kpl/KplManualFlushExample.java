package org.example.kpl;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class KplManualFlushExample {
    private static final Logger LOGGER = LoggerFactory.getLogger(KplManualFlushExample.class);
    static final int batchSize = 3000;
    static final String stream = "test-stream";
    static final int messageLength = 128;

    public static void main(String[] args) {
        KinesisProducer producer = buildKPL();
        long batchCount = 0;
        long sequenceNumber = 0;
        while (true) {
            ++batchCount;
            long startTime = System.currentTimeMillis();

            List<Message> msgs = new ArrayList<>();
            LOGGER.info("Generating batch of user records [batchNumber={}, size={}]", ++batchCount, batchSize);
            for (int i = 0; i <= batchSize; ++i) {
                ++sequenceNumber;
                msgs.add(new Message(sequenceNumber, generateData(sequenceNumber, messageLength)));
            }

            // add the user records
            List<ListenableFuture<UserRecordResult>> futures = new ArrayList<>();
            for (Message msg : msgs) {
                futures.add(producer.addUserRecord(stream, msg.partitionKey, msg.bytes));
            }
            LOGGER.info("Done adding user records, flushing [batchNumber={}, size={}]", ++batchCount, futures.size());
            // Call flush to force publish of messages.
            // Expected this to trigger all messages to be published, but some messages are still remain in the bugger and don't get published until the RecordMaxBufferedTime is met.
            producer.flush();
            try {
                ListenableFuture<List<UserRecordResult>> allFutures = Futures.allAsList(futures);
                allFutures.get();
                LOGGER.info("Done publishing batch [batchNumber={}, size={}, durationMs={}]", ++batchCount, futures.size(), System.currentTimeMillis() - startTime);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                break;
            }
        }
    }

    private static class Message {
        public String partitionKey;
        public ByteBuffer bytes;

        public Message(long sequenceNumber, ByteBuffer bytes) {
            this.partitionKey = "" + sequenceNumber;
            this.bytes = bytes;
        }
    }

    public static ByteBuffer generateData(long sequenceNumber, int totalLen) {
        StringBuilder sb = new StringBuilder();
        sb.append(Long.toString(sequenceNumber));
        sb.append(" ");
        while (sb.length() < totalLen) {
            sb.append("a");
        }
        try {
            return ByteBuffer.wrap(sb.toString().getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    private static KinesisProducer buildKPL() {
        KinesisProducerConfiguration config = new KinesisProducerConfiguration()
                .setRegion("us-west-2")
                .setAggregationEnabled(true)
                .setVerifyCertificate(false)
                .setRecordMaxBufferedTime(60000L)
                .setRecordTtl(Integer.MAX_VALUE); // set to max value so we don't discard records if they fail to publish to kinesis
        return new KinesisProducer(config);
    }
}
