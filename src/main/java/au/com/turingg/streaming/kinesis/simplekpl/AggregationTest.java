package au.com.turingg.streaming.kinesis.simplekpl;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * @author Behrang Saeedzadeh
 */
public class AggregationTest {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final KinesisProducerConfiguration conf = new KinesisProducerConfiguration();

        // Disable collection but enable aggregation
        conf.setCollectionMaxCount(1);
        conf.setAggregationEnabled(true);
        conf.setRecordMaxBufferedTime(10_000);

        conf.setMetricsLevel("none");
        conf.setLogLevel("info");
        conf.setConnectTimeout(10_000);
        conf.setRegion("ap-southeast-2");
        conf.setCredentialsProvider(new DefaultAWSCredentialsProviderChain());

        final KinesisProducer producer = new KinesisProducer(conf);

        final List<ListenableFuture<UserRecordResult>> results = new ArrayList<>();

        final String streamName = "TestAggregationStream";
        final String data = "<some data>";
        final String pk = "<some pk>";

        // Add 10 user records. As Record Max Buffered Time is reasonably
        // large (10 seconds) and all the user records have the same partition key,
        // I am expecting all of them to be bundled into 1 Kinesis record, hence
        // requiring only 1 call to the `PutRecords` API
        for (int i = 0; i < 10; i++) {
            final ListenableFuture<UserRecordResult> result = producer.addUserRecord(
                    streamName,
                    pk,
                    ByteBuffer.wrap(data.getBytes())
            );

            results.add(result);
        }


        final List<UserRecordResult> userRecords = results.stream().map(r -> {
            try {
                return r.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());

        userRecords.forEach(r -> System.out.println(ToStringBuilder.reflectionToString(r)));

        final List<String> sequenceNumbers = userRecords.stream().map(UserRecordResult::getSequenceNumber).collect(Collectors.toList());
        Collections.sort(sequenceNumbers);
        System.out.println(sequenceNumbers);

        Thread.sleep(15_000);
    }

}
