import com.yang.kafka.demo.KafkaDemoApplication;
import com.yang.kafka.demo.demo4.SeekOffset;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.SpringApplication;

import java.time.Duration;

/**
 * @author JMWANG
 */
public class OffsetTest {

    @Before
    public void StBefore() {
        String[] args = new String[0];
        SpringApplication.run(KafkaDemoApplication.class, args);
    }

    @Test
    public void setOffset() throws InterruptedException {
        SeekOffset.SingleCase(2);

        while (true) {
            ConsumerRecords<String, String> records = SeekOffset.getConsumer().poll(Duration.ofMillis(100));

            if (records.count() > 0) {
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record);
                }
            }

            Thread.sleep(1000);

            SeekOffset.getConsumer().commitAsync();
        }
    }
}
