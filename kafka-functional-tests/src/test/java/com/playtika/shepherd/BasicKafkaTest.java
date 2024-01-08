package com.playtika.shepherd;

import com.playtika.shepherd.inernal.DistributedConfig;
import com.playtika.shepherd.inernal.utils.BytesUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class BasicKafkaTest {

    protected static final Map<String, String> TEST_PROPERTIES = Map.of(
            DistributedConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "500",
            DistributedConfig.SESSION_TIMEOUT_MS_CONFIG, "1500"
    );

    protected static KafkaContainer kafka;

    @BeforeAll
    public static void setUp(){
        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
                .withEnv("KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS", "400");
        kafka.start();
    }

    @AfterAll
    public static void tearDown(){
        kafka.stop();
    }

    public static List<byte[]> toArrays(List<ByteBuffer> buffers){
        return buffers.stream().map(BytesUtils::getBytes).toList();
    }
}
