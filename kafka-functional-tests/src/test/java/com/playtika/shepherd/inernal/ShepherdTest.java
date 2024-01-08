package com.playtika.shepherd.inernal;

import com.playtika.shepherd.BasicKafkaTest;
import com.playtika.shepherd.common.PastureListener;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static com.playtika.shepherd.inernal.CheckedHerd.checked;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;


public class ShepherdTest extends BasicKafkaTest {

    private static final Logger logger = LoggerFactory.getLogger(ShepherdTest.class);

    @Test
    public void shouldBalanceStaticHerd() {

        String groupId = "static-herd";

        ByteBuffer cow1 = ByteBuffer.wrap(new byte[]{1});
        ByteBuffer cow2 = ByteBuffer.wrap(new byte[]{0});

        Herd herd = checked(new Herd() {
            @Override
            public Population getPopulation() {
                return new Population(Set.of(cow1, cow2), -1);
            }

            @Override
            public void reset() {
            }
        });


        AtomicReference<List<ByteBuffer>> cows1 = new AtomicReference<>(List.of());
        PastureListener rebalanceListener1 = (population, version, generation, isLeader) -> {
            logger.info("Assigned cows1 [{}]", toArrays(population));
            cows1.set(population);
        };

        PastureShepherd herder1 = new PastureShepherdBuilder()
                .setBootstrapServers(kafka.getBootstrapServers())
                .setGroupId(groupId)
                .setHerd(herd)
                .setRebalanceListener(rebalanceListener1)
                .setProperties(TEST_PROPERTIES)
                .build();

        herder1.start();

        await().timeout(ofSeconds(5)).untilAsserted(() -> {
            assertThat(cows1.get()).containsExactlyInAnyOrder(cow1, cow2);
        });

        //setup another pasture
        AtomicReference<List<ByteBuffer>> cows2 = new AtomicReference<>(List.of());
        PastureListener rebalanceListener2 = (population, version, generation, isLeader) -> {
            logger.info("Assigned cows2 [{}]", toArrays(population));
            cows2.set(population);
        };

        PastureShepherd herder2 = new PastureShepherdBuilder()
                .setBootstrapServers(kafka.getBootstrapServers())
                .setGroupId(groupId)
                .setHerd(herd)
                .setRebalanceListener(rebalanceListener2)
                .setProperties(TEST_PROPERTIES)
                .build();
        herder2.start();

        await().timeout(ofSeconds(5)).untilAsserted(() -> {
            assertThat(cows1.get().size()).isEqualTo(1);
            assertThat(cows2.get().size()).isEqualTo(1);
        });

        //stop first pasture
        herder1.stop(Duration.ofSeconds(10).toMillis());

        await().timeout(ofSeconds(3)).untilAsserted(() -> {
            assertThat(cows2.get()).containsExactlyInAnyOrder(cow1, cow2);
        });

    }

    @Test
    public void shouldBalanceDynamicHerd() {

        String groupId = "dynamic-group";

        ByteBuffer cow1 = ByteBuffer.wrap(new byte[]{1});
        ByteBuffer cow2 = ByteBuffer.wrap(new byte[]{0});
        Set<ByteBuffer> population = ConcurrentHashMap.newKeySet();
        population.addAll(List.of(cow1, cow2));
        AtomicInteger version = new AtomicInteger(1);

        Herd herd = checked(new Herd() {
            @Override
            public Population getPopulation() {
                return new Population(population, version.get());
            }

            @Override
            public void reset() {
            }
        });


        AtomicReference<List<ByteBuffer>> cows1 = new AtomicReference<>(List.of());
        PastureListener rebalanceListener1 = new PastureListener() {
            @Override
            public void assigned(List<ByteBuffer> population, int version, int generation, boolean isLeader) {
                logger.info("Assigned cows1 [{}]", toArrays(population));
                cows1.set(population);
            }
        };

        PastureShepherd herder1 = new PastureShepherdBuilder()
                .setBootstrapServers(kafka.getBootstrapServers())
                .setGroupId(groupId)
                .setHerd(herd)
                .setRebalanceListener(rebalanceListener1)
                .setProperties(TEST_PROPERTIES)
                .build();

        AtomicReference<List<ByteBuffer>> cows2 = new AtomicReference<>(List.of());
        PastureListener rebalanceListener2 = new PastureListener() {
            @Override
            public void assigned(List<ByteBuffer> population, int version, int generation, boolean isLeader) {
                logger.info("Assigned cows2 [{}]", toArrays(population));
                cows2.set(population);
            }
        };

        PastureShepherd herder2 = new PastureShepherdBuilder()
                .setBootstrapServers(kafka.getBootstrapServers())
                .setGroupId(groupId)
                .setHerd(herd)
                .setRebalanceListener(rebalanceListener2)
                .setProperties(TEST_PROPERTIES)
                .build();

        herder1.start();
        herder2.start();

        await().timeout(ofSeconds(5)).untilAsserted(() -> {
            assertThat(cows1.get().size()).isEqualTo(1);
            assertThat(cows2.get().size()).isEqualTo(1);
            assertThat(Stream.concat(cows1.get().stream(), cows2.get().stream()).toList()).containsExactlyInAnyOrder(cow1, cow2);
        });

        //add cows to herd
        ByteBuffer cow3 = ByteBuffer.wrap(new byte[]{2});
        ByteBuffer cow4 = ByteBuffer.wrap(new byte[]{3});

        population.addAll(List.of(cow3, cow4));
        version.set(2);

        Stream.of(herder1, herder2).filter(PastureShepherd::isLeader).forEach(PastureShepherd::setNeedsReconfigRebalance);

        await().timeout(ofSeconds(5)).untilAsserted(() -> {
            assertThat(cows1.get().size()).isEqualTo(2);
            assertThat(cows2.get().size()).isEqualTo(2);
            assertThat(Stream.concat(cows1.get().stream(), cows2.get().stream()).toList()).containsExactlyInAnyOrder(cow1, cow2, cow3, cow4);
        });

        //removed cows from herd
        population.removeAll(List.of(cow1, cow2));
        version.set(3);

        Stream.of(herder1, herder2).filter(PastureShepherd::isLeader).forEach(PastureShepherd::setNeedsReconfigRebalance);

        await().timeout(ofSeconds(5)).untilAsserted(() -> {
            assertThat(cows1.get().size()).isEqualTo(1);
            assertThat(cows2.get().size()).isEqualTo(1);
            assertThat(Stream.concat(cows1.get().stream(), cows2.get().stream()).toList()).containsExactlyInAnyOrder(cow3, cow4);
        });
    }

}
