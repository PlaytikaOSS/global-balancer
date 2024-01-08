package com.playtika.shepherd;

import com.playtika.shepherd.common.Pasture;
import com.playtika.shepherd.common.Shepherd;
import com.playtika.shepherd.inernal.ShepherdTest;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

public class KafkaFarmTest extends BasicKafkaTest{

    private static final Logger logger = LoggerFactory.getLogger(ShepherdTest.class);

    @Test
    public void shouldBalanceStaticHerd() {

        KafkaFarm kafkaRanch = new KafkaFarm(kafka.getBootstrapServers(), TEST_PROPERTIES);

        ByteBuffer cow1 = ByteBuffer.wrap(new byte[]{1});
        ByteBuffer cow2 = ByteBuffer.wrap(new byte[]{0});

        ByteBuffer[] cows = new ByteBuffer[]{cow1, cow2};

        AtomicReference<List<ByteBuffer>> cows1 = new AtomicReference<>(List.of());

        Pasture pasture1 = kafkaRanch.addPasture("static-herd", (population, version, generation, isLeader) -> {
            logger.info("Assigned cows1 [{}]", toArrays(population));
            cows1.set(population);
        });

        pasture1.getShepherd().setPopulation(cows, -1);

        await().timeout(ofSeconds(5)).untilAsserted(() -> {
            assertThat(cows1.get()).containsExactlyInAnyOrder(cow1, cow2);
        });

        //setup another pasture
        AtomicReference<List<ByteBuffer>> cows2 = new AtomicReference<>(List.of());
        Pasture pasture2 = kafkaRanch.addPasture("static-herd", (population, version, generation, isLeader) -> {
            logger.info("Assigned cows2 [{}]", toArrays(population));
            cows2.set(population);
        });
        pasture2.getShepherd().setPopulation(cows, -1);

        await().timeout(ofSeconds(5)).untilAsserted(() -> {
            assertThat(cows1.get().size()).isEqualTo(1);
            assertThat(cows2.get().size()).isEqualTo(1);
        });

        //setup third pasture
        AtomicReference<List<ByteBuffer>> cows3 = new AtomicReference<>(List.of());
        Shepherd shepherd3 = kafkaRanch.addPasture("static-herd", (population, version, generation, isLeader) -> {
            logger.info("Assigned cows3 [{}]", toArrays(population));
            cows3.set(population);
        }).getShepherd();
        shepherd3.setPopulation(cows, -1);

        await().timeout(ofSeconds(5)).untilAsserted(() -> {
            assertThat(Stream.of(cows1.get().stream(), cows2.get().stream(), cows3.get().stream())
                    .flatMap(byteBufferStream -> byteBufferStream).toList())
                    .containsExactlyInAnyOrder(cow1, cow2);
        });

        //stop first 2 pastures
        pasture1.close(Duration.ofSeconds(10));
        pasture2.close(Duration.ofSeconds(10));

        await().timeout(ofSeconds(3)).untilAsserted(() -> {
            assertThat(cows3.get()).containsExactlyInAnyOrder(cow1, cow2);
        });
    }

    @Test
    public void shouldBalanceDynamicHerd() {
        shouldBalanceDynamicHerd(false);
    }

    @Test
    public void shouldBalanceDynamicHerdVersioned() {
        shouldBalanceDynamicHerd(true);
    }

    private int nextVersion(int version, boolean versioned){
        return versioned ? ++version : -1;
    }

    private void shouldBalanceDynamicHerd(boolean versioned) {

        KafkaFarm kafkaRanch = new KafkaFarm(kafka.getBootstrapServers(), TEST_PROPERTIES);

        ByteBuffer cow1 = ByteBuffer.wrap(new byte[]{1});
        ByteBuffer cow2 = ByteBuffer.wrap(new byte[]{0});
        ByteBuffer[] cows = new ByteBuffer[]{cow1, cow2};
        int ver1 = nextVersion(0, versioned);

        AtomicReference<List<ByteBuffer>> cows1 = new AtomicReference<>(List.of());
        AtomicInteger version1 = new AtomicInteger();
        String herdName = versioned ? "dynamic-group-versioned" : "dynamic-group";
        Shepherd shepherd1 = kafkaRanch.addPasture(herdName, (population, version, generation, isLeader) -> {
            logger.info("Assigned cows1 [{}]", toArrays(population));
            cows1.set(population);
            version1.set(version);
        }).getShepherd();
        shepherd1.setPopulation(cows, ver1);

        AtomicReference<List<ByteBuffer>> cows2 = new AtomicReference<>(List.of());
        AtomicInteger version2 = new AtomicInteger();
        Shepherd shepherd2 = kafkaRanch.addPasture(herdName, (population, version, generation, isLeader) -> {
            logger.info("Assigned cows2 [{}]", toArrays(population));
            cows2.set(population);
            version2.set(version);
        }).getShepherd();
        shepherd2.setPopulation(cows, ver1);

        await().timeout(ofSeconds(5)).untilAsserted(() -> {
            assertThat(version1.get()).isEqualTo(ver1);
            assertThat(cows1.get().size()).isEqualTo(1);
            assertThat(version2.get()).isEqualTo(ver1);
            assertThat(cows2.get().size()).isEqualTo(1);
            assertThat(Stream.concat(cows1.get().stream(), cows2.get().stream()).toList()).containsExactlyInAnyOrder(cow1, cow2);
        });

        //add cows to herd
        ByteBuffer cow3 = ByteBuffer.wrap(new byte[]{2});
        ByteBuffer cow4 = ByteBuffer.wrap(new byte[]{3});
        ByteBuffer[] cowsAdded = new ByteBuffer[]{cow1, cow2, cow3, cow4};
        int ver2 = nextVersion(ver1, versioned);

        Stream.of(shepherd1, shepherd2).forEach(shepherd -> shepherd.setPopulation(cowsAdded, ver2));

        await().timeout(ofSeconds(5)).untilAsserted(() -> {
            assertThat(version1.get()).isEqualTo(ver2);
            assertThat(cows1.get().size()).isEqualTo(2);
            assertThat(version2.get()).isEqualTo(ver2);
            assertThat(cows2.get().size()).isEqualTo(2);
            assertThat(Stream.concat(cows1.get().stream(), cows2.get().stream()).toList()).containsExactlyInAnyOrder(cow1, cow2, cow3, cow4);
        });

        //removed cows from herd
        ByteBuffer[] cowsRemoved = new ByteBuffer[]{cow3, cow4};
        int ver3 = nextVersion(ver2, versioned);

        Stream.of(shepherd1, shepherd2).forEach(shepherd -> shepherd.setPopulation(cowsRemoved, ver3));

        await().timeout(ofSeconds(5)).untilAsserted(() -> {
            assertThat(version1.get()).isEqualTo(ver3);
            assertThat(cows1.get().size()).isEqualTo(1);
            assertThat(version2.get()).isEqualTo(ver3);
            assertThat(cows2.get().size()).isEqualTo(1);
            assertThat(Stream.concat(cows1.get().stream(), cows2.get().stream()).toList()).containsExactlyInAnyOrder(cow3, cow4);
        });

        //leave only one cow
        ByteBuffer[] cowsRemoved2 = new ByteBuffer[]{cow4};
        int ver4 = nextVersion(ver3, versioned);

        Stream.of(shepherd1, shepherd2).forEach(shepherd -> shepherd.setPopulation(cowsRemoved2, ver4));

        await().timeout(ofSeconds(5)).untilAsserted(() -> {
            assertThat(version1.get()).isEqualTo(ver4);
            assertThat(version2.get()).isEqualTo(ver4);
            assertThat(Stream.concat(cows1.get().stream(), cows2.get().stream()).toList()).containsExactlyInAnyOrder(cow4);
        });

        //leave no cow
        ByteBuffer[] noCows = new ByteBuffer[]{};
        int ver5 = nextVersion(ver4, versioned);

        Stream.of(shepherd1, shepherd2).forEach(shepherd -> shepherd.setPopulation(noCows, ver5));

        await().timeout(ofSeconds(5)).untilAsserted(() -> {
            assertThat(version1.get()).isEqualTo(ver5);
            assertThat(version2.get()).isEqualTo(ver5);
            assertThat(Stream.concat(cows1.get().stream(), cows2.get().stream()).toList()).isEmpty();
        });
    }

    @Test
    public void shouldBalanceDynamicConcurrentSequenceHerd() {

        KafkaFarm kafkaRanch = new KafkaFarm(kafka.getBootstrapServers(), TEST_PROPERTIES);

        ByteBuffer cow1 = ByteBuffer.wrap(new byte[]{1});
        ByteBuffer cow2 = ByteBuffer.wrap(new byte[]{0});
        ByteBuffer cow3 = ByteBuffer.wrap(new byte[]{2});
        ByteBuffer cow4 = ByteBuffer.wrap(new byte[]{3});
        List<ByteBuffer[]> herdsSequence = List.of(
                new ByteBuffer[]{cow1, cow2},
                new ByteBuffer[]{cow3, cow4},
                new ByteBuffer[]{cow1},
                new ByteBuffer[]{cow4}
        );

        AtomicReference<List<ByteBuffer>> cows1 = new AtomicReference<>(List.of());
        Shepherd shepherd1 = kafkaRanch.addPasture("random-group", (population, version, generation, isLeader) -> {
            logger.info("Assigned cows1 [{}]", toArrays(population));
            cows1.set(population);
        }).getShepherd();
        shepherd1.setPopulation(new ByteBuffer[]{}, 0);

        AtomicReference<List<ByteBuffer>> cows2 = new AtomicReference<>(List.of());
        Shepherd shepherd2 = kafkaRanch.addPasture("random-group", (population, version, generation, isLeader) -> {
            logger.info("Assigned cows2 [{}]", toArrays(population));
            cows2.set(population);
        }).getShepherd();
        shepherd2.setPopulation(new ByteBuffer[]{}, 0);

        int version = 1;
        for(int i = 0; i < 10; i++) {
            int ver = version;
            Thread thread1 = new Thread(() -> {
                setHerds(shepherd1, herdsSequence, ver);
            });

            Thread thread2 = new Thread(() -> {
                setHerds(shepherd2, herdsSequence, ver);
            });

            thread1.start();
            thread2.start();

            await().timeout(ofSeconds(5)).untilAsserted(() -> {
                assertThat(Stream.concat(cows1.get().stream(), cows2.get().stream()).toList()).containsExactlyInAnyOrder(cow4);
            });

            version += herdsSequence.size();
        }

    }

    private static final Random random = new Random();
    private void setHerds(Shepherd shepherd, List<ByteBuffer[]> herdsSequence, int version) {
        for(ByteBuffer[] population : herdsSequence){
            try {
                Thread.sleep(random.nextLong(10));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            shepherd.setPopulation(population, version++);
        }
    }
}
