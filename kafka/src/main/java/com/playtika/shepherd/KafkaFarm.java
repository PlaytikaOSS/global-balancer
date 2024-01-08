package com.playtika.shepherd;

import com.playtika.shepherd.common.Pasture;
import com.playtika.shepherd.common.Shepherd;
import com.playtika.shepherd.common.PastureListener;
import com.playtika.shepherd.common.Farm;
import com.playtika.shepherd.inernal.Herd;
import com.playtika.shepherd.inernal.PastureShepherd;
import com.playtika.shepherd.inernal.PastureShepherdBuilder;
import com.playtika.shepherd.inernal.Population;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.playtika.shepherd.inernal.CheckedHerd.checked;

public class KafkaFarm implements Farm {

    private final String bootstrapServers;
    private final Map<String, String> properties;

    public KafkaFarm(String bootstrapServers, Map<String, String> properties) {
        this.bootstrapServers = bootstrapServers;
        this.properties = properties;
    }

    @Override
    public Pasture addPasture(String herdName, PastureListener pastureListener) {
        PushHerd pushHerd = new PushHerd(pastureListener);

        PastureShepherd pastureShepherd = new PastureShepherdBuilder()
                .setBootstrapServers(bootstrapServers)
                .setGroupId(herdName)
                .setProperties(properties)
                .setRebalanceListener(pushHerd)
                .setHerd(checked(pushHerd))
                .build();

        pushHerd.setPastureShepherd(pastureShepherd);

        pushHerd.setPopulation(new ByteBuffer[0], -1);

        pastureShepherd.start();

        return pushHerd;
    }

    private static final class PushHerd implements Herd, Pasture, Shepherd, PastureListener {

        private final PastureListener pastureListener;
        private PastureShepherd pastureShepherd;

        private Population snapshot;
        private Population latest;

        private PushHerd(PastureListener pastureListener) {
            this.pastureListener = pastureListener;
        }

        @Override
        public synchronized void setPopulation(ByteBuffer[] population, int version) {
            Set<ByteBuffer> latest = new HashSet<>(Arrays.asList(population));
            if(this.snapshot == null
                    || versionUpdated(this.snapshot.getVersion(), version)
                    || !this.snapshot.getSheep().equals(latest)){
                this.latest = new Population(latest, version);
                if(snapshot != null){
                    this.snapshot = null;
                    pastureShepherd.setNeedsReconfigRebalance();
                }
            }
        }

        private boolean versionUpdated(int snapshot, int latest){
            return latest > 0 && latest > snapshot;
        }

        @Override
        public synchronized Population getPopulation() {
            if(snapshot != null){
                throw new IllegalStateException("Should be called only once on rebalance");
            }
            if(latest == null){
                throw new IllegalStateException("Herd was not initialized before rebalance");
            }

            snapshot = latest;
            latest = null;
            return snapshot;
        }

        @Override
        public synchronized void reset() {
            if(snapshot != null) {
                latest = snapshot;
                snapshot = null;
            }
        }

        @Override
        public synchronized void assigned(List<ByteBuffer> population, int version, int generation, boolean isLeader) {
            this.pastureListener.assigned(population, version, generation, isLeader);
        }

        @Override
        public Shepherd getShepherd() {
            return this;
        }

        @Override
        public void close(Duration timeout) {
            pastureShepherd.stop(timeout.toMillis());
        }

        public void setPastureShepherd(PastureShepherd pastureShepherd) {
            this.pastureShepherd = pastureShepherd;
        }
    }
}
