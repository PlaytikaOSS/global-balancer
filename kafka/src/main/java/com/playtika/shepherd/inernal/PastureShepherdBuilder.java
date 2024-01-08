package com.playtika.shepherd.inernal;

import com.playtika.shepherd.common.PastureListener;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.playtika.shepherd.inernal.DistributedConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.CLIENT_ID_CONFIG;

public class PastureShepherdBuilder {

    private static final AtomicInteger CLIENT_ID_SEQUENCE = new AtomicInteger(1);

    private String bootstrapServers;
    private String groupId;
    private String clientId;

    private Map<String, String> properties;

    private Time time;

    private Herd herd;

    private PastureListener rebalanceListener;

    public PastureShepherd build(){

        String clientId = this.clientId == null ? "pasture-" + CLIENT_ID_SEQUENCE.getAndIncrement() : this.clientId;

        LogContext logContext = new LogContext("[Herder clientId=" + clientId + ", groupId=" + groupId + "] ");

        Map<String, String> properties = new HashMap<>();
        if(this.properties != null){
            properties.putAll(this.properties);
        }
        properties.putAll(Map.of(
                BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                GROUP_ID_CONFIG, groupId,
                CLIENT_ID_CONFIG, clientId
        ));

        DistributedConfig distributedConfig = new DistributedConfig(properties);

        PastureWorker pastureWorker = new PastureWorker(
                distributedConfig,
                time == null ? Time.SYSTEM : time,
                clientId, logContext,
                herd,
                new RoundRobinAssignor(),
                rebalanceListener);

        return new PastureShepherd(pastureWorker, logContext);
    }

    public PastureShepherdBuilder setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        return this;
    }

    public PastureShepherdBuilder setGroupId(String groupId) {
        this.groupId = groupId;
        return this;
    }

    public PastureShepherdBuilder setClientId(String clientId) {
        this.clientId = clientId;
        return this;
    }

    public PastureShepherdBuilder setProperties(Map<String, String> properties) {
        this.properties = properties;
        return this;
    }

    public PastureShepherdBuilder setTime(Time time) {
        this.time = time;
        return this;
    }

    public PastureShepherdBuilder setHerd(Herd herd) {
        this.herd = herd;
        return this;
    }

    public PastureShepherdBuilder setRebalanceListener(PastureListener rebalanceListener) {
        this.rebalanceListener = rebalanceListener;
        return this;
    }
}
