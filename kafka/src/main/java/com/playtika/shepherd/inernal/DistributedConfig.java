/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.playtika.shepherd.inernal;

import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.CommonClientConfigs.AUTO_INCLUDE_JMX_REPORTER_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_DOC;
import static org.apache.kafka.clients.CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.CLIENT_DNS_LOOKUP_DOC;
import static org.apache.kafka.clients.CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

/**
 * Provides configuration for Kafka workers running in distributed mode.
 */
public class DistributedConfig extends AbstractConfig {

    private static final Logger log = LoggerFactory.getLogger(DistributedConfig.class);

    /*
     * NOTE: DO NOT CHANGE EITHER CONFIG STRINGS OR THEIR JAVA VARIABLE NAMES AS
     * THESE ARE PART OF THE PUBLIC API AND CHANGE WILL BREAK USER CODE.
     */

   public static final String BOOTSTRAP_SERVERS_DEFAULT = "localhost:9092";

    public static final String PROTOCOL_CONFIG = "protocol";
    public static final String PROTOCOL_DOC = "Compatibility mode for Kafka Protocol";

    public static final String KAFKA_CLUSTER_ID = "kafka.cluster.id";

    /**
     * <code>group.id</code>
     */
    public static final String GROUP_ID_CONFIG = CommonClientConfigs.GROUP_ID_CONFIG;
    private static final String GROUP_ID_DOC = "A unique string that identifies the cluster group this worker belongs to.";

    /**
     * <code>session.timeout.ms</code>
     */
    public static final String SESSION_TIMEOUT_MS_CONFIG = CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG;
    private static final String SESSION_TIMEOUT_MS_DOC = "The timeout used to detect worker failures. " +
            "The worker sends periodic heartbeats to indicate its liveness to the broker. If no heartbeats are " +
            "received by the broker before the expiration of this session timeout, then the broker will remove the " +
            "worker from the group and initiate a rebalance. Note that the value must be in the allowable range as " +
            "configured in the broker configuration by <code>group.min.session.timeout.ms</code> " +
            "and <code>group.max.session.timeout.ms</code>.";

    /**
     * <code>heartbeat.interval.ms</code>
     */
    public static final String HEARTBEAT_INTERVAL_MS_CONFIG = CommonClientConfigs.HEARTBEAT_INTERVAL_MS_CONFIG;
    private static final String HEARTBEAT_INTERVAL_MS_DOC = "The expected time between heartbeats to the group " +
            "coordinator when using Kafka's group management facilities. Heartbeats are used to ensure that the " +
            "worker's session stays active and to facilitate rebalancing when new members join or leave the group. " +
            "The value must be set lower than <code>session.timeout.ms</code>, but typically should be set no higher " +
            "than 1/3 of that value. It can be adjusted even lower to control the expected time for normal rebalances.";

    /**
     * <code>rebalance.timeout.ms</code>
     */
    public static final String REBALANCE_TIMEOUT_MS_CONFIG = CommonClientConfigs.REBALANCE_TIMEOUT_MS_CONFIG;
    private static final String REBALANCE_TIMEOUT_MS_DOC = CommonClientConfigs.REBALANCE_TIMEOUT_MS_DOC;

    /**
     * <code>worker.sync.timeout.ms</code>
     */
    public static final String WORKER_SYNC_TIMEOUT_MS_CONFIG = "worker.sync.timeout.ms";
    private static final String WORKER_SYNC_TIMEOUT_MS_DOC = "When the worker is out of sync with other workers and needs" +
            " to resynchronize configurations, wait up to this amount of time before giving up, leaving the group, and" +
            " waiting a backoff period before rejoining.";

    /**
     * <code>group.unsync.timeout.ms</code>
     */
    public static final String WORKER_UNSYNC_BACKOFF_MS_CONFIG = "worker.unsync.backoff.ms";
    private static final String WORKER_UNSYNC_BACKOFF_MS_DOC = "When the worker is out of sync with other workers and " +
            " fails to catch up within worker.sync.timeout.ms, leave the cluster for this long before rejoining.";
    public static final int WORKER_UNSYNC_BACKOFF_MS_DEFAULT = 5 * 60 * 1000;

    public static final String SCHEDULED_REBALANCE_MAX_DELAY_MS_CONFIG = "scheduled.rebalance.max.delay.ms";
    public static final String SCHEDULED_REBALANCE_MAX_DELAY_MS_DOC = "The maximum delay that is "
            + "scheduled in order to wait for the return of one or more departed workers before "
            + "rebalancing and reassigning to the group.";
    public static final int SCHEDULED_REBALANCE_MAX_DELAY_MS_DEFAULT = Math.toIntExact(TimeUnit.SECONDS.toMillis(300));


    @SuppressWarnings("unchecked")
    private static ConfigDef config() {
        return baseConfigDef()
            .define(GROUP_ID_CONFIG,
                    ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,
                    GROUP_ID_DOC)
            .define(SESSION_TIMEOUT_MS_CONFIG,
                    ConfigDef.Type.INT,
                    Math.toIntExact(TimeUnit.SECONDS.toMillis(10)),
                    ConfigDef.Importance.HIGH,
                    SESSION_TIMEOUT_MS_DOC)
            .define(REBALANCE_TIMEOUT_MS_CONFIG,
                    ConfigDef.Type.INT,
                    Math.toIntExact(TimeUnit.MINUTES.toMillis(1)),
                    ConfigDef.Importance.HIGH,
                    REBALANCE_TIMEOUT_MS_DOC)
            .define(HEARTBEAT_INTERVAL_MS_CONFIG,
                    ConfigDef.Type.INT,
                    Math.toIntExact(TimeUnit.SECONDS.toMillis(3)),
                    ConfigDef.Importance.HIGH,
                    HEARTBEAT_INTERVAL_MS_DOC)
            .define(CommonClientConfigs.METADATA_MAX_AGE_CONFIG,
                    ConfigDef.Type.LONG,
                    TimeUnit.MINUTES.toMillis(5),
                    atLeast(0),
                    ConfigDef.Importance.LOW,
                    CommonClientConfigs.METADATA_MAX_AGE_DOC)
            .define(CommonClientConfigs.CLIENT_ID_CONFIG,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.LOW,
                    CommonClientConfigs.CLIENT_ID_DOC)
            .define(CommonClientConfigs.SEND_BUFFER_CONFIG,
                    ConfigDef.Type.INT,
                    128 * 1024,
                    atLeast(CommonClientConfigs.SEND_BUFFER_LOWER_BOUND),
                    ConfigDef.Importance.MEDIUM,
                    CommonClientConfigs.SEND_BUFFER_DOC)
            .define(CommonClientConfigs.RECEIVE_BUFFER_CONFIG,
                    ConfigDef.Type.INT,
                    32 * 1024,
                    atLeast(CommonClientConfigs.RECEIVE_BUFFER_LOWER_BOUND),
                    ConfigDef.Importance.MEDIUM,
                    CommonClientConfigs.RECEIVE_BUFFER_DOC)
            .define(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG,
                    ConfigDef.Type.LONG,
                    50L,
                    atLeast(0L),
                    ConfigDef.Importance.LOW,
                    CommonClientConfigs.RECONNECT_BACKOFF_MS_DOC)
            .define(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG,
                    ConfigDef.Type.LONG,
                    TimeUnit.SECONDS.toMillis(1),
                    atLeast(0L),
                    ConfigDef.Importance.LOW,
                    CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_DOC)
            .define(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG,
                    ConfigDef.Type.LONG,
                    CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS,
                    atLeast(0L),
                    ConfigDef.Importance.LOW,
                    CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC)
            .define(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG,
                    ConfigDef.Type.LONG,
                    CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS,
                    atLeast(0L),
                    ConfigDef.Importance.LOW,
                    CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC)
            .define(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG,
                    ConfigDef.Type.LONG,
                    100L,
                    atLeast(0L),
                    ConfigDef.Importance.LOW,
                    CommonClientConfigs.RETRY_BACKOFF_MS_DOC)
            .define(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG,
                    ConfigDef.Type.INT,
                    Math.toIntExact(TimeUnit.SECONDS.toMillis(40)),
                    atLeast(0),
                    ConfigDef.Importance.MEDIUM,
                    CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC)
            /* default is set to be a bit lower than the server default (10 min), to avoid both client and server closing connection at same time */
            .define(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG,
                    ConfigDef.Type.LONG,
                    TimeUnit.MINUTES.toMillis(9),
                    ConfigDef.Importance.MEDIUM,
                    CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC)
            // security support
            .define(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                    ConfigDef.Type.STRING,
                    CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
                    in(Utils.enumOptions(SecurityProtocol.class)),
                    ConfigDef.Importance.MEDIUM,
                    CommonClientConfigs.SECURITY_PROTOCOL_DOC)
            .withClientSaslSupport()
            .define(WORKER_SYNC_TIMEOUT_MS_CONFIG,
                    ConfigDef.Type.INT,
                    3000,
                    ConfigDef.Importance.MEDIUM,
                    WORKER_SYNC_TIMEOUT_MS_DOC)
            .define(WORKER_UNSYNC_BACKOFF_MS_CONFIG,
                    ConfigDef.Type.INT,
                    WORKER_UNSYNC_BACKOFF_MS_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    WORKER_UNSYNC_BACKOFF_MS_DOC)
            .define(PROTOCOL_CONFIG,
                    ConfigDef.Type.STRING,
                    Protocol.SIMPLE.protocol(),
                    ConfigDef.LambdaValidator.with(
                        (name, value) -> {
                            try {
                                Protocol.fromProtocol((String) value);
                            } catch (Throwable t) {
                                throw new ConfigException(name, value, "Invalid protocol");
                            }
                        },
                        () -> "[" + Utils.join(Protocol.values(), ", ") + "]"),
                    ConfigDef.Importance.LOW,
                    PROTOCOL_DOC)
            .define(SCHEDULED_REBALANCE_MAX_DELAY_MS_CONFIG,
                    ConfigDef.Type.INT,
                    SCHEDULED_REBALANCE_MAX_DELAY_MS_DEFAULT,
                    between(0, Integer.MAX_VALUE),
                    ConfigDef.Importance.LOW,
                    SCHEDULED_REBALANCE_MAX_DELAY_MS_DOC);
    }

    protected static ConfigDef baseConfigDef() {
        ConfigDef result = new ConfigDef()
                .define(BOOTSTRAP_SERVERS_CONFIG, ConfigDef.Type.LIST, BOOTSTRAP_SERVERS_DEFAULT,
                        ConfigDef.Importance.HIGH, BOOTSTRAP_SERVERS_DOC)
                .define(CLIENT_DNS_LOOKUP_CONFIG,
                        ConfigDef.Type.STRING,
                        ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                        in(ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                                ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY.toString()),
                        ConfigDef.Importance.MEDIUM,
                        CLIENT_DNS_LOOKUP_DOC)
                .define(METRICS_SAMPLE_WINDOW_MS_CONFIG, ConfigDef.Type.LONG,
                        30000, atLeast(0), ConfigDef.Importance.LOW,
                        CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_DOC)
                .define(METRICS_NUM_SAMPLES_CONFIG, ConfigDef.Type.INT,
                        2, atLeast(1), ConfigDef.Importance.LOW,
                        CommonClientConfigs.METRICS_NUM_SAMPLES_DOC)
                .define(METRICS_RECORDING_LEVEL_CONFIG, ConfigDef.Type.STRING,
                        Sensor.RecordingLevel.INFO.toString(),
                        in(Sensor.RecordingLevel.INFO.toString(), Sensor.RecordingLevel.DEBUG.toString()),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.METRICS_RECORDING_LEVEL_DOC)
                .define(METRIC_REPORTER_CLASSES_CONFIG, ConfigDef.Type.LIST,
                        "", ConfigDef.Importance.LOW,
                        CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC)
                .define(AUTO_INCLUDE_JMX_REPORTER_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        true,
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.AUTO_INCLUDE_JMX_REPORTER_DOC)
                // security support
                .withClientSslSupport();
        return result;
    }

    public Integer rebalanceTimeout() {
        return getInt(DistributedConfig.REBALANCE_TIMEOUT_MS_CONFIG);
    }


    // Visible for testing
    DistributedConfig(Map<String, String> props) {
        super(config(), props);
    }

    private String kafkaClusterId;

    public String kafkaClusterId() {
        if (kafkaClusterId == null) {
            kafkaClusterId = lookupKafkaClusterId(this);
        }
        return kafkaClusterId;
    }

    // Visible for testing
    static String lookupKafkaClusterId(DistributedConfig config) {
        log.info("Creating Kafka admin client");
        try (Admin adminClient = Admin.create(config.originals())) {
            return lookupKafkaClusterId(adminClient);
        }
    }

    // Visible for testing
    static String lookupKafkaClusterId(Admin adminClient) {
        log.debug("Looking up Kafka cluster ID");
        try {
            KafkaFuture<String> clusterIdFuture = adminClient.describeCluster().clusterId();
            if (clusterIdFuture == null) {
                log.info("Kafka cluster version is too old to return cluster ID");
                return null;
            }
            log.debug("Fetching Kafka cluster ID");
            String kafkaClusterId = clusterIdFuture.get();
            log.info("Kafka cluster ID: {}", kafkaClusterId);
            return kafkaClusterId;
        } catch (InterruptedException e) {
            throw new KafkaException("Unexpectedly interrupted when looking up Kafka cluster info", e);
        } catch (ExecutionException e) {
            throw new KafkaException("Failed to connect to and describe Kafka cluster. "
                    + "Check worker's broker connection and security properties.", e);
        }
    }


}
