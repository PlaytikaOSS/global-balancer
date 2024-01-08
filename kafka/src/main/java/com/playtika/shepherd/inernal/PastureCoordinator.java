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

import com.playtika.shepherd.common.PastureListener;
import com.playtika.shepherd.inernal.utils.BytesUtils;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.consumer.internals.AbstractCoordinator;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.playtika.shepherd.inernal.ProtocolHelper.deserializeAssignment;
import static org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocolCollection;
import static org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember;

/**
 * This class manages the coordination process with the Kafka group coordinator on the broker for managing assignments
 * to pastures.
 */
public class PastureCoordinator extends AbstractCoordinator {

    private final Logger logger = LoggerFactory.getLogger(PastureCoordinator.class);

    private volatile Assignment assignmentSnapshot;

    private boolean rejoinRequested;
    private volatile int lastCompletedGenerationId;
    private final int coordinatorDiscoveryTimeoutMs;

    private final Herd herd;
    private final Assignor assignor;
    private final PastureListener listener;

    /**
     * Initialize the coordination manager.
     */
    public PastureCoordinator(GroupRebalanceConfig config,
                              LogContext logContext,
                              ConsumerNetworkClient client,
                              Metrics metrics,
                              String metricGrpPrefix,
                              Time time,
                              Herd herd,
                              Assignor assignor,
                              PastureListener listener) {
        super(config,
              logContext,
              client,
              metrics,
              metricGrpPrefix,
              time);

        this.assignmentSnapshot = null;
        this.herd = herd;
        this.listener = listener;
        this.rejoinRequested = false;
        this.assignor = assignor;
        this.coordinatorDiscoveryTimeoutMs = config.heartbeatIntervalMs;
        this.lastCompletedGenerationId = Generation.NO_GENERATION.generationId;
    }

    @Override
    public void requestRejoin(final String reason) {
        logger.debug("Request joining group due to: {}", reason);
        rejoinRequested = true;
    }

    @Override
    public String protocolType() {
        return Protocol.SIMPLE.protocol();
    }

    // expose for tests
    @Override
    protected synchronized boolean ensureCoordinatorReady(final Timer timer) {
        return super.ensureCoordinatorReady(timer);
    }

    public void poll(long timeout) {
        // poll for io until the timeout expires
        final long start = time.milliseconds();
        long now = start;
        long remaining;

        do {
            if (coordinatorUnknown()) {
                logger.debug("Broker coordinator is marked unknown. Attempting discovery with a timeout of {}ms",
                        coordinatorDiscoveryTimeoutMs);
                if (ensureCoordinatorReady(time.timer(coordinatorDiscoveryTimeoutMs))) {
                    logger.debug("Broker coordinator is ready");
                } else {
                    logger.debug("Can not connect to broker coordinator");
                    if (assignmentSnapshot != null) {
                        logger.info("Broker coordinator was unreachable for {}ms. Revoking previous assignment {} to " +
                                "avoid running tasks while not being a member the group", coordinatorDiscoveryTimeoutMs, assignmentSnapshot);
                        listener.assigned(List.of(), -1, - generationId(), false);
                        assignmentSnapshot = null;
                    }
                }
                now = time.milliseconds();
            }

            if (rejoinNeededOrPending()) {
                ensureActiveGroup();
                now = time.milliseconds();
            }

            pollHeartbeat(now);

            long elapsed = now - start;
            remaining = timeout - elapsed;

            // Note that because the network client is shared with the background heartbeat thread,
            // we do not want to block in poll longer than the time to the next heartbeat.
            long pollTimeout = Math.min(Math.max(0, remaining), timeToNextHeartbeat(now));
            client.poll(time.timer(pollTimeout));

            now = time.milliseconds();
            elapsed = now - start;
            remaining = timeout - elapsed;
        } while (remaining > 0);
    }

    @Override
    public JoinGroupRequestProtocolCollection metadata() {
        herd.reset();

        return new JoinGroupRequestProtocolCollection(Collections.singleton(
                        new JoinGroupRequestData.JoinGroupRequestProtocol()
                                .setName(Protocol.SIMPLE.protocol())
                                .setMetadata(new byte[0]))
                .iterator());
    }

    @Override
    protected void onJoinComplete(int generation, String memberId, String protocol, ByteBuffer memberAssignment) {
        Assignment newAssignment = deserializeAssignment(memberAssignment);
        logger.debug("Deserialized new assignment: {}", newAssignment);
        // At this point we always consider ourselves to be a member of the cluster, even if there was an assignment
        // error (the leader couldn't make the assignment) or we are behind the config and cannot yet work on our assigned
        // tasks. It's the responsibility of the code driving this process to decide how to react (e.g. trying to get
        // up to date, try to rejoin again, leaving the group and backing off, etc.).
        rejoinRequested = false;
        assignmentSnapshot = newAssignment;
        lastCompletedGenerationId = generation;
        listener.assigned(newAssignment.getAssigned(), newAssignment.getVersion(), generation, isLeader());
    }

    @Override
    protected Map<String, ByteBuffer> onLeaderElected(String leaderId,
                                                      String protocol,
                                                      List<JoinGroupResponseMember> allMemberMetadata,
                                                      boolean skipAssignment) {
        if (skipAssignment)
            throw new IllegalStateException("Can't skip assignment because static membership is not supported.");

        Population population = herd.getPopulation();

        logger.info("Will rebalance population: [{}]", population.getSheep().stream().map(BytesUtils::getBytes).toList());

        return assignor.performAssignment(leaderId, protocol,
                population.getSheep(), population.getVersion(),
                allMemberMetadata);
    }

    @Override
    protected boolean onJoinPrepare(Timer timer, int generation, String memberId) {
        logger.info("Rebalance started generation: {}, memberId: {}", generation, memberId);
        return true;
    }

    @Override
    protected boolean rejoinNeededOrPending() {
        final Assignment localAssignmentSnapshot = assignmentSnapshot;
        return super.rejoinNeededOrPending() || (localAssignmentSnapshot == null) || rejoinRequested;
    }

    @Override
    public String memberId() {
        Generation generation = generationIfStable();
        if (generation != null)
            return generation.memberId;
        return JoinGroupRequest.UNKNOWN_MEMBER_ID;
    }

    /**
     * Return the current generation. The generation refers to this worker's knowledge with
     * respect to which generation is the latest one and, therefore, this information is local.
     *
     * @return the generation ID or -1 if no generation is defined
     */
    public int generationId() {
        return super.generation().generationId;
    }

    /**
     * Return id that corresponds to the group generation that was active when the last join was successful
     *
     * @return the generation ID of the last group that was joined successfully by this member or -1 if no generation
     * was stable at that point
     */
    public int lastCompletedGenerationId() {
        return lastCompletedGenerationId;
    }

    public boolean isLeader() {
        final Assignment localAssignmentSnapshot = assignmentSnapshot;
        return localAssignmentSnapshot != null && memberId().equals(localAssignmentSnapshot.getLeader());
    }



}
