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

import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.playtika.shepherd.inernal.utils.ThreadUtils.shutdownExecutorServiceQuietly;
import static org.apache.kafka.common.utils.ThreadUtils.createThreadFactory;

/**
 * <p>
 *     Distributed "shepherd" that coordinates with other shepherd to spread creatures across multiple pastures.
 * </p>
 * <p>
 *     Under the hood, this is implemented as a group managed by Kafka's group membership facilities (i.e. the generalized
 *     group/consumer coordinator). Each instance of DistributedHerder joins the group and indicates what its current
 *     configuration state is (where it is in the configuration log). The group coordinator selects one member to take
 *     this information and assign each instance a subset of population.
 *     The assignment strategy depends on the {@link Assignor} used.
 * </p>
 */
public class PastureShepherd implements Runnable {

    private final Logger logger;

    // Visible for testing
    private final ExecutorService shepherdExecutor;
    // Visible for testing
    private final PastureWorker worker;
    private final AtomicBoolean stopping;

    private volatile boolean needsReconfigRebalance;


    // The thread that the shepherd's tick loop runs on. Would be final, but cannot be set in the constructor,
    // and it's also useful to be able to modify it for testing
    Thread shepherdThread;

    private boolean running;

    /**
     * Create a shepherd that will form a cluster with other {@link PastureShepherd} instances (in this or other JVMs)
     * that have the same group ID.
     */
    public PastureShepherd(PastureWorker worker, LogContext logContext) {

        logger = logContext.logger(PastureShepherd.class);

        this.worker = worker;

        // Thread factory uses String.format and '%' is handled as a placeholder
        // need to escape if the client.id contains an actual % character
        String escapedClientIdForThreadNameFormat = worker.getClientId().replace("%", "%%");
        this.shepherdExecutor = new ThreadPoolExecutor(1, 1, 0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingDeque<>(1),
                createThreadFactory(
                        this.getClass().getSimpleName() + "-" + escapedClientIdForThreadNameFormat + "-%d", false));

        stopping = new AtomicBoolean(false);
        needsReconfigRebalance = false;
    }

    public void setNeedsReconfigRebalance(){
        if(!worker.isLeader()){
            throw new IllegalStateException("Only leader may initiate rebalance on herd change");
        }
        needsReconfigRebalance = true;
        worker.wakeup();
    }

    public void start() {
        this.shepherdExecutor.submit(this);
    }

    @Override
    public void run() {
        try {
            logger.info("Herder starting");
            shepherdThread = Thread.currentThread();

            logger.info("Herder started");
            running = true;

            while (!stopping.get()) {
                tick();
            }

            halt();

            logger.info("Herder stopped");
        } catch (Throwable t) {
            logger.error("Uncaught exception in shepherd work thread, exiting: ", t);
            Exit.exit(1);
        } finally {
            running = false;
        }
    }

    // public for testing
    public void tick() {
        // The main loop does two primary things: 1) drive the group membership protocol, responding to rebalance events
        // as they occur, and 2) handle external requests targeted at the leader. All the "real" work of the shepherd is
        // performed in this thread, which keeps synchronization straightforward at the cost of some operations possibly
        // blocking up this thread (especially those in callbacks due to rebalance events).

        try {
            logger.debug("Ensuring group membership is still active");
            worker.ensureActive();
        } catch (WakeupException e) {
            // May be due to a request from another thread, or might be stopping. If the latter, we need to check the
            // flag immediately. If the former, we need to re-run the ensureActive call since we can't handle requests
            // unless we're in the group.
            logger.trace("Woken up while ensure group membership is still active");
            return;
        }

        // With eager protocol we should return immediately if needsReconfigRebalance has
        // been set to retain the old workflow
        if (needsReconfigRebalance) {
            // Task reconfigs require a rebalance. Request the rebalance, clean out state, and then restart
            // this loop, which will then ensure the rebalance occurs without any other requests being
            // processed until it completes.
            logger.debug("Requesting rebalance due to reconfiguration of tasks");
            worker.requestRejoin();
            needsReconfigRebalance = false;
            return;
        }

        // Let the group take any actions it needs to
        try {
            logger.trace("Polling for group activity");
            worker.poll(Long.MAX_VALUE);
        } catch (WakeupException e) { // FIXME should not be WakeupException
            logger.trace("Woken up while polling for group activity");
            // Ignore. Just indicates we need to check the exit flag, for requested actions, etc.
        }
    }

    // public for testing
    public void halt() {
        synchronized (this) {
            worker.stop();
        }
    }

    public void stop(long timeoutMs) {
        logger.info("Pasture shepherd stopping");

        stopping.set(true);
        worker.wakeup();
        shutdownExecutorServiceQuietly(shepherdExecutor, timeoutMs, TimeUnit.MILLISECONDS);
        logger.info("Pasture shepherd stopped");
        running = false;
    }

    public boolean isLeader(){
        return worker.isLeader();
    }
}
