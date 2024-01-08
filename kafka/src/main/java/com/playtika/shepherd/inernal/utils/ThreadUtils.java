package com.playtika.shepherd.inernal.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class ThreadUtils {

    private static final Logger log = LoggerFactory.getLogger(ThreadUtils.class);

    public static void shutdownExecutorServiceQuietly(ExecutorService executorService,
                                                      long timeout, TimeUnit timeUnit) {
        executorService.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!executorService.awaitTermination(timeout, timeUnit)) {
                executorService.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!executorService.awaitTermination(timeout, timeUnit)) {
                    log.error("Executor {} did not terminate in time", executorService);
                }
            }
        } catch (InterruptedException e) {
            // (Re-)Cancel if current thread also interrupted
            executorService.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }
}

