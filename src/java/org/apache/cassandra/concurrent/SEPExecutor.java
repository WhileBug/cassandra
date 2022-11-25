/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.metrics.ThreadPoolMetrics;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.concurrent.SimpleCondition;
import static org.apache.cassandra.concurrent.SEPWorker.Work;

public class SEPExecutor extends AbstractLocalAwareExecutorService implements SEPExecutorMBean {

    public static transient org.slf4j.Logger logger_IC = org.slf4j.LoggerFactory.getLogger(SEPExecutor.class);

    public static transient org.slf4j.Logger logger_IC = org.slf4j.LoggerFactory.getLogger(SEPExecutor.class);

    private static final transient Logger logger = LoggerFactory.getLogger(SEPExecutor.class);

    private final transient SharedExecutorPool pool;

    private final transient AtomicInteger maximumPoolSize;

    private final transient MaximumPoolSizeListener maximumPoolSizeListener;

    public final transient String name;

    private final transient String mbeanName;

    @VisibleForTesting
    public final transient ThreadPoolMetrics metrics;

    // stores both a set of work permits and task permits:
    // bottom 32 bits are number of queued tasks, in the range [0..maxTasksQueued]   (initially 0)
    // top 32 bits are number of work permits available in the range [-resizeDelta..maximumPoolSize]   (initially maximumPoolSize)
    private final transient AtomicLong permits = new AtomicLong();

    private final transient AtomicLong completedTasks = new AtomicLong();

    volatile transient boolean shuttingDown = false;

    final transient SimpleCondition shutdown = new SimpleCondition();

    // TODO: see if other queue implementations might improve throughput
    protected final transient ConcurrentLinkedQueue<FutureTask<?>> tasks = new ConcurrentLinkedQueue<>();

    SEPExecutor(SharedExecutorPool pool, int maximumPoolSize, MaximumPoolSizeListener maximumPoolSizeListener, String jmxPath, String name) {
        this.pool = pool;
        this.name = name;
        this.mbeanName = "org.apache.cassandra." + jmxPath + ":type=" + name;
        this.maximumPoolSize = new AtomicInteger(maximumPoolSize);
        this.maximumPoolSizeListener = maximumPoolSizeListener;
        this.permits.set(combine(0, maximumPoolSize));
        this.metrics = new ThreadPoolMetrics(this, jmxPath, name).register();
        MBeanWrapper.instance.registerMBean(this, mbeanName);
    }

    protected void onCompletion() {
        completedTasks.incrementAndGet();
    }

    @Override
    public int getMaxTasksQueued() {
        return Integer.MAX_VALUE;
    }

    // schedules another worker for this pool if there is work outstanding and there are no spinning threads that
    // will self-assign to it in the immediate future
    boolean maybeSchedule() {
        if (pool.spinningCount.get() > 0 || !takeWorkPermit(true))
            return false;
        pool.schedule(new Work(this));
        return true;
    }

    protected void addTask(FutureTask<?> task) {
        // we add to the queue first, so that when a worker takes a task permit it can be certain there is a task available
        // this permits us to schedule threads non-spuriously; it also means work is serviced fairly
        tasks.add(task);
        int taskPermits;
        while (true) {
            long current = permits.get();
            taskPermits = taskPermits(current);
            // because there is no difference in practical terms between the work permit being added or not (the work is already in existence)
            // we always add our permit, but block after the fact if we breached the queue limit
            if (permits.compareAndSet(current, updateTaskPermits(current, taskPermits + 1)))
                break;
        }
        if (taskPermits == 0) {
            // we only need to schedule a thread if there are no tasks already waiting to be processed, as
            // the original enqueue will have started a thread to service its work which will have itself
            // spawned helper workers that would have either exhausted the available tasks or are still being spawned.
            // to avoid incurring any unnecessary signalling penalties we also do not take any work to hand to the new
            // worker, we simply start a worker in a spinning state
            pool.maybeStartSpinningWorker();
        }
    }

    public enum TakeTaskPermitResult {

        // No task permits available
        NONE_AVAILABLE,
        // Took a permit and reduced task permits
        TOOK_PERMIT,
        // Detected pool shrinking and returned work permit ahead of SEPWorker exit.
        RETURNED_WORK_PERMIT
    }

    // takes permission to perform a task, if any are available; once taken it is guaranteed
    // that a proceeding call to tasks.poll() will return some work
    TakeTaskPermitResult takeTaskPermit(boolean checkForWorkPermitOvercommit) {
        TakeTaskPermitResult result;
        while (true) {
            long current = permits.get();
            long updated;
            int workPermits = workPermits(current);
            int taskPermits = taskPermits(current);
            if (workPermits < 0 && checkForWorkPermitOvercommit) {
                // Work permits are negative when the pool is reducing in size.  Atomically
                // adjust the number of work permits so there is no race of multiple SEPWorkers
                // exiting.  On conflicting update, recheck.
                result = TakeTaskPermitResult.RETURNED_WORK_PERMIT;
                updated = updateWorkPermits(current, workPermits + 1);
            } else {
                if (taskPermits == 0)
                    return TakeTaskPermitResult.NONE_AVAILABLE;
                result = TakeTaskPermitResult.TOOK_PERMIT;
                updated = updateTaskPermits(current, taskPermits - 1);
            }
            if (permits.compareAndSet(current, updated)) {
                return result;
            }
        }
    }

    // takes a worker permit and (optionally) a task permit simultaneously; if one of the two is unavailable, returns false
    boolean takeWorkPermit(boolean takeTaskPermit) {
        int taskDelta = takeTaskPermit ? 1 : 0;
        while (true) {
            long current = permits.get();
            int workPermits = workPermits(current);
            int taskPermits = taskPermits(current);
            if (workPermits <= 0 || taskPermits == 0)
                return false;
            if (permits.compareAndSet(current, combine(taskPermits - taskDelta, workPermits - 1))) {
                return true;
            }
        }
    }

    // gives up a work permit
    void returnWorkPermit() {
        while (true) {
            long current = permits.get();
            int workPermits = workPermits(current);
            if (permits.compareAndSet(current, updateWorkPermits(current, workPermits + 1)))
                return;
        }
    }

    @Override
    public void maybeExecuteImmediately(Runnable command) {
        FutureTask<?> ft = newTaskFor(command, null);
        if (!takeWorkPermit(false)) {
            addTask(ft);
        } else {
            try {
                ft.run();
            } finally {
                returnWorkPermit();
                // we have to maintain our invariant of always scheduling after any work is performed
                // in this case in particular we are not processing the rest of the queue anyway, and so
                // the work permit may go wasted if we don't immediately attempt to spawn another worker
                maybeSchedule();
            }
        }
    }

    public synchronized void shutdown() {
        if (shuttingDown)
            return;
        shuttingDown = true;
        pool.executors.remove(this);
        if (getActiveTaskCount() == 0)
            shutdown.signalAll();
        // release metrics
        metrics.release();
        MBeanWrapper.instance.unregisterMBean(mbeanName);
    }

    public synchronized List<Runnable> shutdownNow() {
        shutdown();
        List<Runnable> aborted = new ArrayList<>();
        while (takeTaskPermit(false) == TakeTaskPermitResult.TOOK_PERMIT) aborted.add(tasks.poll());
        return aborted;
    }

    public boolean isShutdown() {
        return shuttingDown;
    }

    public boolean isTerminated() {
        return shuttingDown && shutdown.isSignaled();
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        shutdown.await(timeout, unit);
        return isTerminated();
    }

    @Override
    public int getPendingTaskCount() {
        return taskPermits(permits.get());
    }

    @Override
    public long getCompletedTaskCount() {
        return completedTasks.get();
    }

    public int getActiveTaskCount() {
        return maximumPoolSize.get() - workPermits(permits.get());
    }

    public int getCorePoolSize() {
        return 0;
    }

    public void setCorePoolSize(int newCorePoolSize) {
        throw new IllegalArgumentException("Cannot resize core pool size of SEPExecutor");
    }

    @Override
    public int getMaximumPoolSize() {
        return maximumPoolSize.get();
    }

    @Override
    public synchronized void setMaximumPoolSize(int newMaximumPoolSize) {
        final int oldMaximumPoolSize = maximumPoolSize.get();
        if (newMaximumPoolSize < 0) {
            throw new IllegalArgumentException("Maximum number of workers must not be negative");
        }
        int deltaWorkPermits = newMaximumPoolSize - oldMaximumPoolSize;
        if (!maximumPoolSize.compareAndSet(oldMaximumPoolSize, newMaximumPoolSize)) {
            throw new IllegalStateException("Maximum pool size has been changed while resizing");
        }
        if (deltaWorkPermits == 0)
            return;
        permits.updateAndGet(cur -> updateWorkPermits(cur, workPermits(cur) + deltaWorkPermits));
        logger.info("Resized {} maximum pool size from {} to {}", name, oldMaximumPoolSize, newMaximumPoolSize);
        // If we we have more work permits than before we should spin up a worker now rather than waiting
        // until either a new task is enqueued (if all workers are descheduled) or a spinning worker calls
        // maybeSchedule().
        pool.maybeStartSpinningWorker();
        maximumPoolSizeListener.onUpdateMaximumPoolSize(newMaximumPoolSize);
    }

    private static int taskPermits(long both) {
        return (int) both;
    }

    private static // may be negative if resizing
    int workPermits(long both) {
        // sign extending right shift
        return (int) (both >> 32);
    }

    private static long updateTaskPermits(long prev, int taskPermits) {
        return (prev & (-1L << 32)) | taskPermits;
    }

    private static long updateWorkPermits(long prev, int workPermits) {
        return (((long) workPermits) << 32) | (prev & (-1L >>> 32));
    }

    private static long combine(int taskPermits, int workPermits) {
        return (((long) workPermits) << 32) | taskPermits;
    }
}
