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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.utils.ExecutorUtils;

/**
 * Centralized location for shared executors
 */
public class ScheduledExecutors {

    public static transient org.slf4j.Logger logger_IC = org.slf4j.LoggerFactory.getLogger(ScheduledExecutors.class);

    /**
     * This pool is used for periodic fast (sub-microsecond) tasks.
     */
    public static final transient DebuggableScheduledThreadPoolExecutor scheduledFastTasks = new DebuggableScheduledThreadPoolExecutor("ScheduledFastTasks");

    /**
     * This pool is used for periodic short (sub-second) tasks.
     */
    public static final transient DebuggableScheduledThreadPoolExecutor scheduledTasks = new DebuggableScheduledThreadPoolExecutor("ScheduledTasks");

    /**
     * This executor is used for tasks that can have longer execution times, and usually are non periodic.
     */
    public static final transient DebuggableScheduledThreadPoolExecutor nonPeriodicTasks = new DebuggableScheduledThreadPoolExecutor("NonPeriodicTasks");

    /**
     * This executor is used for tasks that do not need to be waited for on shutdown/drain.
     */
    public static final transient DebuggableScheduledThreadPoolExecutor optionalTasks = new DebuggableScheduledThreadPoolExecutor("OptionalTasks");

    @VisibleForTesting
    public static void shutdownAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
        ExecutorUtils.shutdownNowAndWait(timeout, unit, scheduledFastTasks, scheduledTasks, nonPeriodicTasks, optionalTasks);
    }
}
