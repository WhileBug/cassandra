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

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import com.google.common.annotations.VisibleForTesting;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.FastThreadLocalThread;
import org.apache.cassandra.utils.memory.BufferPool;

/**
 * This class is an implementation of the <i>ThreadFactory</i> interface. This
 * is useful to give Java threads meaningful names which is useful when using
 * a tool like JConsole.
 */
public class NamedThreadFactory implements ThreadFactory {

    public static transient org.slf4j.Logger logger_IC = org.slf4j.LoggerFactory.getLogger(NamedThreadFactory.class);

    public static transient org.slf4j.Logger logger_IC = org.slf4j.LoggerFactory.getLogger(NamedThreadFactory.class);

    private static volatile transient String globalPrefix;

    public static void setGlobalPrefix(String prefix) {
        globalPrefix = prefix;
    }

    public static String globalPrefix() {
        String prefix = globalPrefix;
        return prefix == null ? "" : prefix;
    }

    public final transient String id;

    private final transient int priority;

    private final transient ClassLoader contextClassLoader;

    private final transient ThreadGroup threadGroup;

    protected final transient AtomicInteger n = new AtomicInteger(1);

    public NamedThreadFactory(String id) {
        this(id, Thread.NORM_PRIORITY);
    }

    public NamedThreadFactory(String id, int priority) {
        this(id, priority, null, null);
    }

    public NamedThreadFactory(String id, int priority, ClassLoader contextClassLoader, ThreadGroup threadGroup) {
        this.id = id;
        this.priority = priority;
        this.contextClassLoader = contextClassLoader;
        this.threadGroup = threadGroup;
    }

    public Thread newThread(Runnable runnable) {
        String name = id + ':' + n.getAndIncrement();
        Thread thread = createThread(threadGroup, runnable, name, true);
        thread.setPriority(priority);
        if (contextClassLoader != null)
            thread.setContextClassLoader(contextClassLoader);
        return thread;
    }

    private static final transient AtomicInteger threadCounter = new AtomicInteger();

    @VisibleForTesting
    public static Thread createThread(Runnable runnable) {
        return createThread(null, runnable, "anonymous-" + threadCounter.incrementAndGet());
    }

    public static Thread createThread(Runnable runnable, String name) {
        return createThread(null, runnable, name);
    }

    public static Thread createThread(Runnable runnable, String name, boolean daemon) {
        return createThread(null, runnable, name, daemon);
    }

    public static Thread createThread(ThreadGroup threadGroup, Runnable runnable, String name) {
        return createThread(threadGroup, runnable, name, false);
    }

    public static Thread createThread(ThreadGroup threadGroup, Runnable runnable, String name, boolean daemon) {
        String prefix = globalPrefix;
        Thread thread = new FastThreadLocalThread(threadGroup, runnable, prefix != null ? prefix + name : name);
        thread.setDaemon(daemon);
        return thread;
    }
}
