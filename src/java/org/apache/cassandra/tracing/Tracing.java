/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.tracing;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.concurrent.ExecutorLocal;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.UUIDGen;

/**
 * A trace session context. Able to track and store trace sessions. A session is usually a user initiated query, and may
 * have multiple local and remote events before it is completed.
 */
public abstract class Tracing implements ExecutorLocal<TraceState> {

    public static transient org.slf4j.Logger logger_IC = org.slf4j.LoggerFactory.getLogger(Tracing.class);

    public static final transient IVersionedSerializer<TraceType> traceTypeSerializer = new IVersionedSerializer<TraceType>() {

        public void serialize(TraceType traceType, DataOutputPlus out, int version) throws IOException {
            out.write((byte) traceType.ordinal());
        }

        public TraceType deserialize(DataInputPlus in, int version) throws IOException {
            return TraceType.deserialize(in.readByte());
        }

        public long serializedSize(TraceType traceType, int version) {
            return 1;
        }
    };

    /* this enum is used in serialization; preserve order for compatibility */
    public enum TraceType {

        NONE, QUERY, REPAIR;

        private static final TraceType[] ALL_VALUES = values();

        public static TraceType deserialize(byte b) {
            if (b < 0 || ALL_VALUES.length <= b)
                return NONE;
            return ALL_VALUES[b];
        }

        public static byte serialize(TraceType value) {
            return (byte) value.ordinal();
        }

        private static final int[] TTLS = { DatabaseDescriptor.getTracetypeQueryTTL(), DatabaseDescriptor.getTracetypeQueryTTL(), DatabaseDescriptor.getTracetypeRepairTTL() };

        public int getTTL() {
            return TTLS[ordinal()];
        }
    }

    protected static final transient Logger logger = LoggerFactory.getLogger(Tracing.class);

    private final transient InetAddressAndPort localAddress = FBUtilities.getLocalAddressAndPort();

    private final FastThreadLocal<TraceState> state = new FastThreadLocal<>();

    protected final ConcurrentMap<UUID, TraceState> sessions = new ConcurrentHashMap<>();

    public static final Tracing instance;

    static {
        Tracing tracing = null;
        String customTracingClass = System.getProperty("cassandra.custom_tracing_class");
        if (null != customTracingClass) {
            try {
                tracing = FBUtilities.construct(customTracingClass, "Tracing");
                logger.info("Using {} as tracing queries (as requested with -Dcassandra.custom_tracing_class)", customTracingClass);
            } catch (Exception e) {
                JVMStabilityInspector.inspectThrowable(e);
                logger.error(String.format("Cannot use class %s for tracing, ignoring by defaulting to normal tracing", customTracingClass), e);
            }
        }
        instance = null != tracing ? tracing : new TracingImpl();
    }

    public UUID getSessionId() {
        assert isTracing();
        return state.get().sessionId;
    }

    public TraceType getTraceType() {
        assert isTracing();
        return state.get().traceType;
    }

    public int getTTL() {
        assert isTracing();
        return state.get().ttl;
    }

    /**
     * Indicates if the current thread's execution is being traced.
     */
    public static boolean isTracing() {
        logger_IC.info("[InconsistencyDetector][org.apache.cassandra.tracing.Tracing.instance]=" + org.json.simple.JSONValue.toJSONString(instance).replace("\n", "").replace("\r", ""));
        return instance.get() != null;
    }

    public UUID newSession(Map<String, ByteBuffer> customPayload) {
        logger_IC.info("[InconsistencyDetector][org.apache.cassandra.tracing.Tracing.instance]=" + org.json.simple.JSONValue.toJSONString(instance).replace("\n", "").replace("\r", ""));
        return newSession(TimeUUIDType.instance.compose(ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes())), TraceType.QUERY, customPayload);
    }

    public UUID newSession(TraceType traceType) {
        logger_IC.info("[InconsistencyDetector][org.apache.cassandra.tracing.Tracing.instance]=" + org.json.simple.JSONValue.toJSONString(instance).replace("\n", "").replace("\r", ""));
        return newSession(TimeUUIDType.instance.compose(ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes())), traceType, Collections.EMPTY_MAP);
    }

    public UUID newSession(UUID sessionId, Map<String, ByteBuffer> customPayload) {
        return newSession(sessionId, TraceType.QUERY, customPayload);
    }

    /**
     * This method is intended to be overridden in tracing implementations that need access to the customPayload
     */
    protected UUID newSession(UUID sessionId, TraceType traceType, Map<String, ByteBuffer> customPayload) {
        assert get() == null;
        TraceState ts = newTraceState(localAddress, sessionId, traceType);
        set(ts);
        sessions.put(sessionId, ts);
        logger_IC.info("[InconsistencyDetector][org.apache.cassandra.tracing.Tracing.sessions]=" + org.json.simple.JSONValue.toJSONString(sessions).replace("\n", "").replace("\r", ""));
        return sessionId;
    }

    public void doneWithNonLocalSession(TraceState state) {
        if (state.releaseReference() == 0)
            sessions.remove(state.sessionId);
        logger_IC.info("[InconsistencyDetector][org.apache.cassandra.tracing.Tracing.sessions]=" + org.json.simple.JSONValue.toJSONString(sessions).replace("\n", "").replace("\r", ""));
        logger_IC.info("[InconsistencyDetector][org.apache.cassandra.tracing.Tracing.state]=" + org.json.simple.JSONValue.toJSONString(state).replace("\n", "").replace("\r", ""));
    }

    /**
     * Stop the session and record its complete.  Called by coodinator when request is complete.
     */
    public void stopSession() {
        TraceState state = get();
        if (// inline isTracing to avoid implicit two calls to state.get()
        state == null) {
            logger.trace("request complete");
        } else {
            stopSessionImpl();
            state.stop();
            sessions.remove(state.sessionId);
            set(null);
        }
        logger_IC.info("[InconsistencyDetector][org.apache.cassandra.tracing.Tracing.sessions]=" + org.json.simple.JSONValue.toJSONString(sessions).replace("\n", "").replace("\r", ""));
        logger_IC.info("[InconsistencyDetector][org.apache.cassandra.tracing.Tracing.state]=" + org.json.simple.JSONValue.toJSONString(state).replace("\n", "").replace("\r", ""));
    }

    protected abstract void stopSessionImpl();

    public TraceState get() {
        return state.get();
    }

    public TraceState get(UUID sessionId) {
        return sessions.get(sessionId);
    }

    public void set(final TraceState tls) {
        state.set(tls);
        logger_IC.info("[InconsistencyDetector][org.apache.cassandra.tracing.Tracing.state]=" + org.json.simple.JSONValue.toJSONString(state).replace("\n", "").replace("\r", ""));
    }

    public TraceState begin(final String request, final Map<String, String> parameters) {
        return begin(request, null, parameters);
    }

    public abstract TraceState begin(String request, InetAddress client, Map<String, String> parameters);

    /**
     * Determines the tracing context from a message.  Does NOT set the threadlocal state.
     *
     * @param header The internode message header
     */
    public TraceState initializeFromMessage(final Message.Header header) {
        final UUID sessionId = header.traceSession();
        if (sessionId == null) {
            logger_IC.info("[InconsistencyDetector][org.apache.cassandra.tracing.Tracing.sessions]=" + org.json.simple.JSONValue.toJSONString(sessions).replace("\n", "").replace("\r", ""));
            return null;
        }
        TraceState ts = get(sessionId);
        if (ts != null && ts.acquireReference()) {
            logger_IC.info("[InconsistencyDetector][org.apache.cassandra.tracing.Tracing.sessions]=" + org.json.simple.JSONValue.toJSONString(sessions).replace("\n", "").replace("\r", ""));
            return ts;
        }
        TraceType traceType = header.traceType();
        if (header.verb.isResponse()) {
            logger_IC.info("[InconsistencyDetector][org.apache.cassandra.tracing.Tracing.sessions]=" + org.json.simple.JSONValue.toJSONString(sessions).replace("\n", "").replace("\r", ""));
            // received a message for a session we've already closed out.  see CASSANDRA-5668
            return new ExpiredTraceState(newTraceState(header.from, sessionId, traceType));
        } else {
            ts = newTraceState(header.from, sessionId, traceType);
            sessions.put(sessionId, ts);
            logger_IC.info("[InconsistencyDetector][org.apache.cassandra.tracing.Tracing.sessions]=" + org.json.simple.JSONValue.toJSONString(sessions).replace("\n", "").replace("\r", ""));
            return ts;
        }
    }

    /**
     * Record any tracing data, if enabled on this message.
     */
    public void traceOutgoingMessage(Message<?> message, int serializedSize, InetAddressAndPort sendTo) {
        try {
            final UUID sessionId = message.traceSession();
            if (sessionId == null) {
                logger_IC.info("[InconsistencyDetector][org.apache.cassandra.tracing.Tracing.state]=" + org.json.simple.JSONValue.toJSONString(state).replace("\n", "").replace("\r", ""));
                return;
            }
            String logMessage = String.format("Sending %s message to %s message size %d bytes", message.verb(), sendTo, serializedSize);
            TraceState state = get(sessionId);
            if (// session may have already finished; see CASSANDRA-5668
            state == null) {
                TraceType traceType = message.traceType();
                trace(ByteBuffer.wrap(UUIDGen.decompose(sessionId)), logMessage, traceType.getTTL());
            } else {
                state.trace(logMessage);
                if (message.verb().isResponse())
                    doneWithNonLocalSession(state);
            }
        } catch (Exception e) {
            logger.warn("failed to capture the tracing info for an outbound message to {}, ignoring", sendTo, e);
        }
    }

    public Map<ParamType, Object> addTraceHeaders(Map<ParamType, Object> addToMutable) {
        assert isTracing();
        addToMutable.put(ParamType.TRACE_SESSION, Tracing.instance.getSessionId());
        addToMutable.put(ParamType.TRACE_TYPE, Tracing.instance.getTraceType());
        logger_IC.info("[InconsistencyDetector][org.apache.cassandra.tracing.Tracing.instance]=" + org.json.simple.JSONValue.toJSONString(instance).replace("\n", "").replace("\r", ""));
        return addToMutable;
    }

    protected abstract TraceState newTraceState(InetAddressAndPort coordinator, UUID sessionId, Tracing.TraceType traceType);

    // repair just gets a varargs method since it's so heavyweight anyway
    public static void traceRepair(String format, Object... args) {
        final TraceState state = instance.get();
        if (// inline isTracing to avoid implicit two calls to state.get()
        state == null) {
            logger_IC.info("[InconsistencyDetector][org.apache.cassandra.tracing.Tracing.instance]=" + org.json.simple.JSONValue.toJSONString(instance).replace("\n", "").replace("\r", ""));
            return;
        }
        state.trace(format, args);
    }

    // normal traces get zero-, one-, and two-argument overloads so common case doesn't need to create varargs array
    public static void trace(String message) {
        final TraceState state = instance.get();
        if (// inline isTracing to avoid implicit two calls to state.get()
        state == null) {
            logger_IC.info("[InconsistencyDetector][org.apache.cassandra.tracing.Tracing.instance]=" + org.json.simple.JSONValue.toJSONString(instance).replace("\n", "").replace("\r", ""));
            return;
        }
        state.trace(message);
    }

    public static void trace(String format, Object arg) {
        final TraceState state = instance.get();
        if (// inline isTracing to avoid implicit two calls to state.get()
        state == null) {
            logger_IC.info("[InconsistencyDetector][org.apache.cassandra.tracing.Tracing.instance]=" + org.json.simple.JSONValue.toJSONString(instance).replace("\n", "").replace("\r", ""));
            return;
        }
        state.trace(format, arg);
    }

    public static void trace(String format, Object arg1, Object arg2) {
        final TraceState state = instance.get();
        if (// inline isTracing to avoid implicit two calls to state.get()
        state == null) {
            logger_IC.info("[InconsistencyDetector][org.apache.cassandra.tracing.Tracing.instance]=" + org.json.simple.JSONValue.toJSONString(instance).replace("\n", "").replace("\r", ""));
            return;
        }
        state.trace(format, arg1, arg2);
    }

    public static void trace(String format, Object... args) {
        final TraceState state = instance.get();
        if (// inline isTracing to avoid implicit two calls to state.get()
        state == null) {
            logger_IC.info("[InconsistencyDetector][org.apache.cassandra.tracing.Tracing.instance]=" + org.json.simple.JSONValue.toJSONString(instance).replace("\n", "").replace("\r", ""));
            return;
        }
        state.trace(format, args);
    }

    /**
     * Called for non-local traces (traces that are not initiated by local node == coordinator).
     */
    public abstract void trace(ByteBuffer sessionId, String message, int ttl);
}
