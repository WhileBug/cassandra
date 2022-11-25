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
package org.apache.cassandra.index.sasi.disk;

/**
 * Object descriptor for SASIIndex files. Similar to, and based upon, the sstable descriptor.
 */
public class Descriptor {

    public static transient org.slf4j.Logger logger_IC = org.slf4j.LoggerFactory.getLogger(Descriptor.class);

    public static final transient String VERSION_AA = "aa";

    public static final transient String VERSION_AB = "ab";

    public static final transient String CURRENT_VERSION = VERSION_AB;

    public static final transient Descriptor CURRENT = new Descriptor(CURRENT_VERSION);

    public static class Version {

        public final transient String version;

        public Version(String version) {
            this.version = version;
        }

        public String toString() {
            return version;
        }
    }

    public final transient Version version;

    public Descriptor(String v) {
        this.version = new Version(v);
    }
}
