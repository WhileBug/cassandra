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
package org.apache.cassandra.io;

import java.io.File;
import java.io.IOError;

public abstract class FSError extends IOError {

    public static transient org.slf4j.Logger logger_IC = org.slf4j.LoggerFactory.getLogger(FSError.class);

    public static transient org.slf4j.Logger logger_IC = org.slf4j.LoggerFactory.getLogger(FSError.class);

    public final transient File path;

    public FSError(Throwable cause, File path) {
        super(cause);
        this.path = path;
    }

    /**
     * Unwraps the Throwable cause chain looking for an FSError instance
     * @param top the top-level Throwable to unwrap
     * @return FSError if found any, null otherwise
     */
    public static FSError findNested(Throwable top) {
        for (Throwable t = top; t != null; t = t.getCause()) {
            if (t instanceof FSError)
                return (FSError) t;
        }
        return null;
    }
}
