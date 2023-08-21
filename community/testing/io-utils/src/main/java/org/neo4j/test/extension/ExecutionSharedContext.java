/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.test.extension;

import java.util.concurrent.ConcurrentHashMap;

public final class ExecutionSharedContext {
    public static final String SHARED_RESOURCE = "sharedContext";
    static final String CREATED_TEST_FILE_PAIRS_KEY = "createdTestFilePairs";
    static final String LOCKED_TEST_FILE_KEY = "lockedFileName";
    static final String SUCCESSFUL_TEST_FILE_KEY = "successfulFileName";
    private static final ConcurrentHashMap<String, Object> map = new ConcurrentHashMap<>();

    public static final ExecutionSharedContext CONTEXT = new ExecutionSharedContext();

    private ExecutionSharedContext() {}

    public static void clear() {
        map.clear();
    }

    @SuppressWarnings("unchecked")
    public static <T> T getValue(String key) {
        return (T) map.get(key);
    }

    public static void setValue(String key, Object value) {
        map.put(key, value);
    }
}
