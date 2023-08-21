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
package org.neo4j.test.mockito.mock;

import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.neo4j.graphdb.NotFoundException;

public class Properties implements Answer<Object>, Iterable<String> {
    public static Properties properties(Property... properties) {
        return new Properties(properties);
    }

    public static Properties properties(Map<String, Object> properties) {
        return new Properties(properties);
    }

    private final SortedMap<String, Object> propertiesMap = new TreeMap<>();

    private Properties(Property[] properties) {
        for (Property property : properties) {
            this.propertiesMap.put(property.key(), property.value());
        }
    }

    private Properties(Map<String, Object> properties) {
        this.propertiesMap.putAll(properties);
    }

    @Override
    public Object answer(InvocationOnMock invocation) {
        Object[] arguments = invocation.getArguments();
        @SuppressWarnings("SuspiciousMethodCalls")
        Object result = propertiesMap.get(arguments[0]);
        if (result == null) {
            if (arguments.length == 2) {
                return arguments[1];
            } else {
                throw new NotFoundException();
            }
        }
        return result;
    }

    @Override
    public Iterator<String> iterator() {
        return propertiesMap.keySet().iterator();
    }

    public SortedMap<String, Object> getProperties() {
        return propertiesMap;
    }
}
