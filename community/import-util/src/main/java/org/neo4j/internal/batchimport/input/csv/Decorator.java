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
package org.neo4j.internal.batchimport.input.csv;

import java.util.function.Function;
import org.neo4j.batchimport.api.input.InputEntityVisitor;
import org.neo4j.graphdb.Resource;

public interface Decorator extends Function<InputEntityVisitor, InputEntityVisitor>, Resource {
    /**
     * @return whether or not this decorator is mutable. This is important because a state-less decorator
     * can be called from multiple parallel processing threads. A mutable decorator has to be called by
     * a single thread and may incur a performance penalty.
     */
    default boolean isMutable() {
        return false;
    }

    @Override
    default void close() { // Nothing to close by default
    }
}
