/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api.operations;

import org.neo4j.function.Function;
import org.neo4j.kernel.impl.api.KernelStatement;

public interface SchemaStateOperations
{
    /**
     * The schema state is flushed when ever the schema is updated. If you build objects
     * the rely on the current state of the schema, use this to make sure you don't use
     * outdated schema information.
     *
     * Additionally, schema state entries are evicted using an LRU policy. The size
     * of the LRU cache is determined by GraphDatabaseSettings.query_cache_size
     *
     * NOTE: This currently is solely used by Cypher and might or might not be turned into
     * a more generic facility in the future
     */
    <K, V> V schemaStateGetOrCreate( KernelStatement state, K key, Function<K, V> creator );

    /**
     * Check if some key is in the schema state.
     */
    <K> boolean schemaStateContains( KernelStatement state, K key );

    /**
     * Flush the schema state.
     */
    void schemaStateFlush( KernelStatement state );
}
