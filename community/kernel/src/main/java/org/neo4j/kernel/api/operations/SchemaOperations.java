/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.api.operations;

import org.neo4j.helpers.Function;
import org.neo4j.kernel.api.ConstraintViolationKernelException;
import org.neo4j.kernel.api.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;

public interface SchemaOperations
{
    /**
     * Adds a {@link org.neo4j.kernel.impl.nioneo.store.IndexRule} to the database which applies globally on both
     * existing as well as new data.
     */
    IndexRule addIndexRule( long labelId, long propertyKey ) throws ConstraintViolationKernelException;

    /**
     * Returns the index rule for the given labelId and propertyKey.
     */
    IndexRule getIndexRule( long labelId, long propertyKey ) throws SchemaRuleNotFoundException;

    /**
     * Returns the index descriptor for the given indexId.
     */
    IndexDescriptor getIndexDescriptor( long indexId ) throws IndexNotFoundKernelException;

    /**
     * Get all indexes for a label.
     */
    Iterable<IndexRule> getIndexRules( long labelId );

    /**
     * Returns all index rules.
     */
    Iterable<IndexRule> getIndexRules();

    /**
     * Retrieve the state of an index.
     */
    InternalIndexState getIndexState( IndexRule indexRule ) throws IndexNotFoundKernelException;

    /**
     * Drops a {@link IndexRule} from the database
     */
    void dropIndexRule( IndexRule indexRule ) throws ConstraintViolationKernelException;

    /**
     * The schema state is flushed when ever the schema is updated. If you build objects
     * the rely on the current state of the schema, use this to make sure you don't use
     * outdated schema information.
     *
     * Additionally, schema state entries are evicted using an LRU policy. The size
     * of the LRU cache is determined by GraphDatabaseSettings.query_cache_size
     *
     * NOTE: This currently is solely used by Cypher and might or might not be turned into
     * a more generic facility in teh future
     */
    <K, V> V getOrCreateFromSchemaState( K key, Function<K, V> creator );

    /**
     * Check if some key is in the schema state.
     */
    <K> boolean schemaStateContains( K key );
}
