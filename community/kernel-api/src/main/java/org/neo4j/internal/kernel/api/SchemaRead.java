/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.internal.kernel.api;

import java.util.Iterator;
import java.util.function.Function;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.values.storable.Value;

/**
 * Surface for getting schema information, such as fetching specific indexes or constraints.
 */
public interface SchemaRead extends SchemaReadCore
{
    /**
     * Acquire a reference to the index mapping the given {@code schema}, but without requiring a transaction to be open, and without taking any schema locks.
     *
     * @param schema The schema for which to look up an index.
     * @return An index matching the schema, or {@link IndexDescriptor#NO_INDEX} if no such index was found or something went wrong.
     */
    Iterator<IndexDescriptor> indexForSchemaNonTransactional( SchemaDescriptor schema );

    /**
     * Computes the selectivity of the unique values.
     *
     * @param index The index of interest
     * @return The selectivity of the given index
     * @throws IndexNotFoundKernelException if the index is not there
     */
    double indexUniqueValuesSelectivity( IndexDescriptor index ) throws IndexNotFoundKernelException;

    /**
     * Returns the size of the index.
     *
     * @param index The index of interest
     * @return The size of the current index
     * @throws IndexNotFoundKernelException if the index is not there
     */
    long indexSize( IndexDescriptor index ) throws IndexNotFoundKernelException;

    /**
     * Count the number of index entries for the given nodeId and value.
     *
     * @param index The index of interest
     * @param nodeId node id to match.
     * @param propertyKeyId the indexed property to look at (composite indexes apply to more than one property, so we need to specify this)
     * @param value the property value
     * @return number of index entries for the given {@code nodeId} and {@code value}.
     */
    long nodesCountIndexed( IndexDescriptor index, long nodeId, int propertyKeyId, Value value ) throws KernelException;

    /**
     * Returns the index sample info.
     *
     * @param index The index of interest
     * @return index sample info
     * @throws IndexNotFoundKernelException if the index does not exist.
     */
    IndexSample indexSample( IndexDescriptor index ) throws IndexNotFoundKernelException;

    /**
     * Finds all constraints for the given schema
     *
     * @param descriptor The descriptor of the schema
     * @return All constraints for the given schema
     */
    Iterator<ConstraintDescriptor> constraintsGetForSchema( SchemaDescriptor descriptor );

    /**
     * Checks if a constraint exists
     *
     * @param descriptor The descriptor of the constraint to check.
     * @return {@code true} if the constraint exists, otherwise {@code false}
     */
    boolean constraintExists( ConstraintDescriptor descriptor );

    /**
     * Produce a snapshot of the current schema, which can be accessed without acquiring any schema locks.
     * <p>
     * This is useful for inspecting schema elements when you have no intention of updating the schema,
     * and where waiting on schema locks from, for instance, constraint creating transactions,
     * would be inconvenient.
     * <p>
     * The snapshot observes transaction state of the current transaction.
     */
    SchemaReadCore snapshot();

    /**
     * Get the owning constraint for a constraint index or <tt>null</tt> if the index does not have an owning
     * constraint.
     */
    Long indexGetOwningUniquenessConstraintId( IndexDescriptor index );

    /**
     * Returns schema state for the given key or create a new state if not there
     * @param key The key to access
     * @param creator function creating schema state
     * @param <K> type of the key
     * @param <V> type of the schema state value
     * @return the state associated with the key or a new value if non-existing
     */
    <K, V> V schemaStateGetOrCreate( K key, Function<K, V> creator );

    /**
     * Flush the schema state
     */
    void schemaStateFlush();

    /**
     * NOTE this is a temporary method to be able to check if
     * we can use type scans. Will be removed/changed.
     */
    @Deprecated
    boolean relationshipTypeScanStoreEnabled();
}
