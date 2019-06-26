/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.register.Register;
import org.neo4j.values.storable.Value;

/**
 * Surface for getting schema information, such as fetching specific indexes or constraints.
 */
public interface SchemaRead extends SchemaReadCore
{
    /**
     * Acquire a reference to the index mapping the given {@code label} and {@code properties}.
     *
     * @param label the index label
     * @param properties the index properties
     * @return the IndexReference, or {@link IndexReference#NO_INDEX} if such an index does not exist.
     */
    IndexReference index( int label, int... properties );

    /**
     * Acquire an index reference of the given {@code label} and {@code properties}. This method does not assert
     * that the created reference points to a valid online index.
     *
     * @param label the index label
     * @param properties the index properties
     * @return a IndexReference for the given label and properties
     */
    IndexReference indexReferenceUnchecked( int label, int... properties );

    /**
     * Acquire an index reference of the given {@link SchemaDescriptor}. This method does not assert
     * that the created reference points to a valid online index.
     *
     * @param schema {@link SchemaDescriptor} for the index.
     * @return a IndexReference for the given schema.
     */
    IndexReference indexReferenceUnchecked( SchemaDescriptor schema );

    /**
     * Returns the index with the given name
     *
     * @param name The name of the index you are looking for
     * @return The index associated with the given name
     */
    IndexReference indexGetForName( String name );

    /**
     * Get the index id (the id or the schema rule record) for a committed index
     * - throws exception for indexes that aren't committed.
     */
    long indexGetCommittedId( IndexReference index ) throws SchemaKernelException;

    /**
     * Computes the selectivity of the unique values.
     *
     * @param index The index of interest
     * @return The selectivity of the given index
     * @throws IndexNotFoundKernelException if the index is not there
     */
    double indexUniqueValuesSelectivity( IndexReference index ) throws IndexNotFoundKernelException;

    /**
     * Returns the size of the index.
     *
     * @param index The index of interest
     * @return The size of the current index
     * @throws IndexNotFoundKernelException if the index is not there
     */
    long indexSize( IndexReference index ) throws IndexNotFoundKernelException;

    /**
     * Count the number of index entries for the given nodeId and value.
     *
     * @param index The index of interest
     * @param nodeId node id to match.
     * @param propertyKeyId the indexed property to look at (composite indexes apply to more than one property, so we need to specify this)
     * @param value the property value
     * @return number of index entries for the given {@code nodeId} and {@code value}.
     */
    long nodesCountIndexed( IndexReference index, long nodeId, int propertyKeyId, Value value ) throws KernelException;

    /**
     * Returns how many updates that have been applied to the index since the last sampling, and total index size at the last sampling.
     *
     * Results are written to a {@link Register.DoubleLongRegister}, writing the update count into the first long, and
     * the size into the second.
     *
     * @param index The index of interest
     * @param target A {@link Register.DoubleLongRegister} to which to write the update count and size.
     * @return {@code target}
     * @throws IndexNotFoundKernelException if the index does not exist.
     */
    Register.DoubleLongRegister indexUpdatesAndSize( IndexReference index, Register.DoubleLongRegister target )
            throws IndexNotFoundKernelException;

    /**
     * Returns the number of unique entries and the total number of entries in an index.
     *
     * Results are written to a {@link Register.DoubleLongRegister}, writing the number of unique entries into
     * the first long, and the total number of entries into the second.
     *
     * @param index The index of interest
     * @param target A {@link Register.DoubleLongRegister} to which to write the entry counts.
     * @return {@code target}
     * @throws IndexNotFoundKernelException if the index does not exist.
     */
    Register.DoubleLongRegister indexSample( IndexReference index, Register.DoubleLongRegister target )
            throws IndexNotFoundKernelException;

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
    Long indexGetOwningUniquenessConstraintId( IndexReference index );

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
}
