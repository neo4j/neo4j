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
package org.neo4j.graphdb.schema;

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.api.index.IndexDescriptor;

/**
 * Interface for managing the schema of your graph database. This currently includes
 * the indexing support added in Neo4j 2.0. Please see the Neo4j manual for details.
 *
 * Compatibility note: New methods may be added to this interface without notice,
 * backwards compatibility is only guaranteed for clients of this interface, not for
 * implementors.
 */
public interface Schema
{
    /**
     * The states that an index can be in. This mostly relates to tracking the background
     * population of an index, to tell when it is done populating and is online serving
     * requests.
     */
    enum IndexState
    {
        ONLINE,
        POPULATING,
        FAILED
    }

    /**
     * Returns an {@link IndexCreator} where details about the index to create can be
     * specified. When all details have been entered {@link IndexCreator#create() create}
     * must be called for it to actually be created.
     * 
     * Creating an index enables indexing for nodes with the specified label. The index will
     * have the details supplied to the {@link IndexCreator returned index creator}.
     * All existing and all future nodes matching the index definition will be indexed,
     * speeding up future operations.
     *
     * @param label {@link Label label} on nodes to be indexed
     *
     * @return an {@link IndexCreator} capable of providing details for, as well as creating
     * an index for the given {@link Label label}.
     */
    IndexCreator indexFor( Label label );
    
    /**
     * @param label the {@link Label} to get {@link IndexDefinition indexes} for.
     * @return all {@link IndexDefinition indexes} attached to the given {@link Label label}.
     */
    Iterable<IndexDefinition> getIndexes( Label label );
    
    /**
     * @return all {@link IndexDefinition indexes} in this database.
     */
    Iterable<IndexDefinition> getIndexes();

    /**
     * Poll the database for the state of a given index. This can be used to track in which
     * state the creation of the index is, for example if it's still
     * {@link IndexState#POPULATING populating} in the background, or has come
     * {@link IndexState#ONLINE online}.
     *
     * @param index the index that we want to poll state for
     * @return the current {@link IndexState} of the index
     */
    IndexState getIndexState( IndexDefinition index );
    
    /**
     * If {@link #getIndexState(IndexDefinition)} return {@link IndexState#FAILED} this method will
     * return the failure description.
     * @param index the {@link IndexDescriptor} to get failure from.
     * @return the failure description.
     * @throws IllegalStateException if the {@code index} isn't in a {@link IndexState#FAILED} state.
     */
    String getIndexFailure( IndexDefinition index );

    /**
     * Returns a {@link ConstraintCreator} where details about the constraint can be
     * specified. When all details have been entered {@link ConstraintCreator#create()}
     * must be called for it to actually be created.
     *
     * Creating a constraint will block on the {@linkplain ConstraintCreator#create() create method} until
     * all existing data has been verified for compliance. If any existing data doesn't comply with the constraint an
     * exception will be thrown, and the constraint will not be created.
     * 
     * @param label the label this constraint is for.
     * @return a {@link ConstraintCreator} capable of providing details for, as well as creating
     * a constraint for the given {@linkplain Label label}.
     */
    ConstraintCreator constraintFor( Label label );
    
    /**
     * @param label the {@linkplain Label label} to get constraints for.
     * @return all constraints for the given label.
     */
    Iterable<ConstraintDefinition> getConstraints( Label label );

    /**
     * @param type the {@linkplain RelationshipType relationship type} to get constraints for.
     * @return all constraints for the given relationship type.
     */
    Iterable<ConstraintDefinition> getConstraints( RelationshipType type );

    /**
     * @return all constraints
     */
    Iterable<ConstraintDefinition> getConstraints();

    /**
     * Wait until an index comes online
     * 
     * @param index the index that we want to wait for
     * @param duration duration to wait for the index to come online
     * @param unit TimeUnit of duration
     * @throws IllegalStateException if the index did not enter the ONLINE state
     *             within the given duration or if the index entered the FAILED
     *             state
     */
    void awaitIndexOnline( IndexDefinition index, long duration, TimeUnit unit );

    /**
     * Wait until all indices comes online
     * 
     * @param duration duration to wait for all indexes to come online
     * @param unit TimeUnit of duration
     * @throws IllegalStateException if some index did not enter the ONLINE
     *             state within the given duration or if the index entered the
     *             FAILED state
     */
    void awaitIndexesOnline( long duration, TimeUnit unit );
}
