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
package org.neo4j.graphdb.schema;

import java.util.concurrent.TimeUnit;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

/**
 * Interface for managing the schema of your graph database. This currently includes
 * the indexing support added in Neo4j 2.0. Please see the Neo4j manual for details.
 *
 * Compatibility note: New methods may be added to this interface without notice,
 * backwards compatibility is only guaranteed for clients of this interface, not for
 * implementors.
 */
@PublicApi
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
     * Begin specifying an index for all nodes with the given label.
     *
     * Returns an {@link IndexCreator} where details about the index to create can be
     * specified. When all details have been entered, {@link IndexCreator#create() create}
     * must be called for the index to actually be created.
     *
     * Creating an index enables indexing for nodes with the specified label. The index will
     * have the details supplied to the {@link IndexCreator returned index creator}.
     * All existing and all future nodes matching the index definition will be indexed,
     * speeding up future read operations.
     *
     * @param label {@link Label label} on nodes to be indexed
     *
     * @return an {@link IndexCreator} capable of providing details for, as well as creating
     * an index for the given {@link Label label}.
     */
    IndexCreator indexFor( Label label );

    /**
     * Begin specifying an index for all nodes with any of the given labels.
     *
     * Returns an {@link IndexCreator} where details about the index to create can be
     * specified. When all details have been entered, {@link IndexCreator#create() create}
     * must be called for the index to actually be created.
     *
     * Creating an index enables indexing for nodes with any of the specified labels.
     * The index will have the details supplied to the {@link IndexCreator returned index creator}.
     * All existing and all future nodes matching the index definition will be indexed,
     * speeding up future read operations.
     *
     * This behaves similar to the {@link #indexFor(Label)} method, with the exception that
     * multiple labels can be specified. Doing so will create a so-called
     * {@linkplain IndexDefinition#isMultiTokenIndex() multi-token} index.
     *
     * Note that not all index types support multi-token indexes.
     * See {@link IndexType} for more information.
     *
     * The list of labels may not contain any duplicates.
     *
     * @param labels The list of labels for which nodes should be indexed.
     * @return an {@link IndexCreator} capable of providing details for, as well as creating
     * an index for the given list of {@link Label labels}.
     */
    IndexCreator indexFor( Label... labels );

    /**
     * Begin specifying an index for all relationships with the given relationship type.
     *
     * Returns an {@link IndexCreator} where details about the index to create can be
     * specified. When all details have been entered, {@link IndexCreator#create() create}
     * must be called for the index to actually be created.
     *
     * Creating an index enables indexing for relationships with the specified relationship type.
     * The index will have the details supplied to the {@link IndexCreator returned index creator}.
     * All existing and all future relationships matching the index definition will be indexed,
     * speeding up future read operations.
     *
     * @param type {@link RelationshipType relationship type} on relationships to be indexed.
     * @return an {@link IndexCreator} capable of providing details for, as well as creating
     * an index for the given {@link RelationshipType}.
     */
    IndexCreator indexFor( RelationshipType type );

    /**
     * Begin specifying an index for all relationships with any of the given relationship types.
     *
     * Returns an {@link IndexCreator} where details about the index to create can be
     * specified. When all details have been entered, {@link IndexCreator#create() create}
     * must be called for the index to actually be created.
     *
     * Creating an index enables indexing for relationships with any of the specified relationship types.
     * The index will have the details supplied to the {@link IndexCreator returned index creator}.
     * All existing and all future relationships matching the index definition will be indexes,
     * speeding up future read operations.
     *
     * @param types {@link RelationshipType relationship types} on relationships to be indexed.
     * @return an {@link IndexCreator} capable of providing details for, as well as creating
     * an index for the given {@link RelationshipType RelationshipTypes}.
     */
    IndexCreator indexFor( RelationshipType... types );

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
     * Poll the database for the population progress. This can be used to track the progress of the
     * population job associated to the given index. If the index is
     * {@link IndexState#POPULATING populating} or {@link IndexState#ONLINE online}, the state will contain current
     * progress. If the index is {@link IndexState#FAILED failed} then the state returned from this method
     * should be regarded as invalid.
     *
     * @param index the index that we want to poll state for
     * @return the current population progress for the index
     *
     */
    IndexPopulationProgress getIndexPopulationProgress( IndexDefinition index );

    /**
     * If {@link #getIndexState(IndexDefinition)} return {@link IndexState#FAILED} this method will
     * return the failure description.
     * @param index the {@link IndexDefinition} to get failure from.
     * @return the failure description.
     * @throws IllegalStateException if the {@code index} isn't in a {@link IndexState#FAILED} state.
     */
    String getIndexFailure( IndexDefinition index );

    /**
     * Returns a {@link ConstraintCreator} where details about the constraint can be
     * specified. When all details have been entered, {@link ConstraintCreator#create()}
     * must be called for it to actually be created.
     *
     * Creating a constraint will block on the {@linkplain ConstraintCreator#create() create method} until
     * all existing data has been verified for compliance.
     * If any existing data doesn't comply with the constraint an exception will be thrown,
     * and the constraint will not be created.
     *
     * @param label the label this constraint is for.
     * @return a {@link ConstraintCreator} capable of providing details for, as well as creating
     * a constraint for the given {@linkplain Label label}.
     */
    ConstraintCreator constraintFor( Label label );

    /**
     * Returns a {@link ConstraintCreator} where details about the constraint can be specified.
     * When all details have been entered, {@link ConstraintCreator#create()}
     * must be called for it the actually be created.
     *
     * Creating a constraint will block on the {@linkplain ConstraintCreator#create() create method} until
     * all existing data has been verified for compliance.
     * If any existing data doesn't comply with the constraint an exception will be thrown,
     * and the constraint will not be created.
     *
     * @param type the relationship type this constraint is for.
     * @return a {@link ConstraintCreator} capable of providing details for, as well as creating
     * a constraint for the given {@linkplain RelationshipType}.
     */
    ConstraintCreator constraintFor( RelationshipType type );

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
     * Wait until an index with the given name comes online.
     *
     * @param indexName the name of the index that we want to wait for.
     * @param duration duration to wait for the index to come online
     * @param unit TimeUnit of duration
     * @throws IllegalStateException if the index did not enter the ONLINE state
     * within the given duration, or if the index entered the FAILED state.
     */
    void awaitIndexOnline( String indexName, long duration, TimeUnit unit );

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

    /**
     * Get a {@link ConstraintDefinition} by the given name of the constraint.
     * @param constraintName The name of the constraint.
     * @return The constraint with that name.
     * @throws IllegalArgumentException if there is no constraint with that name.
     */
    ConstraintDefinition getConstraintByName( String constraintName );

    /**
     * Get an {@link IndexDefinition} by the name of the index.
     * @param indexName The name of the index.
     * @return The index with that name.
     * @throws IllegalArgumentException if there is no index with that name.
     */
    IndexDefinition getIndexByName( String indexName );
}
