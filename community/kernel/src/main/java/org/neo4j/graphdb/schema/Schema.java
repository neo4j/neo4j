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
package org.neo4j.graphdb.schema;

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.ResourceIterable;

/**
 * Interface for managing the schema of your graph database. This currently includes
 * the new indexing support, added in Neo4j 2.0, please see the Neo4j manual for details.
 */
public interface Schema
{
    /**
     * The states that an index can be in. This mostly relates to tracking the background
     * population of an index, to tell when it is done populating and is online serving
     * requests.
     */
    public static enum IndexState
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
     * @return an {@link IndexCreator} capable of providing details for, as well as creating
     * an index for the given {@link Label label}.
     */
    IndexCreator indexCreator( Label label );

    /**
     * @param label the {@link Label} to get {@link IndexDefinition indexes} for.
     * @return all {@link IndexDefinition indexes} attached to the given {@link Label label}.
     */
    ResourceIterable<IndexDefinition> getIndexes( Label label );
    
    /**
     * @return all {@link IndexDefinition indexes} in this database.
     */
    ResourceIterable<IndexDefinition> getIndexes();

    /**
     * Poll the database for the state of a given index. This can be used to track
     * when, during creation of a new index, an index is done populating itself and
     * comes online to serve requests.
     *
     * @param index the index that we want to poll state for
     * @return the current {@link IndexState} of the index
     */
    IndexState getIndexState( IndexDefinition index );

    /**
     * Wait until an index comes online
     *
     * @param index the index that we want to wait for
     * @param duration duration to wait for the index to come online
     * @param unit TimeUnit of duration
     * @throws IllegalStateException if the index did not enter the ONLINE state within the given duration or
     * if the index entered the FAILED state
     */
    void awaitIndexOnline( IndexDefinition index, long duration, TimeUnit unit );
}
