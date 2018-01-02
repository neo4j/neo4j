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
package org.neo4j.kernel.api.index;

import java.io.IOException;

import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.kernel.impl.api.index.SwallowingIndexUpdater;
import org.neo4j.kernel.impl.api.index.UpdateMode;

import static org.neo4j.register.Register.DoubleLong;

/**
 * Used for initial population of an index.
 */
public interface IndexPopulator
{
    /**
     * Remove all data in the index and paves the way for populating an index.
     */
    void create() throws IOException;

    /**
     * Closes and deletes this index.
     */
    void drop() throws IOException;

    /**
     * Called when initially populating an index over existing data. Guaranteed to be
     * called by the same thread every time. All data coming in here is guaranteed to not
     * have been added to this index previously, so no checks needs to be performed before applying it.
     * Implementations may verify constraints at this time, or defer them until the first verification
     * of {@link #verifyDeferredConstraints(PropertyAccessor)}.
     *
     * @param nodeId node id to index.
     * @param propertyValue property value for the entry to index.
     */
    void add( long nodeId, Object propertyValue )
            throws IndexEntryConflictException, IOException, IndexCapacityExceededException;

    /**
     * Verify constraints for all entries added so far.
     */
    @Deprecated // TODO we want to remove this in 2.1, and properly prevent value collisions.
    void verifyDeferredConstraints( PropertyAccessor accessor )  throws IndexEntryConflictException, IOException;

    /**
     * Return an updater for applying a set of changes to this index, generally this will be a set of changes from a
     * transaction.
     *
     * Index population goes through the existing data in the graph and feeds relevant data to this populator.
     * Simultaneously as population progresses there might be incoming updates
     * from committing transactions, which needs to be applied as well. This populator will only receive updates
     * for nodes that it already has seen. Updates coming in here must be applied idempotently as the same data
     * may have been {@link #add(long, Object) added previously}.
     * Updates can come in two different {@link NodePropertyUpdate#getUpdateMode() modes}.
     * <ol>
     *   <li>{@link UpdateMode#ADDED} means that there's an added property to a node already seen by this
     *   populator and so needs to be added. Note that this addition needs to be applied idempotently.
     *   <li>{@link UpdateMode#CHANGED} means that there's a change to a property for a node already seen by
     *   this populator and that this new change needs to be applied. Note that this change needs to be
     *   applied idempotently.</li>
     *   <li>{@link UpdateMode#REMOVED} means that a property already seen by this populator or even the node itself
     *   has been removed and need to be removed from this index as well. Note that this removal needs to be
     *   applied idempotently.</li>
     * </ol>
     */
    IndexUpdater newPopulatingUpdater( PropertyAccessor accessor ) throws IOException;

    // void update( Iterable<NodePropertyUpdate> updates ) throws IndexEntryConflictException, IOException;

    // TODO instead of this flag, we should store if population fails and mark indexes as failed internally
    // Rationale: Users should be required to explicitly drop failed indexes

    /**
     * Close this populator and releases any resources related to it.
     * If {@code populationCompletedSuccessfully} is {@code true} then it must mark this index
     * as {@link InternalIndexState#ONLINE} so that future invocations of its parent
     * {@link SchemaIndexProvider#getInitialState(long)} also returns {@link InternalIndexState#ONLINE}.
     */
    void close( boolean populationCompletedSuccessfully ) throws IOException, IndexCapacityExceededException;

    /**
     * Called then a population failed. The failure string should be stored for future retrieval by
     * {@link SchemaIndexProvider#getPopulationFailure(long)}. Called before {@link #close(boolean)}
     * if there was a failure during population.
     *
     * @param failure the description of the failure.
     * @throws IOException if marking failed.
     */
    void markAsFailed( String failure ) throws IOException;

    long sampleResult( DoubleLong.Out result );

    class Adapter implements IndexPopulator
    {
        @Override
        public void create() throws IOException
        {
        }

        @Override
        public void drop() throws IOException
        {
        }

        @Override
        public void add( long nodeId, Object propertyValue ) throws IndexEntryConflictException, IOException
        {
        }

        @Override
        public void verifyDeferredConstraints( PropertyAccessor accessor ) throws IndexEntryConflictException, IOException
        {
        }

        @Override
        public IndexUpdater newPopulatingUpdater( PropertyAccessor accessor )
        {
            return SwallowingIndexUpdater.INSTANCE;
        }

        @Override
        public void close( boolean populationCompletedSuccessfully ) throws IOException
        {
        }

        @Override
        public void markAsFailed( String failure )
        {
        }

        @Override
        public long sampleResult( DoubleLong.Out result )
        {
            result.write( 0l, 0l );
            return 0;
        }
    }
}
