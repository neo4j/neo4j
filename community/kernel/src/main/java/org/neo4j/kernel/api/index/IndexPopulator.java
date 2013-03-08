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
package org.neo4j.kernel.api.index;


/**
 * Used for initial population of an index.
 */
public interface IndexPopulator
{
    /**
     * Remove all data in the index and paves the way for populating an index.
     */
    void create();

    /**
     * Closes and deletes this index.
     */
    void drop();
    
    /**
     * Called when initially populating an index over existing data. Guaranteed to be
     * called by the same thread every time. All data coming in here is guaranteed to not
     * have been added to this index previously, by any method.
     * 
     * @param nodeId node id to index.
     * @param propertyValue property value for the entry to index.
     */
    void add( long nodeId, Object propertyValue );

    /**
     * Apply a set of changes to this index, generally this will be a set of changes from a transaction.
     * @param updates
     */
    void update( Iterable<NodePropertyUpdate> updates );
    
    /**
     * Close this populator and releases any resources related to it.
     * If {@code populationCompletedSuccessfully} is {@code true} then it must mark this index
     * as {@link InternalIndexState#ONLINE} so that future invocations of its parent
     * {@link SchemaIndexProvider#getInitialState(long)} also returns {@link InternalIndexState#ONLINE}.
     */
    void close( boolean populationCompletedSuccessfully );
    
    public static class Adapter implements IndexPopulator
    {
        @Override
        public void create()
        {
        }
        
        @Override
        public void drop()
        {
        }
        
        @Override
        public void add( long nodeId, Object propertyValue )
        {
        }

        @Override
        public void update( Iterable<NodePropertyUpdate> updates )
        {
        }

        @Override
        public void close( boolean populationCompletedSuccessfully )
        {
        }
    }
}
