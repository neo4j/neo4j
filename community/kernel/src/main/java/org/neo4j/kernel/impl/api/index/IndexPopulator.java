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
package org.neo4j.kernel.impl.api.index;

/**
 * Used for initial population of an index.
 */
public interface IndexPopulator
{
    /**
     * Remove all data in the index and paves the way for populating an index.
     */
    void createIndex();

    /**
     * Delete this index
     */
    void dropIndex();
    
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
     * Complete the creation of this index. If this is a persisted index, implementors of this
     */
    void populationCompleted();
    
    public static class Adapter implements IndexPopulator
    {
        @Override
        public void createIndex()
        {
        }
        
        @Override
        public void dropIndex()
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
        public void populationCompleted()
        {
        }
    }
}
