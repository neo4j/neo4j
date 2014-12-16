/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport;

public interface StoreProcessor<T>
{
    /**
     * Processes an item.
     *
     * @return {@code true} if the store should be updated as part of processing this item.
     */
    boolean process( T item );

    void done();

    public static class Multiple<T> implements StoreProcessor<T>
    {
        private final StoreProcessor<T>[] processors;

        @SafeVarargs
        public Multiple( StoreProcessor<T>... processors )
        {
            this.processors = processors;
        }

        @Override
        public boolean process( T item )
        {
            boolean result = false;
            for ( StoreProcessor<T> processor : processors )
            {
                result |= processor.process( item );
            }
            return result;
        }

        @Override
        public void done()
        {
            for ( StoreProcessor<T> processor : processors )
            {
                processor.done();
            }
        }
    }
}
