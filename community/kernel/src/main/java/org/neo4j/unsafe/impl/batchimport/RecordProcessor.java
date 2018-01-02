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
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

/**
 * Generic processor of {@link AbstractBaseRecord} from a store.
 *
 * @param <T>
 */
public interface RecordProcessor<T extends AbstractBaseRecord>
{
    /**
     * Processes an item.
     *
     * @return {@code true} if processing this item resulted in changes that should be updated back to the source.
     */
    boolean process( T item );

    void done();

    public static class Multiple<T extends AbstractBaseRecord> implements RecordProcessor<T>
    {
        private final RecordProcessor<T>[] processors;

        @SafeVarargs
        public Multiple( RecordProcessor<T>... processors )
        {
            this.processors = processors;
        }

        @Override
        public boolean process( T item )
        {
            boolean result = false;
            for ( RecordProcessor<T> processor : processors )
            {
                result |= processor.process( item );
            }
            return result;
        }

        @Override
        public void done()
        {
            for ( RecordProcessor<T> processor : processors )
            {
                processor.done();
            }
        }
    }
}
