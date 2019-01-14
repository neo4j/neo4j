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
package org.neo4j.unsafe.impl.batchimport.store;

import java.util.function.LongFunction;

import org.neo4j.kernel.impl.store.id.IdRange;
import org.neo4j.kernel.impl.store.id.IdSequence;

/**
 * Assumes that records have been allocated such that there will be a free record, right after a given record,
 * to place the secondary unit of that record.
 */
public class SecondaryUnitPrepareIdSequence implements PrepareIdSequence
{
    @Override
    public LongFunction<IdSequence> apply( IdSequence idSequence )
    {
        return new NeighbourIdSequence();
    }

    private static class NeighbourIdSequence implements LongFunction<IdSequence>, IdSequence
    {
        private long id;
        private boolean returned;

        @Override
        public IdSequence apply( long firstUnitId )
        {
            this.id = firstUnitId;
            returned = false;
            return this;
        }

        @Override
        public long nextId()
        {
            try
            {
                if ( returned )
                {
                    throw new IllegalStateException( "Already returned" );
                }
                return id + 1;
            }
            finally
            {
                returned = true;
            }
        }

        @Override
        public IdRange nextIdBatch( int size )
        {
            throw new UnsupportedOperationException();
        }
    }
}
