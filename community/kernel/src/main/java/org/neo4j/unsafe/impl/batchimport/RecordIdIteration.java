/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

public class RecordIdIteration
{
    public static final PrimitiveLongIterator backwards( long lowIncluded, long highExcluded )
    {
        return new PrimitiveLongCollections.PrimitiveLongBaseIterator()
        {
            private long next = highExcluded - 1;

            @Override
            protected boolean fetchNext()
            {
                return next >= lowIncluded ? next( next-- ) : false;
            }
        };
    }

    public static final PrimitiveLongIterator forwards( long lowIncluded, long highExcluded )
    {
        return new PrimitiveLongCollections.PrimitiveLongBaseIterator()
        {
            private long nextId = lowIncluded;

            @Override
            protected boolean fetchNext()
            {
                return nextId < highExcluded ? next( nextId++ ) : false;
            }
        };
    }

    public static PrimitiveLongIterator allIn( RecordStore<? extends AbstractBaseRecord> store )
    {
        return forwards( store.getNumberOfReservedLowIds(), store.getHighId() );
    }

    public static PrimitiveLongIterator allInReversed( RecordStore<? extends AbstractBaseRecord> store )
    {
        return backwards( store.getNumberOfReservedLowIds(), store.getHighId() );
    }
}
