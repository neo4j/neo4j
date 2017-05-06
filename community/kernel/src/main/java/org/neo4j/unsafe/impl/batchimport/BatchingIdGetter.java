/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import org.neo4j.kernel.impl.store.id.IdRangeIterator;
import org.neo4j.kernel.impl.store.id.IdSequence;

public class BatchingIdGetter extends PrimitiveLongCollections.PrimitiveLongBaseIterator
{
    private final IdSequence source;
    private IdRangeIterator batch;
    private final int batchSize;

    public BatchingIdGetter( IdSequence source )
    {
        this( source, 10_000 );
    }

    public BatchingIdGetter( IdSequence source, int batchSize )
    {
        this.source = source;
        this.batchSize = batchSize;
    }

    @Override
    protected boolean fetchNext()
    {
        long id;
        if ( batch == null || (id = batch.next()) == -1 )
        {
            batch = new IdRangeIterator( source.nextIdBatch( batchSize ) );
            id = batch.next();
        }
        return next( id );
    }
}
