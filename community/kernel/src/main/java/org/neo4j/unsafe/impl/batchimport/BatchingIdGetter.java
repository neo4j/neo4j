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
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.id.IdRange;
import org.neo4j.kernel.impl.store.id.IdRangeIterator;
import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.kernel.impl.store.id.validation.IdValidator;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

/**
 * Exposes batches of ids from a {@link RecordStore} as a {@link PrimitiveLongIterator}.
 * It makes use of {@link IdSequence#nextIdBatch(int)} (with default batch size the number of records per page)
 * and caches that batch, exhausting it in {@link #next()} before getting next batch.
 */
public class BatchingIdGetter extends PrimitiveLongCollections.PrimitiveLongBaseIterator implements IdSequence
{
    private final IdSequence source;
    private IdRangeIterator batch;
    private final int batchSize;

    public BatchingIdGetter( RecordStore<? extends AbstractBaseRecord> source )
    {
        this( source, source.getRecordsPerPage() );
    }

    public BatchingIdGetter( RecordStore<? extends AbstractBaseRecord> source, int batchSize )
    {
        this.source = source;
        this.batchSize = batchSize;
    }

    @Override
    protected boolean fetchNext()
    {
        return next( nextId() );
    }

    @Override
    public long nextId()
    {
        long id;
        if ( batch == null || (id = batch.nextId()) == -1 )
        {
            IdRange idRange = source.nextIdBatch( batchSize );
            while ( IdValidator.hasReservedIdInRange( idRange.getRangeStart(), idRange.getRangeStart() + idRange.getRangeLength() ) )
            {
                idRange = source.nextIdBatch( batchSize );
            }
            batch = new IdRangeIterator( idRange );
            id = batch.nextId();
        }
        return id;
    }

    @Override
    public IdRange nextIdBatch( int size )
    {
        throw new UnsupportedOperationException();
    }
}
