/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.batchimport;

import org.eclipse.collections.api.iterator.LongIterator;

import java.util.function.LongConsumer;

import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdRange;
import org.neo4j.internal.id.IdRangeIterator;
import org.neo4j.internal.id.IdSequence;
import org.neo4j.internal.id.IdValidator;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

import static java.lang.String.format;
import static org.neo4j.internal.id.IdRangeIterator.VALUE_REPRESENTING_NULL;

/**
 * Exposes batches of ids from a {@link RecordStore} as a {@link LongIterator}.
 * It makes use of {@link IdGenerator#nextIdBatch(int, boolean, PageCursorTracer)} (with default batch size the number of records per page)
 * and caches that batch, exhausting it in {@link #nextId(PageCursorTracer)} before getting next batch.
 */
public class BatchingIdGetter implements IdSequence
{
    private static final long NO_ID_EXPECTATION = -1;

    private final IdGenerator source;
    private IdRangeIterator batch;
    private final int batchSize;
    private long idExpectation = NO_ID_EXPECTATION;

    BatchingIdGetter( CommonAbstractStore<? extends AbstractBaseRecord,?> source )
    {
        this( source, source.getRecordsPerPage() );
    }

    BatchingIdGetter( CommonAbstractStore<? extends AbstractBaseRecord,?> source, int batchSize )
    {
        this.source = source.getIdGenerator();
        this.batchSize = batchSize;
    }

    @Override
    public long nextId( PageCursorTracer cursorTracer )
    {
        long id;
        if ( batch == null || (id = batch.nextId( cursorTracer )) == VALUE_REPRESENTING_NULL )
        {
            idExpectation = NO_ID_EXPECTATION;
            IdRange idRange = source.nextIdBatch( batchSize, true, cursorTracer );
            while ( IdValidator.hasReservedIdInRange( idRange.getRangeStart(), idRange.getRangeStart() + idRange.getRangeLength() ) )
            {
                idRange = source.nextIdBatch( batchSize, true, cursorTracer );
            }
            batch = new IdRangeIterator( idRange );
            id = batch.nextId( cursorTracer );
        }
        if ( idExpectation != NO_ID_EXPECTATION )
        {
            if ( id != idExpectation )
            {
                throw new IllegalStateException(
                        format( "Id generator allocated range with non-consecutive IDs, expected:%d, but got:%d", idExpectation, id ) );
            }
        }
        idExpectation = id + 1;
        return id;
    }

    void visitUnused( LongConsumer visitor, PageCursorTracer cursorTracer )
    {
        if ( batch != null )
        {
            long unusedId;
            while ( (unusedId = batch.nextId( cursorTracer )) != VALUE_REPRESENTING_NULL )
            {
                visitor.accept( unusedId );
            }
        }
    }
}
