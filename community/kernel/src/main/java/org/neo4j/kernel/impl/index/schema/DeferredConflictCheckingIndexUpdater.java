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
package org.neo4j.kernel.impl.index.schema;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.values.storable.ValueTuple;

import static org.neo4j.internal.kernel.api.IndexQuery.exact;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.kernel.api.QueryContext.NULL_CONTEXT;
import static org.neo4j.storageengine.api.UpdateMode.REMOVED;

/**
 * This deferring conflict checker solves e.g. a problem of applying updates to an index that is aware of,
 * and also prevents, duplicates while applying. Consider this scenario:
 *
 * <pre>
 *    GIVEN:
 *    Node A w/ property value P
 *    Node B w/ property value Q
 *
 *    WHEN Applying a transaction that:
 *    Sets A property value to Q
 *    Deletes B
 * </pre>
 *
 * Then an index that is conscious about conflicts when applying may see intermediary conflicts,
 * depending on the order in which updates are applied. Remembering which value tuples have been altered and
 * checking conflicts for those in {@link #close()} works around that problem.
 *
 * This updater wrapping should only be used in specific places to solve specific problems, not generally
 * when applying updates to online indexes.
 */
public class DeferredConflictCheckingIndexUpdater implements IndexUpdater
{
    private final IndexUpdater actual;
    private final Supplier<IndexReader> readerSupplier;
    private final IndexDescriptor indexDescriptor;
    private final PageCursorTracer cursorTracer;
    private final Set<ValueTuple> touchedTuples = new HashSet<>();

    public DeferredConflictCheckingIndexUpdater( IndexUpdater actual, Supplier<IndexReader> readerSupplier, IndexDescriptor indexDescriptor,
            PageCursorTracer cursorTracer )
    {
        this.actual = actual;
        this.readerSupplier = readerSupplier;
        this.indexDescriptor = indexDescriptor;
        this.cursorTracer = cursorTracer;
    }

    @Override
    public void process( IndexEntryUpdate<?> update ) throws IndexEntryConflictException
    {
        ValueIndexEntryUpdate<?> valueUpdate = asValueUpdate( update );
        actual.process( valueUpdate );
        if ( valueUpdate.updateMode() != REMOVED )
        {
            touchedTuples.add( ValueTuple.of( valueUpdate.values() ) );
        }
    }

    @Override
    public void close() throws IndexEntryConflictException
    {
        actual.close();
        try ( IndexReader reader = readerSupplier.get() )
        {
            for ( ValueTuple tuple : touchedTuples )
            {
                try ( NodeValueIterator client = new NodeValueIterator() )
                {
                    reader.query( NULL_CONTEXT, client, unconstrained(), queryOf( tuple ) );
                    if ( client.hasNext() )
                    {
                        long firstEntityId = client.next();
                        if ( client.hasNext() )
                        {
                            long secondEntityId = client.next();
                            throw new IndexEntryConflictException( firstEntityId, secondEntityId, tuple );
                        }
                    }
                }
            }
        }
        catch ( IndexNotApplicableKernelException e )
        {
            throw new IllegalArgumentException( "Unexpectedly the index reader couldn't handle this query", e );
        }
    }

    private IndexQuery[] queryOf( ValueTuple tuple )
    {
        IndexQuery[] predicates = new IndexQuery[tuple.size()];
        int[] propertyIds = indexDescriptor.schema().getPropertyIds();
        for ( int i = 0; i < predicates.length; i++ )
        {
            predicates[i] = exact( propertyIds[i], tuple.valueAt( i ) );
        }
        return predicates;
    }
}
