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
package org.neo4j.kernel.impl.index.schema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;

import static org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier.TRACER_SUPPLIER;

public class NativeAllEntriesReader<KEY extends NativeIndexKey<KEY>,VALUE extends NativeIndexValue> implements BoundedIterable<Long>
{
    private final GBPTree<KEY,VALUE> tree;
    private final Layout<KEY,VALUE> layout;
    private final long fromIdInclusive;
    private final long toIdExclusive;
    private Seeker<KEY,VALUE> seeker;

    NativeAllEntriesReader( GBPTree<KEY,VALUE> tree, Layout<KEY,VALUE> layout, long fromIdInclusive, long toIdExclusive )
    {
        this.tree = tree;
        this.layout = layout;
        this.fromIdInclusive = fromIdInclusive;
        this.toIdExclusive = toIdExclusive;
    }

    @Override
    public Iterator<Long> iterator()
    {
        KEY from = layout.newKey();
        from.initialize( Long.MIN_VALUE );
        from.initValuesAsLowest();
        KEY to = layout.newKey();
        to.initialize( Long.MAX_VALUE );
        to.initValuesAsHighest();
        try
        {
            closeSeeker();
            seeker = tree.seek( from, to, TRACER_SUPPLIER.get() );
            return new PrefetchingIterator<>()
            {
                @Override
                protected Long fetchNextOrNull()
                {
                    do
                    {
                        try
                        {
                            if ( !seeker.next() )
                            {
                                return null;
                            }
                            long id = seeker.key().getEntityId();
                            if ( id >= fromIdInclusive && id < toIdExclusive )
                            {
                                return id;
                            }
                        }
                        catch ( IOException e )
                        {
                            throw new UncheckedIOException( e );
                        }
                    }
                    while ( true );
                }
            };
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private void closeSeeker() throws IOException
    {
        if ( seeker != null )
        {
            seeker.close();
            seeker = null;
        }
    }

    @Override
    public void close() throws Exception
    {
        closeSeeker();
    }

    @Override
    public long maxCount()
    {
        return UNKNOWN_MAX_COUNT;
    }
}
