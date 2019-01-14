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

import org.neo4j.cursor.RawCursor;
import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Hit;
import org.neo4j.index.internal.gbptree.Layout;

public class NativeAllEntriesReader<KEY extends NativeSchemaKey<KEY>,VALUE extends NativeSchemaValue> implements BoundedIterable<Long>
{
    private final GBPTree<KEY,VALUE> tree;
    private final Layout<KEY,VALUE> layout;
    private RawCursor<Hit<KEY,VALUE>,IOException> seeker;

    NativeAllEntriesReader( GBPTree<KEY,VALUE> tree, Layout<KEY,VALUE> layout )
    {
        this.tree = tree;
        this.layout = layout;
    }

    @Override
    public Iterator<Long> iterator()
    {
        KEY from = layout.newKey();
        from.initAsLowest();
        KEY to = layout.newKey();
        to.initAsHighest();
        try
        {
            closeSeeker();
            seeker = tree.seek( from, to );
            return new PrefetchingIterator<Long>()
            {
                @Override
                protected Long fetchNextOrNull()
                {
                    try
                    {
                        return seeker.next() ? seeker.get().key().getEntityId() : null;
                    }
                    catch ( IOException e )
                    {
                        throw new UncheckedIOException( e );
                    }
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
