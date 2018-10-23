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
package org.neo4j.kernel.api.impl.schema.reader;

import org.apache.lucene.document.Document;

import java.util.Iterator;
import java.util.function.ToLongFunction;

import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;

public class LuceneAllEntriesIndexAccessorReader implements BoundedIterable<Long>
{
    private final BoundedIterable<Document> documents;
    private final ToLongFunction<Document> entityIdReader;
    private final long fromIdInclusive;
    private final long toIdExclusive;

    public LuceneAllEntriesIndexAccessorReader( BoundedIterable<Document> documents, ToLongFunction<Document> entityIdReader,
            long fromIdInclusive, long toIdExclusive )
    {
        this.documents = documents;
        this.entityIdReader = entityIdReader;
        this.fromIdInclusive = fromIdInclusive;
        this.toIdExclusive = toIdExclusive;
    }

    @Override
    public long maxCount()
    {
        return documents.maxCount();
    }

    @Override
    public Iterator<Long> iterator()
    {
        Iterator<Document> iterator = documents.iterator();
        return new PrefetchingIterator<Long>()
        {
            @Override
            protected Long fetchNextOrNull()
            {
                do
                {
                    if ( !iterator.hasNext() )
                    {
                        return null;
                    }
                    long id = entityIdReader.applyAsLong( iterator.next() );
                    if ( id >= fromIdInclusive && id < toIdExclusive )
                    {
                        return id;
                    }
                }
                while ( true );
            }
        };
    }

    @Override
    public void close() throws Exception
    {
        documents.close();
    }

}
