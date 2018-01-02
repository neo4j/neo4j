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
package org.neo4j.kernel.api.impl.index;

import java.util.Iterator;

import org.apache.lucene.document.Document;

import org.neo4j.kernel.api.direct.BoundedIterable;

public class LuceneAllEntriesIndexAccessorReader implements BoundedIterable<Long>
{
    private final BoundedIterable<Document> documents;
    private final LuceneDocumentStructure documentLogic;

    public LuceneAllEntriesIndexAccessorReader( BoundedIterable<Document> documents, LuceneDocumentStructure documentLogic )
    {
        this.documents = documents;
        this.documentLogic = documentLogic;
    }

    @Override
    public long maxCount()
    {
        return documents.maxCount();
    }

    @Override
    public Iterator<Long> iterator()
    {
        final Iterator<Document> iterator = documents.iterator();
        return new Iterator<Long>()
        {
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            public Long next()
            {
                return parse( iterator.next() );
            }

            public void remove()
            {
                iterator.remove();
            }
        };
    }

    @Override
    public void close() throws Exception
    {
        documents.close();
    }

    private Long parse( Document document )
    {
        return documentLogic.getNodeId( document );
    }

}
