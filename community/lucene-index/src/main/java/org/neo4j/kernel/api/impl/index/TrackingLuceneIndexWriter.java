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

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;

import java.io.IOException;

import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;

/**
 * Overflow aware index writer that tracks number of documents in lucene.
 * Every method that modifies index size might throw
 * {@link org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException}.
 * This writer is intended to be used during index population.
 */
class TrackingLuceneIndexWriter extends LuceneIndexWriter
{
    TrackingLuceneIndexWriter( Directory directory, IndexWriterConfig config ) throws IOException
    {
        super( directory, config );
    }

    @Override
    public void addDocument( Document document ) throws IOException, IndexCapacityExceededException
    {
        checkMaxDoc();
        super.addDocument( document );
    }

    @Override
    public void updateDocument( Term term, Document document ) throws IOException, IndexCapacityExceededException
    {
        checkMaxDoc();
        super.updateDocument( term, document );
    }

    private void checkMaxDoc() throws IOException, IndexCapacityExceededException
    {
        int currentMaxDoc = writer.maxDoc();
        long limit = maxDocLimit();

        if ( currentMaxDoc >= limit )
        {
            throw new IndexCapacityExceededException( currentMaxDoc, limit );
        }
    }
}
