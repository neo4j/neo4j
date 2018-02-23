/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.fulltext.lucene;

import java.io.IOException;

import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.kernel.api.impl.index.WritableAbstractDatabaseIndex;
import org.neo4j.kernel.api.impl.schema.writer.PartitionedIndexWriter;

public class WritableFulltext extends WritableAbstractDatabaseIndex<LuceneFulltext>
{
    private PartitionedIndexWriter indexWriter;

    public WritableFulltext( LuceneFulltext luceneFulltext )
    {
        super( luceneFulltext );
    }

    @Override
    public void open() throws IOException
    {
        super.open();
        indexWriter = luceneIndex.getIndexWriter( this );
    }

    @Override
    public void close() throws IOException
    {
        super.close();
        indexWriter = null;
    }

    @Override
    public void drop() throws IOException
    {
        super.drop();
        indexWriter = null;
    }


    public void markAsOnline()
    {
        luceneIndex.markAsOnline();
    }

    public PartitionedIndexWriter getIndexWriter()
    {
        return indexWriter;
    }

    public void setFailed( String failure )
    {
        // TODO how to handle that string?
        luceneIndex.setFailed();
    }

    public ReadOnlyFulltext getIndexReader() throws IOException
    {
        return luceneIndex.getIndexReader();
    }

    InternalIndexState getState()
    {
        return luceneIndex.getState();
    }
}
