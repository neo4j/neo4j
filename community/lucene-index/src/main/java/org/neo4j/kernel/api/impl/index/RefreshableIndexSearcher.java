/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static org.apache.lucene.index.IndexReader.open;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;

/**
 * Logic for keeping an up to date {@link IndexSearcher} given an initial {@link IndexWriter}.
 */
public class RefreshableIndexSearcher
{
    private volatile IndexReader reader;
    private volatile IndexSearcher searcher;

    public RefreshableIndexSearcher( IndexWriter writer ) throws IOException
    {
        assignReader( open( writer, true ) );
    }
    
    private void assignReader( IndexReader reader )
    {
        this.reader = reader;
        this.searcher = new IndexSearcher( reader );
    }

    public void refresh()
    {
        try
        {
            IndexReader newReader = IndexReader.openIfChanged( reader );
            if ( newReader != null )
                assignReader( newReader );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    public IndexSearcher getUpToDateSearcher()
    {
        return searcher;
    }
}
