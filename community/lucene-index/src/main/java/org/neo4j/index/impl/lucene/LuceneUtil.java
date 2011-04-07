/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.index.impl.lucene;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

abstract class LuceneUtil
{
    static void close( IndexWriter writer )
    {
        close( (Object) writer );
    }
    
    static void close( IndexSearcher searcher )
    {
        close( (Object) searcher );
    }
    
    private static void close( Object object )
    {
        if ( object == null )
        {
            return;
        }
        
        try
        {
            if ( object instanceof IndexWriter )
            {
                ((IndexWriter) object).close();
            }
            else if ( object instanceof IndexSearcher )
            {
                ((IndexSearcher) object).close();
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    static void strictAddDocument( IndexWriter writer, Document document )
    {
        try
        {
            writer.addDocument( document );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    static void strictRemoveDocument( IndexWriter writer, Query query )
    {
        try
        {
            writer.deleteDocuments( query );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
