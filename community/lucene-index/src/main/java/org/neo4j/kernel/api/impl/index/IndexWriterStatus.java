/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

class IndexWriterStatus
{
    private static final String KEY_STATUS = "status";
    private static final String ONLINE = "online";

    public void commitAsOnline( LuceneIndexWriter writer ) throws IOException
    {
        writer.commit( stringMap( KEY_STATUS, ONLINE ) );
    }

    public boolean isOnline( Directory directory ) throws IOException
    {
        if ( !IndexReader.indexExists( directory ) )
            return false;

        IndexReader reader = null;
        try
        {
            reader = IndexReader.open( directory );
            Map<String, String> userData = reader.getIndexCommit().getUserData();
            return ONLINE.equals( userData.get( KEY_STATUS ) );
        }
        finally
        {
            if ( reader != null )
            {
                reader.close();
            }
        }
    }

    public void close( LuceneIndexWriter writer ) throws IOException
    {
        writer.close( true );
    }
}
