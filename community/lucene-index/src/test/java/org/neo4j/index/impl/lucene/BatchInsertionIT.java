/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static java.lang.System.currentTimeMillis;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.index.impl.lucene.LuceneIndexImplementation.EXACT_CONFIG;

import java.io.File;
import java.util.Iterator;
import java.util.Map;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.index.BatchInserterIndex;
import org.neo4j.graphdb.index.BatchInserterIndexProvider;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;
import org.neo4j.kernel.impl.util.FileUtils;

public class BatchInsertionIT
{
    private static final String PATH = "target/var/batch";

    @Before
    public void cleanDirectory() throws Exception
    {
        FileUtils.deleteRecursively( new File( PATH ) );
    }
    
    @Ignore
    @Test
    public void testInsertionSpeed()
    {
        BatchInserter inserter = new BatchInserterImpl( new File( PATH, "3" ).getAbsolutePath() );
        BatchInserterIndexProvider provider = new LuceneBatchInserterIndexProvider( inserter );
        BatchInserterIndex index = provider.nodeIndex( "yeah", EXACT_CONFIG );
        index.setCacheCapacity( "key", 1000000 );
        long t = currentTimeMillis();
        for ( int i = 0; i < 1000000; i++ )
        {
            Map<String, Object> properties = map( "key", "value" + i );
            long id = inserter.createNode( properties );
            index.add( id, properties );
        }
        System.out.println( "insert:" + ( currentTimeMillis() - t ) );
        index.flush();

        t = currentTimeMillis();
        for ( int i = 0; i < 1000000; i++ )
        {
            count( (Iterator<Long>) index.get( "key", "value" + i ) );
        }
        System.out.println( "get:" + ( currentTimeMillis() - t ) );
    }
}
