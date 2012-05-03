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
package org.neo4j.kernel.impl.nioneo.store;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;
import org.neo4j.test.TargetDirectory;

public class TestPropertyIndex
{
    @Test
    public void lazyLoadWithinWriteTransaction() throws Exception
    {
        File dir = TargetDirectory.forTest( getClass() ).graphDbDir( true );
        BatchInserter inserter = new BatchInserterImpl( dir.getAbsolutePath() );
        int count = 3000;
        long nodeId = inserter.createNode( mapWithManyProperties( count /* larger than initial property index load threshold */ ) );
        inserter.shutdown();
        
        EmbeddedGraphDatabase db = new EmbeddedGraphDatabase( dir.getAbsolutePath() );
        Transaction tx = db.beginTx();
        try
        {
            db.createNode();
            Node node = db.getNodeById( nodeId );
            assertEquals( count, IteratorUtil.count( node.getPropertyKeys() ) );
            tx.success();
        }
        finally
        {
            tx.finish();
            db.shutdown();
        }
    }

    private Map<String, Object> mapWithManyProperties( int count )
    {
        Map<String, Object> properties = new HashMap<String, Object>();
        for ( int i = 0; i < count; i++ )
            properties.put( "key:" + i, "value" );
        return properties;
    }
}
