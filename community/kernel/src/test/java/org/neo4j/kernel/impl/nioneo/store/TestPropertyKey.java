/**
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
package org.neo4j.kernel.impl.nioneo.store;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

public class TestPropertyKey
{
    @Test
    public void lazyLoadWithinWriteTransaction() throws Exception
    {
        File dir = new File( "dir" );
        BatchInserter inserter = BatchInserters.inserter( dir.getPath(), fs.get() );
        int count = 3000;
        long nodeId = inserter.createNode( mapWithManyProperties( count /* larger than initial property index load threshold */ ) );
        inserter.shutdown();
        
        GraphDatabaseService db = new TestGraphDatabaseFactory().setFileSystem( fs.get() ).newImpermanentDatabase( dir.getPath() );
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

    @Rule public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
}
