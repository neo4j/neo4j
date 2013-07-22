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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.File;
import java.io.IOException;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.test.BatchTransaction;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

import static org.junit.Assert.assertEquals;
import static org.neo4j.test.BatchTransaction.beginBatchTx;

public class TestStandaloneLogExtractor
{
    @Test
    public void testRecreateCleanDbFromStandaloneExtractor() throws Exception
    {
        run( true, 1 );
    }
    
    @Test
    public void testRecreateUncleanDbFromStandaloneExtractor() throws Exception
    {
        run( false, 2 );
    }
    
    private void run( boolean cleanShutdown, int nr ) throws Exception
    {
        EphemeralFileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();
        
        String storeDir = "source" + nr;
        GraphDatabaseAPI db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().
                setFileSystem( fileSystem ).
                newImpermanentDatabase( storeDir );

        createSomeTransactions( db );
        DbRepresentation rep = DbRepresentation.of( db );

        EphemeralFileSystemAbstraction snapshot;
        if ( cleanShutdown )
        {
            db.shutdown();
            snapshot = fileSystem.snapshot();
        } else
        {
            snapshot = fileSystem.snapshot();
            db.shutdown();
        }

        GraphDatabaseAPI newDb = (GraphDatabaseAPI) new TestGraphDatabaseFactory().
                setFileSystem( snapshot ).
                newImpermanentDatabase( storeDir );

        XaDataSource ds = newDb.getXaDataSourceManager().getNeoStoreDataSource();
        LogExtractor extractor = LogExtractor.from( snapshot, new File( storeDir ) );
        long expectedTxId = 2;
        while ( true )
        {
            InMemoryLogBuffer buffer = new InMemoryLogBuffer();
            long txId = extractor.extractNext( buffer );
            assertEquals( expectedTxId++, txId );

            /* first tx=2
             * 1 tx for relationship type
             * 1 tx for property index
             * 1 for the first tx
             * 5 additional tx + 1 tx for the other property index
             * ==> 11
             */
            if ( expectedTxId == 11 ) expectedTxId = -1;
            if ( txId == -1 ) break;
            ds.applyCommittedTransaction( txId, buffer );
        }
        DbRepresentation newRep = DbRepresentation.of( newDb );
        newDb.shutdown();

        assertEquals( rep, newRep );
        fileSystem.shutdown();
    }
    
    private void createSomeTransactions( GraphDatabaseAPI db ) throws IOException
    {
        BatchTransaction tx = beginBatchTx( db );
        Node node = db.createNode();
        node.setProperty( "name", "First" );
        Node otherNode = db.createNode();
        node.createRelationshipTo( otherNode, MyRelTypes.TEST );
        tx.restart();
        db.getXaDataSourceManager().getNeoStoreDataSource().rotateLogicalLog();
        
        for ( int i = 0; i < 5; i++ )
        {
            db.createNode().setProperty( "type", i );
            tx.restart();
        }
        tx.finish();
    }
}
