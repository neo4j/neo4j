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
package org.neo4j.kernel.impl.transaction.xaframework;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.Config.KEEP_LOGICAL_LOGS;
import static org.neo4j.test.BatchTransaction.beginBatchTx;
import static org.neo4j.test.TargetDirectory.forTest;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.test.BatchTransaction;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.TargetDirectory;

public class TestStandaloneLogExtractor
{
    @Test
    public void testRecreateDbFromStandaloneExtractor() throws Exception
    {
        String sourceDir = forTest( getClass() ).directory( "source", true ).getAbsolutePath();
        GraphDatabaseService db = new EmbeddedGraphDatabase( sourceDir, stringMap( KEEP_LOGICAL_LOGS, "true" ) );
        
        BatchTransaction tx = beginBatchTx( db );
        Node node = db.createNode();
        node.setProperty( "name", "First" );
        Node otherNode = db.createNode();
        node.createRelationshipTo( otherNode, MyRelTypes.TEST );
        tx.restart();
        
        for ( int i = 0; i < 5; i++ )
        {
            db.createNode().setProperty( "type", i );
            tx.restart();
        }
        tx.finish();
        DbRepresentation sourceRep = DbRepresentation.of( db );
        db.shutdown();
        
        AbstractGraphDatabase newDb = new EmbeddedGraphDatabase( TargetDirectory.forTest( getClass() ).directory( "target", true ).getAbsolutePath() );
        XaDataSource ds = newDb.getConfig().getTxModule().getXaDataSourceManager().getXaDataSource( Config.DEFAULT_DATA_SOURCE_NAME );
        LogExtractor extractor = LogExtractor.from( sourceDir );
        long expectedTxId = 2;
        while ( true )
        {
            InMemoryLogBuffer buffer = new InMemoryLogBuffer();
            long txId = extractor.extractNext( buffer );
            assertEquals( expectedTxId++, txId );
            
            /* first tx=2
             * 1 tx for relationship type + 1 for the first tx
             * 5 additional tx
             * ==> 9
             */
            if ( expectedTxId == 9 ) expectedTxId = -1;
            if ( txId == -1 ) break;
            ds.applyCommittedTransaction( txId, buffer );
        }
        assertEquals( sourceRep, DbRepresentation.of( newDb ) );
        newDb.shutdown();
    }
}
