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
package org.neo4j.kernel.impl.transaction.xaframework;

import static org.junit.Assert.assertEquals;
import static org.neo4j.test.TargetDirectory.forTest;

import java.io.File;

import org.junit.Test;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.ProcessStreamHandler;
import org.neo4j.test.TargetDirectory;

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
        File sourceDir = forTest( getClass() ).directory( "source" + nr, true );
        Process process = Runtime.getRuntime().exec( new String[]{
            "java", "-cp", System.getProperty( "java.class.path" ), CreateSomeTransactions.class.getName(),
            sourceDir.getPath(), "" + cleanShutdown
        } );

        new ProcessStreamHandler( process, true ).waitForResult();

        GraphDatabaseAPI newDb = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase(
                TargetDirectory.forTest( getClass() ).directory( "target" + nr, true ).getAbsolutePath() );
        XaDataSource ds = newDb.getXaDataSourceManager().getNeoStoreDataSource();
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
        DbRepresentation newRep = DbRepresentation.of( newDb );
        newDb.shutdown();

        assertEquals( DbRepresentation.of( sourceDir ), newRep );
    }
}
