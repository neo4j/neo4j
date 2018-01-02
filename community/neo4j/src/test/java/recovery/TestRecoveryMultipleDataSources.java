/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package recovery;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import static java.lang.Runtime.getRuntime;
import static java.lang.System.exit;
import static java.lang.System.getProperty;
import static org.junit.Assert.assertEquals;

public class TestRecoveryMultipleDataSources
{
    @Rule
    public final TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );

    /**
     * Tests an issue where loading all relationship types and property indexes after
     * the neostore data source had been started internally. The db would be in a
     * state where it would need recovery for the neostore data source, as well as some
     * other data source. This would fail since eventually TxManager#getTransaction()
     * would be called, which would fail since it hadn't as of yet recovered fully.
     * Whereas that failure would happen in a listener and merely be logged, one effect
     * of it would be that there would seem to be no relationship types in the database.
     */
    @Test
    public void recoverNeoAndIndexHavingAllRelationshipTypesAfterRecovery() throws Exception
    {
        // Given (create transactions and kill process, leaving it needing for recovery)
        File storeDir = testDirectory.graphDbDir();
        assertEquals( 0, getRuntime().exec( new String[]{"java", "-Djava.awt.headless=true", "-cp",
                getProperty( "java.class.path" ), getClass().getName(), storeDir.getAbsolutePath()} ).waitFor() );

        // When
        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir );

        // Then
        try ( Transaction ignored = db.beginTx() )
        {
            assertEquals( MyRelTypes.TEST.name(),
                    GlobalGraphOperations.at( db ).getAllRelationshipTypes().iterator().next().name() );
        }
        finally
        {
            db.shutdown();
        }
    }

    public static void main( String[] args ) throws IOException
    {
        if ( args.length != 1 )
        {
            exit( 1 );
        }

        File storeDir = new File( args[0] );
        GraphDatabaseAPI db = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        Transaction tx = db.beginTx();
        db.createNode().createRelationshipTo( db.createNode(), MyRelTypes.TEST );
        tx.success();
        tx.close();

        db.getDependencyResolver().resolveDependency( LogRotation.class ).rotateLogFile();

        tx = db.beginTx();
        db.index().forNodes( "index" ).add( db.createNode(), storeDir.getAbsolutePath(), db.createNode() );
        tx.success();
        tx.close();
        exit( 0 );
    }
}
