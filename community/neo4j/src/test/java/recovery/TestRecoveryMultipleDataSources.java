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
package recovery;

import static java.lang.Runtime.getRuntime;
import static java.lang.System.exit;
import static java.lang.System.getProperty;
import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.impl.util.FileUtils.deleteRecursively;
import static org.neo4j.test.TargetDirectory.forTest;

import java.io.File;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.tooling.GlobalGraphOperations;

public class TestRecoveryMultipleDataSources
{
    private static final String dir = forTest( TestRecoveryMultipleDataSources.class ).graphDbDir( false ).getAbsolutePath();
    
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
        deleteRecursively( new File( dir ) );
        assertEquals( 0, getRuntime().exec( new String[] { "java", "-cp", getProperty( "java.class.path" ),
                getClass().getName() } ).waitFor() );
        
        // When
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( dir );

        // Then
        Transaction transaction = db.beginTx();
        try
        {
            assertEquals( MyRelTypes.TEST.name(), GlobalGraphOperations.at( db ).getAllRelationshipTypes().iterator().next().name() );
        }
        finally
        {
            transaction.finish();
            db.shutdown();
        }
    }

    public static void main( String[] args )
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase( dir );
        Transaction tx = db.beginTx();
        db.createNode().createRelationshipTo( db.createNode(), MyRelTypes.TEST );
        tx.success();
        tx.finish();
        
        db.getDependencyResolver().resolveDependency( XaDataSourceManager.class ).rotateLogicalLogs();
        tx = db.beginTx();
        db.index().forNodes( "index" ).add( db.createNode(), dir, db.createNode() );
        tx.success();
        tx.finish();
        exit( 0 );
    }
}
