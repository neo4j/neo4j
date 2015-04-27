/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.backup;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Settings;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;

public class IncrementalBackupTests
{
    private File serverPath;
    private File backupPath;

    @Rule
    public TestName testName = new TestName();
    private ServerInterface server;
    private GraphDatabaseService db;

    @Before
    public void before() throws Exception
    {
        File base = TargetDirectory.forTest( getClass() ).cleanDirectory( testName.getMethodName() );
        serverPath = new File( base, "server" );
        backupPath = new File( base, "backup" );
    }

    @After
    public void shutItDown() throws Exception
    {
        if ( server != null )
        {
            shutdownServer( server );
            server = null;
        }
        if ( db != null )
        {
            db.shutdown();
            db = null;
        }
    }

    @Test
    public void shouldDoIncrementalBackup() throws Exception
    {
        DbRepresentation initialDataSetRepresentation = createInitialDataSet2( serverPath );
        server = startServer( serverPath, "127.0.0.1:6362" );

        // START SNIPPET: onlineBackup
        OnlineBackup backup = OnlineBackup.from( "127.0.0.1" );

        backup.full( backupPath.getPath() );

        // END SNIPPET: onlineBackup
        assertEquals( initialDataSetRepresentation, DbRepresentation.of( backupPath ) );
        shutdownServer( server );

        DbRepresentation furtherRepresentation = addMoreData2( serverPath );
        server = startServer( serverPath, null );
        // START SNIPPET: onlineBackup
        backup.incremental( backupPath.getPath() );
        // END SNIPPET: onlineBackup
        assertEquals( furtherRepresentation, DbRepresentation.of( backupPath ) );
        shutdownServer( server );
    }


    private DbRepresentation createInitialDataSet2( File path )
    {
        db = startGraphDatabase( path );
        Transaction tx = db.beginTx();
        db.createNode().setProperty( "name", "Goofy" );
        Node donald = db.createNode();
        donald.setProperty( "name", "Donald" );
        Node daisy = db.createNode();
        daisy.setProperty( "name", "Daisy" );
        Relationship knows = donald.createRelationshipTo( daisy,
                DynamicRelationshipType.withName( "LOVES" ) );
        knows.setProperty( "since", 1940 );
        tx.success();
        tx.finish();
        DbRepresentation result = DbRepresentation.of( db );
        db.shutdown();
        return result;
    }

    private DbRepresentation addMoreData2( File path )
    {
        db = startGraphDatabase( path );
        Transaction tx = db.beginTx();
        Node donald = db.getNodeById( 2 );
        Node gladstone = db.createNode();
        gladstone.setProperty( "name", "Gladstone" );
        Relationship hates = donald.createRelationshipTo( gladstone,
                DynamicRelationshipType.withName( "HATES" ) );
        hates.setProperty( "since", 1948 );
        tx.success();
        tx.finish();
        DbRepresentation result = DbRepresentation.of( db );
        db.shutdown();
        return result;
    }

    private GraphDatabaseService startGraphDatabase( File path )
    {
        return new TestGraphDatabaseFactory().
                newEmbeddedDatabaseBuilder( path.getPath() ).
                setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE ).
                setConfig( GraphDatabaseSettings.keep_logical_logs, Settings.TRUE ).
                newGraphDatabase();
    }

    private ServerInterface startServer( File path, String serverAddress ) throws Exception
    {
        ServerInterface server = new EmbeddedServer( path.getPath(), serverAddress );
        server.awaitStarted();
        return server;
    }

    private void shutdownServer( ServerInterface server ) throws Exception
    {
        server.shutdown();
        Thread.sleep( 1000 );
    }
}
