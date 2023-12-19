/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.backup;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestName;

import java.io.File;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;

public class IncrementalBackupIT
{
    public TestName testName = new TestName();
    public TestDirectory testDirectory = TestDirectory.testDirectory();
    public SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    @Rule
    public RuleChain rules = RuleChain.outerRule( testName ).around( testDirectory ).around( suppressOutput );

    private File serverPath;
    private File backupPath;
    private ServerInterface server;
    private GraphDatabaseService db;

    @Before
    public void before() throws Exception
    {
        File base = testDirectory.cleanDirectory( testName.getMethodName() );
        serverPath = new File( base, "server" );
        backupPath = new File( base, "backup" );
        serverPath.mkdirs();
        backupPath.mkdirs();
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
        DbRepresentation initialDataSetRepresentation = createInitialDataSet( serverPath );
        int port = PortAuthority.allocatePort();
        server = startServer( serverPath, "127.0.0.1:" + port );

        OnlineBackup backup = OnlineBackup.from( "127.0.0.1", port );

        backup.full( backupPath.getPath() );

        assertEquals( initialDataSetRepresentation, getBackupDbRepresentation() );
        shutdownServer( server );

        DbRepresentation furtherRepresentation = addMoreData2( serverPath );
        server = startServer( serverPath, "127.0.0.1:" + port );
        backup.incremental( backupPath.getPath() );
        assertEquals( furtherRepresentation, getBackupDbRepresentation() );
        shutdownServer( server );
    }

    @Test
    public void shouldNotServeTransactionsWithInvalidHighIds() throws Exception
    {
        /*
         * This is in effect a high level test for an edge case that happens when a relationship group is
         * created and deleted in the same tx. This can end up causing an IllegalArgumentException because
         * the HighIdApplier used when applying incremental updates (batch transactions in general) will postpone
         * processing of added/altered record ids but deleted ids will be processed on application. This can result
         * in a deleted record causing an IllegalArgumentException even though it is not the highest id in the tx.
         *
         * The way we try to trigger this is:
         * 0. In one tx, create a node with 49 relationships, belonging to two types.
         * 1. In another tx, create another relationship on that node (making it dense) and then delete all
         *    relationships of one type. This results in the tx state having a relationship group record that was
         *    created in this tx and also set to not in use.
         * 2. Receipt of this tx will have the offending rel group command apply its id before the groups that are
         *    altered. This will try to update the high id with a value larger than what has been seen previously and
         *    fail the update.
         * The situation is resolved by a check added in TransactionRecordState which skips the creation of such
         * commands.
         * Note that this problem can also happen in HA slaves.
         */
        DbRepresentation initialDataSetRepresentation = createInitialDataSet( serverPath );
        int port = PortAuthority.allocatePort();
        server = startServer( serverPath, "127.0.0.1:" + port );

        OnlineBackup backup = OnlineBackup.from( "127.0.0.1", port);

        backup.full( backupPath.getPath() );

        assertEquals( initialDataSetRepresentation, getBackupDbRepresentation() );
        shutdownServer( server );

        DbRepresentation furtherRepresentation = createTransactionWithWeirdRelationshipGroupRecord( serverPath );
        server = startServer( serverPath, "127.0.0.1:" + port );
        backup.incremental( backupPath.getPath() );
        assertEquals( furtherRepresentation, getBackupDbRepresentation() );
        shutdownServer( server );
    }

    private DbRepresentation createInitialDataSet( File path )
    {
        db = startGraphDatabase( path );
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode().setProperty( "name", "Goofy" );
            Node donald = db.createNode();
            donald.setProperty( "name", "Donald" );
            Node daisy = db.createNode();
            daisy.setProperty( "name", "Daisy" );
            Relationship knows = donald.createRelationshipTo( daisy,
                    RelationshipType.withName( "LOVES" ) );
            knows.setProperty( "since", 1940 );
            tx.success();
        }
        DbRepresentation result = DbRepresentation.of( db );
        db.shutdown();
        return result;
    }

    private DbRepresentation addMoreData2( File path )
    {
        db = startGraphDatabase( path );
        try ( Transaction tx = db.beginTx() )
        {
            Node donald = db.getNodeById( 2 );
            Node gladstone = db.createNode();
            gladstone.setProperty( "name", "Gladstone" );
            Relationship hates = donald.createRelationshipTo( gladstone,
                    RelationshipType.withName( "HATES" ) );
            hates.setProperty( "since", 1948 );
            tx.success();
        }
        DbRepresentation result = DbRepresentation.of( db );
        db.shutdown();
        return result;
    }

    private DbRepresentation createTransactionWithWeirdRelationshipGroupRecord( File path )
    {
        db = startGraphDatabase( path );
        int i = 0;
        Node node;
        RelationshipType typeToDelete = RelationshipType.withName( "A" );
        RelationshipType theOtherType = RelationshipType.withName( "B" );
        int defaultDenseNodeThreshold =
                Integer.parseInt( GraphDatabaseSettings.dense_node_threshold.getDefaultValue() );

        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            for ( ; i < defaultDenseNodeThreshold - 1; i++ )
            {
                node.createRelationshipTo( db.createNode(), theOtherType );
            }
            node.createRelationshipTo( db.createNode(), typeToDelete );
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            node.createRelationshipTo( db.createNode(), theOtherType );
            for ( Relationship relationship : node.getRelationships( Direction.BOTH, typeToDelete ) )
            {
                relationship.delete();
            }
            tx.success();
        }
        DbRepresentation result = DbRepresentation.of( db );
        db.shutdown();
        return result;
    }

    private GraphDatabaseService startGraphDatabase( File path )
    {
        return new TestGraphDatabaseFactory().
                newEmbeddedDatabaseBuilder( path ).
                setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE ).
                setConfig( GraphDatabaseSettings.keep_logical_logs, Settings.TRUE ).
                newGraphDatabase();
    }

    private ServerInterface startServer( File path, String serverAddress )
    {
        ServerInterface server = new EmbeddedServer( path, serverAddress );
        server.awaitStarted();
        return server;
    }

    private void shutdownServer( ServerInterface server ) throws Exception
    {
        server.shutdown();
        Thread.sleep( 1000 );
    }

    private DbRepresentation getBackupDbRepresentation()
    {
        return DbRepresentation.of( backupPath, Config.defaults( OnlineBackupSettings.online_backup_enabled, Settings.FALSE ) );
    }
}
