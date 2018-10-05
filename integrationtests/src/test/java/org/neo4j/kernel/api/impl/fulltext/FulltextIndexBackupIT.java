/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.api.impl.fulltext;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.backup.OnlineBackup;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.test.rule.CleanupRule;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.NODE;
import static org.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.NODE_CREATE;
import static org.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.QUERY_NODES;
import static org.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.QUERY_RELS;
import static org.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.RELATIONSHIP;
import static org.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.RELATIONSHIP_CREATE;
import static org.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.array;
import static org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings.online_backup_enabled;
import static org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings.online_backup_server;

public class FulltextIndexBackupIT
{
    private static final Label LABEL = Label.label( "LABEL" );
    private static final String PROP = "prop";
    private static final RelationshipType REL = RelationshipType.withName( "REL" );
    private static final String NODE_INDEX = "nodeIndex";
    private static final String REL_INDEX = "relIndex";

    private final SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    private final TestDirectory dir = TestDirectory.testDirectory();
    private final CleanupRule cleanup = new CleanupRule();
    private long nodeId1;
    private long nodeId2;
    private long relId1;

    @Rule
    public final RuleChain rules = RuleChain.outerRule( suppressOutput ).around( dir ).around( cleanup );

    private int backupPort;
    private GraphDatabaseAPI db;

    @Before
    public void setUpPorts()
    {
        backupPort = PortAuthority.allocatePort();
        GraphDatabaseFactory factory = new EnterpriseGraphDatabaseFactory();
        GraphDatabaseBuilder builder = factory.newEmbeddedDatabaseBuilder( dir.storeDir() );
        builder.setConfig( online_backup_enabled, "true" );
        builder.setConfig( online_backup_server, "127.0.0.1:" + backupPort );
        db = (GraphDatabaseAPI) builder.newGraphDatabase();
        cleanup.add( db );
    }

    @Test
    public void fulltextIndexesMustBeTransferredInBackup()
    {
        initializeTestData();
        verifyData( db );
        File backup = dir.storeDir( "backup" );
        OnlineBackup.from( "127.0.0.1", backupPort ).backup( backup );
        db.shutdown();

        GraphDatabaseAPI backupDb = startBackupDatabase( backup );
        verifyData( backupDb );
    }

    @Test
    public void fulltextIndexesMustBeUpdatedByIncrementalBackup()
    {
        initializeTestData();
        File backup = dir.databaseDir( "backup" );
        OnlineBackup.from( "127.0.0.1", backupPort ).backup( backup );

        long nodeId3;
        long nodeId4;
        long relId2;
        try ( Transaction tx = db.beginTx() )
        {
            Node node3 = db.createNode( LABEL );
            node3.setProperty( PROP, "Additional data." );
            Node node4 = db.createNode( LABEL );
            node4.setProperty( PROP, "Even more additional data." );
            Relationship rel = node3.createRelationshipTo( node4, REL );
            rel.setProperty( PROP, "Knows of" );
            nodeId3 = node3.getId();
            nodeId4 = node4.getId();
            relId2 = rel.getId();
            tx.success();
        }
        verifyData( db );

        OnlineBackup.from( "127.0.0.1", backupPort ).backup( backup );
        db.shutdown();

        GraphDatabaseAPI backupDb = startBackupDatabase( backup );
        verifyData( backupDb );

        try ( Transaction tx = backupDb.beginTx() )
        {
            try ( Result nodes = backupDb.execute( format( QUERY_NODES, NODE_INDEX, "additional" ) ) )
            {
                List<Long> nodeIds = nodes.stream().map( m -> ((Node) m.get( NODE )).getId() ).collect( Collectors.toList() );
                assertThat( nodeIds, containsInAnyOrder( nodeId3, nodeId4 ) );
            }
            try ( Result relationships = backupDb.execute( format( QUERY_RELS, REL_INDEX, "knows" ) ) )
            {
                List<Long> relIds = relationships.stream().map( m -> ((Relationship) m.get( RELATIONSHIP )).getId() ).collect( Collectors.toList() );
                assertThat( relIds, containsInAnyOrder( relId2 ) );
            }
            tx.success();
        }
    }

    // TODO test that creation and dropping of fulltext indexes is applied through incremental backup.
    // TODO test that custom analyzer configurations are applied through incremental backup.
    // TODO test that the eventually_consistent setting is transferred through incremental backup.

    private void initializeTestData()
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node1 = db.createNode( LABEL );
            node1.setProperty( PROP, "This is an integration test." );
            Node node2 = db.createNode( LABEL );
            node2.setProperty( PROP, "This is a related integration test." );
            Relationship relationship = node1.createRelationshipTo( node2, REL );
            relationship.setProperty( PROP, "They relate" );
            nodeId1 = node1.getId();
            nodeId2 = node2.getId();
            relId1 = relationship.getId();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, NODE_INDEX, array( LABEL.name() ), array( PROP ) ) ).close();
            db.execute( format( RELATIONSHIP_CREATE, REL_INDEX, array( REL.name() ), array( PROP ) ) ).close();
            tx.success();
        }
        awaitPopulation( db );
    }

    private static void awaitPopulation( GraphDatabaseAPI db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
            tx.success();
        }
    }

    private GraphDatabaseAPI startBackupDatabase( File backupDatabaseDir )
    {
        return (GraphDatabaseAPI) cleanup.add( new EnterpriseGraphDatabaseFactory().newEmbeddedDatabaseBuilder( backupDatabaseDir ).newGraphDatabase() );
    }

    private void verifyData( GraphDatabaseAPI db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            awaitPopulation( db );
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            try ( Result nodes = db.execute( format( QUERY_NODES, NODE_INDEX, "integration" ) ) )
            {
                List<Long> nodeIds = nodes.stream().map( m -> ((Node) m.get( NODE )).getId() ).collect( Collectors.toList() );
                assertThat( nodeIds, containsInAnyOrder( nodeId1, nodeId2 ) );
            }
            try ( Result relationships = db.execute( format( QUERY_RELS, REL_INDEX, "relate" ) ) )
            {
                List<Long> relIds = relationships.stream().map( m -> ((Relationship) m.get( RELATIONSHIP )).getId() ).collect( Collectors.toList() );
                assertThat( relIds, containsInAnyOrder( relId1 ) );
            }
            tx.success();
        }
    }
}
