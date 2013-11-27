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
package org.neo4j.unsafe.batchinsert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.Neo4jMatchers.hasLabels;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

public class TestBatchDatabase
{
    private final String storeDir = "/tmp/dblala";

    private EphemeralFileSystemAbstraction fs;

    @Test
    public void shouldCreateLabeledNodes()
    {
        // given

        GraphDatabaseService gdb = BatchInserters.batchDatabase( storeDir, fs );
        Label luluLabel = label( "lulu" );

        // when
        long nodeId = gdb.createNode( luluLabel ).getId();

        // and
        gdb = turnIntoRealGraphDatabase( gdb );

        // then
        Transaction transaction = gdb.beginTx();
        assertThat( gdb.getNodeById( nodeId ), hasLabels( luluLabel ) );
        transaction.finish();

        gdb.shutdown();
    }

    @Test
    public void shouldCreateAndSeeLabeledNodes()
    {
        // given

        GraphDatabaseService gdb = BatchInserters.batchDatabase( storeDir, fs );
        Label luluLabel = label( "lulu" );

        // when
        long nodeId = gdb.createNode( luluLabel ).getId();

        // then
        assertThat( gdb.getNodeById( nodeId ), hasLabels( luluLabel ) );
    }

    @Test
    public void shouldCreateAndTestLabeledNodes()
    {
        // given

        GraphDatabaseService gdb = BatchInserters.batchDatabase( storeDir, fs );
        Label luluLabel = label( "lulu" );

        // when
        long nodeId = gdb.createNode( luluLabel ).getId();

        // then
        assertThat( gdb.getNodeById( nodeId ), hasLabels( luluLabel ) );
    }

    @Test
    public void shouldAddLabelToNode()
    {
        // given

        GraphDatabaseService gdb = BatchInserters.batchDatabase( storeDir, fs );
        Label luluLabel = label( "lulu" );
        Node node = gdb.createNode();

        // when
        node.addLabel( luluLabel );

        // then
        assertThat( node, hasLabels( luluLabel ) );
    }

    @Test
    public void shouldAddLabelTwiceToNode()
    {
        // given

        GraphDatabaseService gdb = BatchInserters.batchDatabase( storeDir, fs );
        Label luluLabel = label( "lulu" );
        Node node = gdb.createNode();

        // when
        node.addLabel( luluLabel );
        node.addLabel( luluLabel );

        // then
        assertThat( node, hasLabels( luluLabel ) );
    }

    @Test
    public void removingNonExistantLabelFromNodeShouldBeNoOp()
    {
        // given

        GraphDatabaseService gdb = BatchInserters.batchDatabase( storeDir, fs );
        Label luluLabel = label( "lulu" );
        Node node = gdb.createNode();

        // when
        node.removeLabel( luluLabel );

        // then
        assertTrue( asSet( gdb.getNodeById( node.getId() ).getLabels() ).isEmpty() );
    }

    @Test
    public void shouldRemoveLabelFromNode()
    {
        // given

        GraphDatabaseService gdb = BatchInserters.batchDatabase( storeDir, fs );
        Label luluLabel = label( "lulu" );
        Label lalaLabel = label( "lala" );
        Node node = gdb.createNode();
        node.addLabel( lalaLabel );
        node.addLabel( luluLabel );

        // when
        node.removeLabel( luluLabel );

        // then
        assertThat( node, hasLabels( lalaLabel ) );
    }

    private GraphDatabaseService turnIntoRealGraphDatabase( GraphDatabaseService gdb )
    {
        gdb.shutdown();

        TestGraphDatabaseFactory factory = new TestGraphDatabaseFactory();
        factory.setFileSystem( fs );
        return factory.newImpermanentDatabase( storeDir );
    }

    @Before
    public void before()
    {
        fs = new EphemeralFileSystemAbstraction();
    }

    @After
    public void after()
    {
        fs.shutdown();
    }
}
