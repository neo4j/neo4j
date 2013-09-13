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
package org.neo4j.kernel.impl.nioneo.xa;

import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.ThreadToStatementContextBridge;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.scan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.StoreAccess;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.emptySetOf;

public class NeoStoreIndexStoreViewTest
{
    @Rule
    public TargetDirectory.TestDirectory testDirectory = TargetDirectory.cleanTestDirForTest( getClass() );

    Label label = DynamicLabel.label( "Person" );

    GraphDatabaseAPI graphDb;
    NeoStoreIndexStoreView storeView;

    int labelId;
    int propertyKeyId;

    Node alistair;
    Node stefan;

    @Test
    public void shouldScanExistingNodesForALabel() throws Exception
    {
        // given
        NodeUpdateCollectingVisitor visitor = new NodeUpdateCollectingVisitor();
        @SuppressWarnings( "unchecked" )
        Visitor<NodeLabelUpdate,Exception> labelVisitor = mock( Visitor.class );
        StoreScan<Exception> storeScan =
            storeView.visitNodes( new int[] { labelId }, new int[] { propertyKeyId }, visitor, labelVisitor );

        // when
        storeScan.run();

        // then
        assertEquals(
            asSet(
                NodePropertyUpdate.add( alistair.getId(), propertyKeyId, "Alistair", new long[] { labelId } ),
                NodePropertyUpdate.add( stefan.getId(), propertyKeyId, "Stefan", new long[] { labelId } )
            ), visitor.getUpdates() );
    }

    @Test
    public void shouldIgnoreDeletedNodesDuringScan() throws Exception
    {
        // given
        deleteAlistairAndStefanNodes();

        NodeUpdateCollectingVisitor visitor = new NodeUpdateCollectingVisitor();
        @SuppressWarnings( "unchecked" )
        Visitor<NodeLabelUpdate,Exception> labelVisitor = mock( Visitor.class );
        StoreScan<Exception> storeScan =
                storeView.visitNodes( new int[] { labelId }, new int[] { propertyKeyId }, visitor, labelVisitor );

        // when
        storeScan.run();

        // then
        assertEquals( emptySetOf( NodePropertyUpdate.class ), visitor.getUpdates() );
    }

    @Before
    public void before() throws KernelException
    {
        String graphDbPath = testDirectory.directory().getAbsolutePath();
        graphDb = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase( graphDbPath );

        createAlistairAndStefanNodes();
        getOrCreateIds();

        NeoStore neoStore = new StoreAccess( graphDb ).getRawNeoStore();
        storeView = new NeoStoreIndexStoreView( neoStore );
    }

    @After
    public void after()
    {
        graphDb.shutdown();
    }


    private void createAlistairAndStefanNodes()
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            alistair = graphDb.createNode( label );
            alistair.setProperty( "name", "Alistair" );
            stefan = graphDb.createNode( label );
            stefan.setProperty( "name", "Stefan" );
            tx.success();
        }
    }

    private void deleteAlistairAndStefanNodes()
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            alistair.delete();
            stefan.delete();
            tx.success();
        }
    }

    private void getOrCreateIds() throws KernelException
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            ThreadToStatementContextBridge bridge =
                    graphDb.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );

            try ( Statement statement = bridge.statement() )
            {
                labelId = statement.dataWriteOperations().labelGetOrCreateForName( "Person" );
                propertyKeyId = statement.dataWriteOperations().propertyKeyGetOrCreateForName( "name" );
            }
            tx.success();
        }
    }

    class NodeUpdateCollectingVisitor implements Visitor<NodePropertyUpdate, Exception>
    {
        private final Set<NodePropertyUpdate> updates = new HashSet<>();

        @Override
        public boolean visit( NodePropertyUpdate element ) throws Exception
        {
            updates.add( element );
            return false;
        }

        Set<NodePropertyUpdate> getUpdates()
        {
            return updates;
        }
    }
}
