/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.graphdb;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.kernel.PropertyTracker;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;

public class PropertiesTest
{
    public final @Rule DatabaseRule db = new ImpermanentDatabaseRule();

    @Test
    public void shouldNotWriteDataForChangingNodePropertyToSameValue() throws Exception
    {
        // given
        GraphDatabaseService graphdb = db.getGraphDatabaseService();
        NodeManager nm = db.getGraphDatabaseAPI().getDependencyResolver().resolveDependency( NodeManager.class );
        NeoStoreXaDataSource datasource = db.getGraphDatabaseAPI().getDependencyResolver()
                                            .resolveDependency( XaDataSourceManager.class )
                                            .getNeoStoreDataSource();
        // create the node
        Node entity;
        try ( Transaction tx = graphdb.beginTx() )
        {
            entity = graphdb.createNode();
            entity.setProperty( "foo", "bar" );
            tx.success();
        }
        // this should have been the last transaction
        long txId = datasource.getLastCommittedTxId();
        // register an auto-index
        @SuppressWarnings("unchecked")
        PropertyTracker<Node> propertyTracker = mock( PropertyTracker.class );
        nm.addNodePropertyTracker( propertyTracker );

        // when
        try ( Transaction tx = graphdb.beginTx() )
        {
            entity.setProperty( "foo", "bar" );
            tx.success();
        }

        // then
        assertEquals( "should not commit any transaction", txId, datasource.getLastCommittedTxId() );
        // we still need the change to be tracked, so that (legacy) auto indexes are updated
        verify( propertyTracker ).propertyChanged( entity, "foo", "bar", "bar" );
    }

    @Test
    public void shouldNotWriteDataForChangingRelationshipPropertyToSameValue() throws Exception
    {
        // given
        GraphDatabaseService graphdb = db.getGraphDatabaseService();
        NodeManager nm = db.getGraphDatabaseAPI().getDependencyResolver().resolveDependency( NodeManager.class );
        NeoStoreXaDataSource datasource = db.getGraphDatabaseAPI().getDependencyResolver()
                                            .resolveDependency( XaDataSourceManager.class )
                                            .getNeoStoreDataSource();
        // create the relationship
        Relationship entity;
        try ( Transaction tx = graphdb.beginTx() )
        {
            entity = graphdb.createNode().createRelationshipTo( graphdb.createNode(), withName( "KNOWS" ) );
            entity.setProperty( "foo", "bar" );
            tx.success();
        }
        // this should have been the last transaction
        long txId = datasource.getLastCommittedTxId();
        // register an auto-index
        @SuppressWarnings("unchecked")
        PropertyTracker<Relationship> propertyTracker = mock( PropertyTracker.class );
        nm.addRelationshipPropertyTracker( propertyTracker );

        // when
        try ( Transaction tx = graphdb.beginTx() )
        {
            entity.setProperty( "foo", "bar" );
            tx.success();
        }

        // then
        assertEquals( "should not commit any transaction", txId, datasource.getLastCommittedTxId() );
        // we still need the change to be tracked, so that (legacy) auto indexes are updated
        verify( propertyTracker ).propertyChanged( entity, "foo", "bar", "bar" );
    }
}
