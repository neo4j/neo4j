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
package org.neo4j.kernel.impl.core;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.PropertyTracker;
import org.neo4j.test.ImpermanentGraphDatabase;

public class NodeManagerTest
{
    private GraphDatabaseAPI db;

    @Before
    public void init()
    {
        db = new ImpermanentGraphDatabase();
    }

    @Test
    public void shouldRemoveAllPropertiesWhenDeletingNode() throws Exception
    {
        // GIVEN
        Node node = createNodeWith( "wut", "foo" );

        NodeManager nodeManager = db.getDependencyResolver().resolveDependency( NodeManager.class );
        NodeLoggingTracker tracker = new NodeLoggingTracker();
        nodeManager.addNodePropertyTracker( tracker );

        // WHEN
        delete( node );

        //THEN
        assertThat( tracker.removed, is( "1:wut" ) );
    }

    @Test
    public void shouldNotRemovePropertyTwice() throws Exception
    {
        // GIVEN node with one property
        Node node = createNodeWith( "wut", "foo" );

        NodeManager nodeManager = db.getDependencyResolver().resolveDependency( NodeManager.class );
        NodeLoggingTracker tracker = new NodeLoggingTracker();
        nodeManager.addNodePropertyTracker( tracker );

        // WHEN in the same tx, remove prop and then delete node
        Transaction tx = db.beginTx();
        node.removeProperty( "wut" );
        node.delete();
        tx.success();
        tx.finish();

        //THEN prop is removed only once
        assertThat( tracker.removed, is( "1:wut" ) );
    }

    @Test
    public void shouldRemoveRelationshipProperties() throws Exception
    {
        // GIVEN relationship with one property
        Relationship relationship = createRelationshipWith( "wut", "foo" );

        NodeManager nodeManager = db.getDependencyResolver().resolveDependency( NodeManager.class );
        RelationshipLoggingTracker tracker = new RelationshipLoggingTracker();
        nodeManager.addRelationshipPropertyTracker( tracker );

        // WHEN
        delete( relationship );

        //THEN prop is removed from tracker
        assertThat( tracker.removed, is( "0:wut" ) );
    }

    private void delete( Relationship relationship )
    {
        Transaction tx = db.beginTx();
        relationship.delete();
        tx.success();
        tx.finish();
    }

    private Node createNodeWith( String key, Object value )
    {
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        node.setProperty( key, value );
        tx.success();
        tx.finish();
        return node;
    }


    private Relationship createRelationshipWith( String key, Object value )
    {
        Transaction tx = db.beginTx();
        Node a = db.createNode();
        Node b = db.createNode();
        Relationship relationship = a.createRelationshipTo( b, DynamicRelationshipType.withName( "FOO" ) );
        relationship.setProperty( key, value );
        tx.success();
        tx.finish();
        return relationship;
    }

    private void delete( Node node )
    {
        Transaction tx = db.beginTx();
        node.delete();
        tx.success();
        tx.finish();
    }

    private class NodeLoggingTracker implements PropertyTracker<Node>
    {
        @Override
        public void propertyAdded( Node primitive, String propertyName, Object propertyValue )
        {
        }

        public String removed = "";

        @Override
        public void propertyRemoved( Node primitive, String propertyName, Object propertyValue )
        {
            removed = removed + primitive.getId() + ":" + propertyName;
        }

        @Override
        public void propertyChanged( Node primitive, String propertyName, Object oldValue, Object newValue )
        {
        }
    }

    private class RelationshipLoggingTracker implements PropertyTracker<Relationship>
    {
        @Override
        public void propertyAdded( Relationship primitive, String propertyName, Object propertyValue )
        {
        }

        public String removed = "";

        @Override
        public void propertyRemoved( Relationship primitive, String propertyName, Object propertyValue )
        {
            removed = removed + primitive.getId() + ":" + propertyName;
        }

        @Override
        public void propertyChanged( Relationship primitive, String propertyName, Object oldValue, Object newValue )
        {
        }
    }
}
