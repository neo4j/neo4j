/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.impl.core;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.NotFoundException;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.Transaction;
import org.neo4j.impl.AbstractNeoTestCase;
import org.neo4j.impl.MyRelTypes;

public class TestNeoConstrains extends AbstractNeoTestCase
{
    private Level level1 = null;
    private Level level2 = null;

    private String key = "testproperty";

    public TestNeoConstrains( String testName )
    {
        super( testName );
    }

    public void setUp()
    {
        super.setUp();
        // turn off logging since the code may print nasty stacktrace
        Logger log = Logger
            .getLogger( "org.neo4j.impl.persistence.PersistenceLayerMonitor" );
        level1 = log.getLevel();
        log.setLevel( Level.OFF );
        log = Logger.getLogger( "org.neo4j.impl.core.NeoConstraintsListener" );
        level2 = log.getLevel();
        log.setLevel( Level.OFF );
    }

    public void tearDown()
    {
        Logger log = Logger
            .getLogger( "org.neo4j.impl.persistence.BusinessLayerMonitor" );
        log.setLevel( level1 );
        log = Logger.getLogger( "org.neo4j.impl.core.NeoConstraints" );
        log.setLevel( level2 );
        super.tearDown();
    }

    public void testDeleteNodeWithRel1()
    {
        Node node1 = getNeo().createNode();
        Node node2 = getNeo().createNode();
        node1.createRelationshipTo( node2, MyRelTypes.TEST );
        node1.delete();
        try
        {
            Transaction tx = getTransaction();
            tx.success();
            tx.finish();
            fail( "Should not validate" );
        }
        catch ( Exception e )
        {
            // good
        }
        setTransaction( getNeo().beginTx() );
    }

    public void testDeleteNodeWithRel2()
    {
        Node node1 = getNeo().createNode();
        Node node2 = getNeo().createNode();
        node1.createRelationshipTo( node2, MyRelTypes.TEST );
        node2.delete();
        node1.delete();
        try
        {
            Transaction tx = getTransaction();
            tx.success();
            tx.finish();
            fail( "Should not validate" );
        }
        catch ( Exception e )
        {
            // good
        }
        setTransaction( getNeo().beginTx() );
    }

    public void testDeleteNodeWithRel3()
    {
        // make sure we can delete in wrong order
        Node node0 = getNeo().createNode();
        Node node1 = getNeo().createNode();
        Node node2 = getNeo().createNode();
        Relationship rel0 = node0.createRelationshipTo( node1, MyRelTypes.TEST );
        Relationship rel1 = node0.createRelationshipTo( node2, MyRelTypes.TEST );
        node1.delete();
        rel0.delete();
        Transaction tx = getTransaction();
        tx.success();
        tx.finish();
        setTransaction( getNeo().beginTx() );
        node2.delete();
        rel1.delete();
        node0.delete();
    }

    public void testCreateRelOnDeletedNode()
    {
        Node node1 = getNeo().createNode();
        Node node2 = getNeo().createNode();
        Transaction tx = getTransaction();
        tx.success();
        tx.finish();
        tx = getNeo().beginTx();
        node1.delete();
        try
        {
            node1.createRelationshipTo( node2, MyRelTypes.TEST );
            fail( "Create of rel on deleted node should fail fast" );
        }
        catch ( Exception e )
        { // ok
        }
        try
        {
            tx.failure();
            tx.finish();
            // fail( "Transaction should be marked rollback" );
        }
        catch ( Exception e )
        { // good
        }
        setTransaction( getNeo().beginTx() );
        node2.delete();
        node1.delete();
    }

    public void testAddPropertyDeletedNode()
    {
        Node node = getNeo().createNode();
        node.delete();
        try
        {
            node.setProperty( key, new Integer( 1 ) );
            fail( "Add property on deleted node should not validate" );
        }
        catch ( Exception e )
        {
            // good
        }
    }

    public void testRemovePropertyDeletedNode()
    {
        Node node = getNeo().createNode();
        node.setProperty( key, new Integer( 1 ) );
        node.delete();
        try
        {
            node.removeProperty( key );
            Transaction tx = getTransaction();
            tx.success();
            tx.finish();
            fail( "Change property on deleted node should not validate" );
        }
        catch ( Exception e )
        {
            // ok
        }
    }

    public void testChangePropertyDeletedNode()
    {
        Node node = getNeo().createNode();
        node.setProperty( key, new Integer( 1 ) );
        node.delete();
        try
        {
            node.setProperty( key, new Integer( 2 ) );
            Transaction tx = getTransaction();
            tx.success();
            tx.finish();
            fail( "Change property on deleted node should not validate" );
        }
        catch ( Exception e )
        {
            // ok
        }
    }

    public void testAddPropertyDeletedRelationship()
    {
        Node node1 = getNeo().createNode();
        Node node2 = getNeo().createNode();
        Relationship rel = node1.createRelationshipTo( node2, MyRelTypes.TEST );
        rel.delete();
        try
        {
            rel.setProperty( key, new Integer( 1 ) );
            Transaction tx = getTransaction();
            tx.success();
            tx.finish();
            fail( "Add property on deleted rel should not validate" );
        }
        catch ( Exception e )
        { // good
        }
        node1.delete();
        node2.delete();
    }

    public void testRemovePropertyDeletedRelationship()
    {
        Node node1 = getNeo().createNode();
        Node node2 = getNeo().createNode();
        Relationship rel = node1.createRelationshipTo( node2, MyRelTypes.TEST );
        rel.setProperty( key, new Integer( 1 ) );
        rel.delete();
        try
        {
            rel.removeProperty( key );
            Transaction tx = getTransaction();
            tx.success();
            tx.finish();
            fail( "Remove property on deleted rel should not validate" );
        }
        catch ( Exception e )
        {
            // ok
        }
        node1.delete();
        node2.delete();
    }

    public void testChangePropertyDeletedRelationship()
    {
        Node node1 = getNeo().createNode();
        Node node2 = getNeo().createNode();
        Relationship rel = node1.createRelationshipTo( node2, MyRelTypes.TEST );
        rel.setProperty( key, new Integer( 1 ) );
        rel.delete();
        try
        {
            rel.setProperty( key, new Integer( 2 ) );
            Transaction tx = getTransaction();
            tx.success();
            tx.finish();
            fail( "Change property on deleted rel should not validate" );
        }
        catch ( Exception e )
        {
            // ok
        }
        node1.delete();
        node2.delete();
    }

    public void testMultipleDeleteNode()
    {
        Node node1 = getNeo().createNode();
        node1.delete();
        try
        {
            node1.delete();
            Transaction tx = getTransaction();
            tx.success();
            tx.finish();
            fail( "Should not validate" );
        }
        catch ( Exception e )
        {
            // ok
        }
    }

    public void testMultipleDeleteRelationship()
    {
        Node node1 = getNeo().createNode();
        Node node2 = getNeo().createNode();
        Relationship rel = node1.createRelationshipTo( node2, MyRelTypes.TEST );
        rel.delete();
        node1.delete();
        node2.delete();
        try
        {
            rel.delete();
            Transaction tx = getTransaction();
            tx.success();
            tx.finish();
            fail( "Should not validate" );
        }
        catch ( Exception e )
        {
            // ok
        }
    }

    public void testIllegalPropertyType()
    {
        Logger log = Logger.getLogger( NodeManager.class.getName() );
        Level level = log.getLevel();
        log.setLevel( Level.OFF );
        try
        {
            Node node1 = getNeo().createNode();
            try
            {
                node1.setProperty( key, new Object() );
                fail( "Shouldn't validate" );
            }
            catch ( Exception e )
            { // good
            }
            try
            {
                Transaction tx = getTransaction();
                tx.success();
                tx.finish();
                fail( "Shouldn't validate" );
            }
            catch ( Exception e )
            {
            } // good
            setTransaction( getNeo().beginTx() );
            try
            {
                getNeo().getNodeById( (int) node1.getId() );
                fail( "Node should not exist, previous tx didn't rollback" );
            }
            catch ( NotFoundException e )
            {
                // good
            }
            node1 = getNeo().createNode();
            Node node2 = getNeo().createNode();
            Relationship rel = node1.createRelationshipTo( node2,
                MyRelTypes.TEST );
            try
            {
                rel.setProperty( key, new Object() );
                fail( "Shouldn't validate" );
            }
            catch ( Exception e )
            { // good
            }
            try
            {
                Transaction tx = getTransaction();
                tx.success();
                tx.finish();
                fail( "Shouldn't validate" );
            }
            catch ( Exception e )
            {
            } // good
            setTransaction( getNeo().beginTx() );
            try
            {
                getNeo().getNodeById( (int) node1.getId() );
                fail( "Node should not exist, previous tx didn't rollback" );
            }
            catch ( NotFoundException e )
            {
                // good
            }
            try
            {
                getNeo().getNodeById( (int) node2.getId() );
                fail( "Node should not exist, previous tx didn't rollback" );
            }
            catch ( NotFoundException e )
            {
                // good
            }
        }
        finally
        {
            log.setLevel( level );
        }
    }
}