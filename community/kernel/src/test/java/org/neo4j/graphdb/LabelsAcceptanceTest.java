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
package org.neo4j.graphdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.IteratorUtil.asEnumNameSet;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.count;

import java.io.File;
import java.util.Iterator;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.IdGenerator;
import org.neo4j.kernel.impl.nioneo.store.UnderlyingStorageException;
import org.neo4j.test.ImpermanentDatabaseRule;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.impl.EphemeralIdGenerator;
import org.neo4j.tooling.GlobalGraphOperations;

public class LabelsAcceptanceTest
{
    public @Rule ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();

    private enum Labels implements Label
    {
        MY_LABEL,
        MY_OTHER_LABEL
    }

    @Test
    public void addingALabelUsingAValidIdentifierShouldSucceed() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        Node myNode = null;

        // When
        Transaction tx = beansAPI.beginTx();
        try
        {
            myNode = beansAPI.createNode();
            myNode.addLabel( Labels.MY_LABEL );

            tx.success();
        }
        finally
        {
            tx.finish();
        }

        // Then
        assertTrue( "Label should have been added to node", myNode.hasLabel( Labels.MY_LABEL ) );
    }

    @Test
    public void addingALabelUsingAnInvalidIdentifierShouldFail() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();

        // When I set an empty label
        Transaction tx = beansAPI.beginTx();
        try
        {
            beansAPI.createNode().addLabel( label( "" ));
            fail( "Should have thrown exception" );
        }
        catch( ConstraintViolationException ex )
        {   // Happy
        }
        finally
        {
            tx.finish();
        }

        // And When I set a null label
        Transaction tx2 = beansAPI.beginTx();
        try
        {
            beansAPI.createNode().addLabel( label( null ));
            fail( "Should have thrown exception" );
        }
        catch( ConstraintViolationException ex )
        {   // Happy
        }
        finally
        {
            tx2.finish();
        }
    }

    @Test
    public void addingALabelThatAlreadyExistsBehavesAsNoOp() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        Node myNode = null;

        // When
        Transaction tx = beansAPI.beginTx();
        try
        {
            myNode = beansAPI.createNode();
            myNode.addLabel(Labels.MY_LABEL);
            myNode.addLabel(Labels.MY_LABEL);

            tx.success();
        } finally
        {
            tx.finish();
        }

        // Then
        assertTrue( "Label should have been added to node", myNode.hasLabel( Labels.MY_LABEL ) );
        // TODO: When support for reading labels has been introduced, assert that MY_LABEL occurs only once
    }

    @Test
    public void oversteppingMaxNumberOfLabelsShouldFailGracefully() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = beansAPIWithNoMoreLabelIds();

        // When
        Transaction tx = beansAPI.beginTx();
        try
        {
            beansAPI.createNode().addLabel( Labels.MY_LABEL );
            fail( "Should have thrown exception" );
        }
        catch ( ConstraintViolationException ex )
        {   // Happy
        }
        finally
        {
            tx.finish();
        }
    }

    @Test
    public void removingCommittedLabel() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        Label label = Labels.MY_LABEL;
        Node myNode = createNode( beansAPI, label );

        // When
        Transaction tx = beansAPI.beginTx();
        try
        {
            myNode = beansAPI.createNode();
            myNode.removeLabel( label );
            tx.success();
        }
        finally
        {
            tx.finish();
        }

        // Then
        assertFalse( "Label should have been removed from node", myNode.hasLabel( label ) );
    }

    @Test
    public void createNodeWithLabels() throws Exception
    {
        // GIVEN
        GraphDatabaseService db = dbRule.getGraphDatabaseService();

        // WHEN
        Node node = null;
        Transaction tx = db.beginTx();
        try
        {
            node = db.createNode( Labels.values() );
            tx.success();
        }
        finally
        {
            tx.finish();
        }

        // THEN
        assertEquals( asEnumNameSet( Labels.class ), asLabelNameSet( node.getLabels() ));
    }

    @Test
    public void removingNonExistentLabel() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        Label label = Labels.MY_LABEL;

        // When
        Transaction tx = beansAPI.beginTx();
        Node myNode;
        try
        {
            myNode = beansAPI.createNode();
            myNode.removeLabel( label );
            tx.success();
        }
        finally
        {
            tx.finish();
        }

        // THEN
        assertFalse( myNode.hasLabel( label ) );
    }

    @Test
    public void removingExistingLabelFromUnlabeledNode() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        Label label = Labels.MY_LABEL;
        createNode( beansAPI, label );
        Node myNode = createNode( beansAPI );

        // When
        Transaction tx = beansAPI.beginTx();
        try
        {
            myNode.removeLabel( label );
            tx.success();
        }
        finally
        {
            tx.finish();
        }

        // THEN
        assertFalse( myNode.hasLabel( label ) );
    }

    @Test
    public void removingUncommittedLabel() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        Label label = Labels.MY_LABEL;

        // When
        Transaction tx = beansAPI.beginTx();
        Node myNode;
        try
        {
            myNode = beansAPI.createNode();
            myNode.addLabel( label );
            myNode.removeLabel( label );

            // THEN
            assertFalse( myNode.hasLabel( label ) );

            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    @Test
    public void shouldBeAbleToListLabelsForANode() throws Exception
    {
        // GIVEN
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        Transaction tx = beansAPI.beginTx();
        Node node = null;
        Set<String> expected = asSet( Labels.MY_LABEL.name(), Labels.MY_OTHER_LABEL.name() );
        try
        {
            node = beansAPI.createNode();
            for ( String label : expected )
                node.addLabel( label( label ) );
            tx.success();
        }
        finally
        {
            tx.finish();
        }

        // WHEN
        Set<String> labels = asSet( asStrings( node.getLabels() ) );

        // THEN
        assertEquals( "Node didn't have all labels", expected, labels );
    }
    
    @Test
    public void shouldReturnEmptyListIfNoLabels() throws Exception
    {
        // GIVEN
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        Node node = createNode( beansAPI );

        // WHEN
        Iterable<Label> labels = node.getLabels();

        // THEN
        assertEquals( 0, count( labels ) );
    }

    @Test
    public void getNodesWithLabelCommitted() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        GlobalGraphOperations glops = GlobalGraphOperations.at( beansAPI );

        // When
        Transaction tx = beansAPI.beginTx();
        Node node = beansAPI.createNode( );
        node.addLabel( Labels.MY_LABEL );
        tx.success();
        tx.finish();

        // THEN
        Iterator<Node> labelIter = glops.getAllNodesWithLabel( Labels.MY_LABEL ).iterator();
        assertEquals( labelIter.next(), node );
        assertFalse( labelIter.hasNext() );
        assertFalse( glops.getAllNodesWithLabel( Labels.MY_OTHER_LABEL ).iterator().hasNext() );
    }
    
    @Test
    public void getNodesWithLabelsWithTxAddsAndRemoves() throws Exception
    {
        // GIVEN
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        GlobalGraphOperations glops = GlobalGraphOperations.at( beansAPI );
        Node node1 = createNode( beansAPI, Labels.MY_LABEL, Labels.MY_OTHER_LABEL );
        Node node2 = createNode( beansAPI, Labels.MY_LABEL, Labels.MY_OTHER_LABEL );

        // WHEN
        Transaction tx = beansAPI.beginTx();
        Node node3 = null;
        Set<Node> nodesWithMyLabel = null, nodesWithMyOtherLabel = null;
        try
        {
            node3 = beansAPI.createNode( Labels.MY_LABEL );
            node2.removeLabel( Labels.MY_LABEL );
            // extracted here to be asserted below
            nodesWithMyLabel = asSet( glops.getAllNodesWithLabel( Labels.MY_LABEL ) );
            nodesWithMyOtherLabel = asSet( glops.getAllNodesWithLabel( Labels.MY_OTHER_LABEL ) );
            tx.success();
        }
        finally
        {
            tx.finish();
        }

        // THEN
        assertEquals( asSet( node1, node3 ), nodesWithMyLabel );
        assertEquals( asSet( node1, node2 ), nodesWithMyOtherLabel );
    }

    private Iterable<String> asStrings( Iterable<Label> labels )
    {
        return new IterableWrapper<String, Label>( labels )
        {
            @Override
            protected String underlyingObjectToObject( Label label )
            {
                return label.name();
            }
        };
    }

    private GraphDatabaseService beansAPIWithNoMoreLabelIds()
    {
        return new ImpermanentGraphDatabase()
        {
            @Override
            protected IdGeneratorFactory createIdGeneratorFactory()
            {
                return new EphemeralIdGenerator.Factory()
                {
                    @Override
                    public IdGenerator open( FileSystemAbstraction fs, File fileName, int grabSize, IdType idType,
                                             long highId )
                    {
                        switch(idType)
                        {
                            case PROPERTY_INDEX:
                                return new EphemeralIdGenerator( idType )
                                {
                                    @Override public long nextId()
                                    {
                                        // Same exception as the one thrown by IdGeneratorImpl
                                        throw new UnderlyingStorageException( "Id capacity exceeded" );
                                    }
                                };
                            default:
                                return super.open( fs, fileName, grabSize, idType, Long.MAX_VALUE );
                        }
                    }
                };
            }
        };
    }

    public static Set<String> asLabelNameSet( Iterable<Label> enums )
    {
        return asSet( map( new Function<Label, String>()
        {
            @Override
            public String apply( Label from )
            {
                return from.name();
            }
        }, enums ) );
    }

    private Node createNode( GraphDatabaseService db, Label... labels )
    {
        Transaction tx = db.beginTx();
        try
        {
            Node node = db.createNode( labels );
            tx.success();
            return node;
        }
        finally
        {
            tx.finish();
        }
    }
}
