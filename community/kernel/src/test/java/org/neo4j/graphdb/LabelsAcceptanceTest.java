/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.helpers.Function;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.test.ImpermanentDatabaseRule;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.TestGraphDatabaseFactoryState;
import org.neo4j.test.impl.EphemeralIdGenerator;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.Neo4jMatchers.hasLabel;
import static org.neo4j.graphdb.Neo4jMatchers.hasLabels;
import static org.neo4j.graphdb.Neo4jMatchers.hasNoLabels;
import static org.neo4j.graphdb.Neo4jMatchers.hasNoNodes;
import static org.neo4j.graphdb.Neo4jMatchers.hasNodes;
import static org.neo4j.graphdb.Neo4jMatchers.inTx;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.Iterables.toList;
import static org.neo4j.helpers.collection.IteratorUtil.asEnumNameSet;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.count;

public class LabelsAcceptanceTest
{
    public @Rule ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();

    private enum Labels implements Label
    {
        MY_LABEL,
        MY_OTHER_LABEL
    }

    /** https://github.com/neo4j/neo4j/issues/1279 */
    @Test
    public void shouldInsertLabelsWithoutDuplicatingThem() throws Exception
    {
        final Node node = dbRule.executeAndCommit( new Function<GraphDatabaseService, Node>()
        {
            @Override
            public Node apply( GraphDatabaseService db )
            {
                return db.createNode();
            }
        } );
        // POST "FOOBAR"
        dbRule.executeAndCommit( new Function<GraphDatabaseService, Void>()
        {
            @Override
            public Void apply( GraphDatabaseService db )
            {
                node.addLabel( label( "FOOBAR" ) );
                return null;
            }
        } );
        // POST ["BAZQUX"]
        dbRule.executeAndCommit( new Function<GraphDatabaseService, Void>()
        {
            @Override
            public Void apply( GraphDatabaseService db )
            {
                node.addLabel( label( "BAZQUX" ) );
                return null;
            }
        } );
        // PUT ["BAZQUX"]
        dbRule.executeAndCommit( new Function<GraphDatabaseService, Void>()
        {
            @Override
            public Void apply( GraphDatabaseService db )
            {
                for ( Label label : node.getLabels() )
                {
                    node.removeLabel( label );
                }
                node.addLabel( label( "BAZQUX" ) );
                return null;
            }
        } );
        // GET
        List<Label> labels = dbRule.executeAndCommit( new Function<GraphDatabaseService, List<Label>>()
        {
            @Override
            public List<Label> apply( GraphDatabaseService db )
            {
                List<Label> labels = new ArrayList<>();
                for ( Label label : node.getLabels() )
                {
                    labels.add( label );
                }
                return labels;
            }
        } );
        assertEquals( labels.toString(), 1, labels.size() );
        assertEquals( "BAZQUX", labels.get( 0 ).name() );
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
        assertThat( "Label should have been added to node", myNode, inTx( beansAPI, hasLabel( Labels.MY_LABEL ) ) );
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
            beansAPI.createNode().addLabel( label( "" ) );
            fail( "Should have thrown exception" );
        }
        catch ( ConstraintViolationException ex )
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
            beansAPI.createNode().addLabel( label( null ) );
            fail( "Should have thrown exception" );
        }
        catch ( ConstraintViolationException ex )
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
            myNode.addLabel( Labels.MY_LABEL );
            myNode.addLabel( Labels.MY_LABEL );

            tx.success();
        }
        finally
        {
            tx.finish();
        }

        // Then
        assertThat( "Label should have been added to node", myNode, inTx( beansAPI, hasLabel( Labels.MY_LABEL ) ) );
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

        beansAPI.shutdown();
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
            myNode.removeLabel( label );
            tx.success();
        }
        finally
        {
            tx.finish();
        }

        // Then
        assertThat( myNode, not( inTx( beansAPI, hasLabel( label ) ) ) );
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
        assertThat( node, inTx( db, hasLabels( asEnumNameSet( Labels.class ) ) ));
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
        assertThat( myNode, not( inTx( beansAPI, hasLabel( label ) ) ) );
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
        assertThat( myNode, not( inTx( beansAPI, hasLabel( label ) ) ) ) ;
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
            {
                node.addLabel( label( label ) );
            }
            tx.success();
        }
        finally
        {
            tx.finish();
        }

        assertThat(node, inTx( beansAPI, hasLabels( expected ) ));
    }

    @Test
    public void shouldReturnEmptyListIfNoLabels() throws Exception
    {
        // GIVEN
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        Node node = createNode( beansAPI );

        // WHEN THEN
        assertThat(node, inTx( beansAPI, hasNoLabels() ));
    }

    @Test
    public void getNodesWithLabelCommitted() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();

        // When
        Transaction tx = beansAPI.beginTx();
        Node node = beansAPI.createNode();
        node.addLabel( Labels.MY_LABEL );
        tx.success();
        tx.finish();

        // THEN
        assertThat( beansAPI, inTx( beansAPI, hasNodes( Labels.MY_LABEL, node ) ) );
        assertThat( beansAPI, inTx( beansAPI, hasNoNodes( Labels.MY_OTHER_LABEL ) ) );
    }

    @Test
    public void getNodesWithLabelsWithTxAddsAndRemoves() throws Exception
    {
        // GIVEN
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
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
            nodesWithMyLabel = asSet( beansAPI.findNodes( Labels.MY_LABEL ) );
            nodesWithMyOtherLabel = asSet( beansAPI.findNodes( Labels.MY_OTHER_LABEL ) );
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

    @Test
    public void shouldListLabels() throws Exception
    {
        // Given
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
        GlobalGraphOperations globalOps = GlobalGraphOperations.at( db );
        createNode( db, Labels.MY_LABEL, Labels.MY_OTHER_LABEL );
        List<Label> labels = null;

        // When
        Transaction tx = db.beginTx();
        try
        {
            labels = toList( globalOps.getAllLabels() );
        }
        finally
        {
            tx.finish();
        }

        // Then
        assertEquals( 2, labels.size() );
        assertThat( map( new Function<Label, String>()
        {
            @Override
            public String apply( Label label )
            {
                return label.name();
            }
        }, labels ), hasItems( Labels.MY_LABEL.name(), Labels.MY_OTHER_LABEL.name() ) );
    }

    @Test
    public void deleteAllNodesAndTheirLabels() throws Exception
    {
        // GIVEN
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
        final Label label = DynamicLabel.label( "A" );
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            node.addLabel( label );
            node.setProperty( "name", "bla" );
            tx.success();
        }

        // WHEN
        try ( Transaction tx = db.beginTx() )
        {
            for ( final Node node : GlobalGraphOperations.at( db ).getAllNodes() )
            {
                node.removeLabel( label ); // remove Label ...
                node.delete(); // ... and afterwards the node
            }
            tx.success();
        } // tx.close(); - here comes the exception

        // THEN
        Transaction transaction = db.beginTx();
        assertEquals( 0, count( GlobalGraphOperations.at( db ).getAllNodes() ) );
        transaction.finish();
    }

    @Test
    public void removingLabelDoesNotBreakPreviouslyCreatedLabelsIterator()
    {
        // GIVEN
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
        Label label1 = DynamicLabel.label( "A" );
        Label label2 = DynamicLabel.label( "B" );

        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( label1, label2 );

            for ( Label next : node.getLabels() )
            {
                node.removeLabel( next );
            }
            tx.success();
        }
    }

    @Test
    public void removingPropertyDoesNotBreakPreviouslyCreatedNodePropertyKeysIterator()
    {
        // GIVEN
        GraphDatabaseService db = dbRule.getGraphDatabaseService();

        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            node.setProperty( "name", "Horst" );
            node.setProperty( "age", "72" );

            Iterator<String> iterator = node.getPropertyKeys().iterator();

            while ( iterator.hasNext() )
            {
                node.removeProperty( iterator.next() );
            }
            tx.success();
        }
    }


    @Test
    public void shouldCreateNodeWithLotsOfLabelsAndThenRemoveMostOfThem() throws Exception
    {
        // given
        final int TOTAL_NUMBER_OF_LABELS = 200, NUMBER_OF_PRESERVED_LABELS = 20;
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
        Node node;
        {
            Transaction tx = db.beginTx();
            try
            {
                node = db.createNode();
                for ( int i = 0; i < TOTAL_NUMBER_OF_LABELS; i++ )
                {
                    node.addLabel( DynamicLabel.label( "label:" + i ) );
                }

                tx.success();
            }
            finally
            {
                tx.finish();
            }
        }

        // when
        {
            Transaction tx = db.beginTx();
            try
            {
                for ( int i = NUMBER_OF_PRESERVED_LABELS; i < TOTAL_NUMBER_OF_LABELS; i++ )
                {
                    node.removeLabel( DynamicLabel.label( "label:" + i ) );
                }

                tx.success();
            }
            finally
            {
                tx.finish();
            }
        }
        dbRule.clearCache();

        // then
        Transaction transaction = db.beginTx();
        try
        {
            List<String> labels = new ArrayList<>();
            for ( Label label : node.getLabels() )
            {
                labels.add( label.name() );
            }
            assertEquals( "labels on node: " + labels, NUMBER_OF_PRESERVED_LABELS, labels.size() );
        }
        finally
        {
            transaction.finish();
        }
    }

    @SuppressWarnings("deprecation")
    private GraphDatabaseService beansAPIWithNoMoreLabelIds()
    {
        final EphemeralIdGenerator.Factory idFactory = new EphemeralIdGenerator.Factory()
        {
            @Override
            public IdGenerator open( FileSystemAbstraction fs, File fileName, int grabSize, IdType idType,
                                     long highId )
            {
                switch ( idType )
                {
                case LABEL_TOKEN:
                {
                    IdGenerator generator = generators.get( idType );
                    if ( generator == null )
                    {
                        generator = new EphemeralIdGenerator( idType )
                        {
                            @Override
                            public long nextId()
                            {
                                // Same exception as the one thrown by IdGeneratorImpl
                                throw new UnderlyingStorageException( "Id capacity exceeded" );
                            }
                        };
                        generators.put( idType, generator );
                    }
                    return generator;
                }

                default:
                    return super.open( fs, fileName, grabSize, idType, Long.MAX_VALUE );
                }
            }
        };

        TestGraphDatabaseFactory dbFactory = new TestGraphDatabaseFactory()
        {
            @Override
            protected GraphDatabaseBuilder.DatabaseCreator createImpermanentDatabaseCreator(
                    final String storeDir, final TestGraphDatabaseFactoryState state )
            {
                return new GraphDatabaseBuilder.DatabaseCreator()
                {
                    @Override
                    public GraphDatabaseService newDatabase( Map<String,String> config )
                    {
                        return new ImpermanentGraphDatabase( storeDir, config, state.databaseDependencies() )
                        {
                            @Override
                            protected IdGeneratorFactory createIdGeneratorFactory()
                            {
                                return idFactory;
                            }
                        };
                    }
                };
            }
        };

        return dbFactory.newImpermanentDatabase();
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
