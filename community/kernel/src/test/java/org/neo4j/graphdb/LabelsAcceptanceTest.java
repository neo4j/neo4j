/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.factory.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.id.configuration.CommunityIdTypeConfigurationProvider;
import org.neo4j.kernel.impl.store.id.configuration.IdTypeConfiguration;
import org.neo4j.kernel.impl.store.id.configuration.IdTypeConfigurationProvider;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.TestGraphDatabaseFactoryState;
import org.neo4j.test.impl.EphemeralIdGenerator;
import org.neo4j.test.rule.ImpermanentDatabaseRule;
import org.neo4j.test.rule.TestDirectory;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.collection.primitive.PrimitiveIntCollections.consume;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterables.asList;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.hasLabel;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.hasLabels;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.hasNoLabels;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.hasNoNodes;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.hasNodes;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.inTx;

public class LabelsAcceptanceTest
{
    @Rule
    public final ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    private enum Labels implements Label
    {
        MY_LABEL,
        MY_OTHER_LABEL
    }

    /** https://github.com/neo4j/neo4j/issues/1279 */
    @Test
    public void shouldInsertLabelsWithoutDuplicatingThem() throws Exception
    {
        final Node node = dbRule.executeAndCommit(
                (Function<GraphDatabaseService,Node>) GraphDatabaseService::createNode );
        // POST "FOOBAR"
        dbRule.executeAndCommit( db ->
        {
            node.addLabel( label( "FOOBAR" ) );
        } );
        // POST ["BAZQUX"]
        dbRule.executeAndCommit( db ->
        {
            node.addLabel( label( "BAZQUX" ) );
        } );
        // PUT ["BAZQUX"]
        dbRule.executeAndCommit( db ->
        {
            for ( Label label : node.getLabels() )
            {
                node.removeLabel( label );
            }
            node.addLabel( label( "BAZQUX" ) );
        } );
        // GET
        List<Label> labels = dbRule.executeAndCommit( db ->
        {
            List<Label> labels1 = new ArrayList<>();
            for ( Label label : node.getLabels() )
            {
                labels1.add( label );
            }
            return labels1;
        } );
        assertEquals( labels.toString(), 1, labels.size() );
        assertEquals( "BAZQUX", labels.get( 0 ).name() );
    }

    @Test
    public void addingALabelUsingAValidIdentifierShouldSucceed() throws Exception
    {
        // Given
        GraphDatabaseService graphDatabase = dbRule.getGraphDatabaseAPI();
        Node myNode = null;

        // When
        try ( Transaction tx = graphDatabase.beginTx() )
        {
            myNode = graphDatabase.createNode();
            myNode.addLabel( Labels.MY_LABEL );

            tx.success();
        }

        // Then
        assertThat( "Label should have been added to node", myNode,
                inTx( graphDatabase, hasLabel( Labels.MY_LABEL ) ) );
    }

    @Test
    public void addingALabelUsingAnInvalidIdentifierShouldFail() throws Exception
    {
        // Given
        GraphDatabaseService graphDatabase = dbRule.getGraphDatabaseAPI();

        // When I set an empty label
        try ( Transaction tx = graphDatabase.beginTx() )
        {
            graphDatabase.createNode().addLabel( label( "" ) );
            fail( "Should have thrown exception" );
        }
        catch ( ConstraintViolationException ex )
        {   // Happy
        }

        // And When I set a null label
        try ( Transaction tx2 = graphDatabase.beginTx() )
        {
            graphDatabase.createNode().addLabel( () -> null );
            fail( "Should have thrown exception" );
        }
        catch ( ConstraintViolationException ex )
        {   // Happy
        }
    }

    @Test
    public void addingALabelThatAlreadyExistsBehavesAsNoOp() throws Exception
    {
        // Given
        GraphDatabaseService graphDatabase = dbRule.getGraphDatabaseAPI();
        Node myNode = null;

        // When
        try ( Transaction tx = graphDatabase.beginTx() )
        {
            myNode = graphDatabase.createNode();
            myNode.addLabel( Labels.MY_LABEL );
            myNode.addLabel( Labels.MY_LABEL );

            tx.success();
        }

        // Then
        assertThat( "Label should have been added to node", myNode,
                inTx( graphDatabase, hasLabel( Labels.MY_LABEL ) ) );
    }

    @Test
    public void oversteppingMaxNumberOfLabelsShouldFailGracefully() throws Exception
    {
        // Given
        GraphDatabaseService graphDatabase = beansAPIWithNoMoreLabelIds();

        // When
        try ( Transaction tx = graphDatabase.beginTx() )
        {
            graphDatabase.createNode().addLabel( Labels.MY_LABEL );
            fail( "Should have thrown exception" );
        }
        catch ( ConstraintViolationException ex )
        {   // Happy
        }

        graphDatabase.shutdown();
    }

    @Test
    public void removingCommittedLabel() throws Exception
    {
        // Given
        GraphDatabaseService graphDatabase = dbRule.getGraphDatabaseAPI();
        Label label = Labels.MY_LABEL;
        Node myNode = createNode( graphDatabase, label );

        // When
        try ( Transaction tx = graphDatabase.beginTx() )
        {
            myNode.removeLabel( label );
            tx.success();
        }

        // Then
        assertThat( myNode, not( inTx( graphDatabase, hasLabel( label ) ) ) );
    }

    @Test
    public void createNodeWithLabels() throws Exception
    {
        // GIVEN
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();

        // WHEN
        Node node = null;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode( Labels.values() );
            tx.success();
        }

        // THEN

        Set<String> names = Stream.of( Labels.values() ).map( Labels::name ).collect( toSet() );
        assertThat( node, inTx( db, hasLabels( names ) ) );
    }

    @Test
    public void removingNonExistentLabel() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseAPI();
        Label label = Labels.MY_LABEL;

        // When
        Node myNode;
        try ( Transaction tx = beansAPI.beginTx() )
        {
            myNode = beansAPI.createNode();
            myNode.removeLabel( label );
            tx.success();
        }

        // THEN
        assertThat( myNode, not( inTx( beansAPI, hasLabel( label ) ) ) );
    }

    @Test
    public void removingExistingLabelFromUnlabeledNode() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseAPI();
        Label label = Labels.MY_LABEL;
        createNode( beansAPI, label );
        Node myNode = createNode( beansAPI );

        // When
        try ( Transaction tx = beansAPI.beginTx() )
        {
            myNode.removeLabel( label );
            tx.success();
        }

        // THEN
        assertThat( myNode, not( inTx( beansAPI, hasLabel( label ) ) ) );
    }

    @Test
    public void removingUncommittedLabel() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseAPI();
        Label label = Labels.MY_LABEL;

        // When
        Node myNode;
        try ( Transaction tx = beansAPI.beginTx() )
        {
            myNode = beansAPI.createNode();
            myNode.addLabel( label );
            myNode.removeLabel( label );

            // THEN
            assertFalse( myNode.hasLabel( label ) );

            tx.success();
        }
    }

    @Test
    public void shouldBeAbleToListLabelsForANode() throws Exception
    {
        // GIVEN
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseAPI();
        Node node = null;
        Set<String> expected = asSet( Labels.MY_LABEL.name(), Labels.MY_OTHER_LABEL.name() );
        try ( Transaction tx = beansAPI.beginTx() )
        {
            node = beansAPI.createNode();
            for ( String label : expected )
            {
                node.addLabel( label( label ) );
            }
            tx.success();
        }

        assertThat( node, inTx( beansAPI, hasLabels( expected ) ) );
    }

    @Test
    public void shouldReturnEmptyListIfNoLabels() throws Exception
    {
        // GIVEN
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseAPI();
        Node node = createNode( beansAPI );

        // WHEN THEN
        assertThat( node, inTx( beansAPI, hasNoLabels() ) );
    }

    @Test
    public void getNodesWithLabelCommitted() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseAPI();

        // When
        Node node = null;
        try ( Transaction tx = beansAPI.beginTx() )
        {
            node = beansAPI.createNode();
            node.addLabel( Labels.MY_LABEL );
            tx.success();
        }

        // THEN
        assertThat( beansAPI, inTx( beansAPI, hasNodes( Labels.MY_LABEL, node ) ) );
        assertThat( beansAPI, inTx( beansAPI, hasNoNodes( Labels.MY_OTHER_LABEL ) ) );
    }

    @Test
    public void getNodesWithLabelsWithTxAddsAndRemoves() throws Exception
    {
        // GIVEN
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseAPI();
        Node node1 = createNode( beansAPI, Labels.MY_LABEL, Labels.MY_OTHER_LABEL );
        Node node2 = createNode( beansAPI, Labels.MY_LABEL, Labels.MY_OTHER_LABEL );

        // WHEN
        Node node3;
        Set<Node> nodesWithMyLabel;
        Set<Node> nodesWithMyOtherLabel;
        try ( Transaction tx = beansAPI.beginTx() )
        {
            node3 = beansAPI.createNode( Labels.MY_LABEL );
            node2.removeLabel( Labels.MY_LABEL );
            // extracted here to be asserted below
            nodesWithMyLabel = asSet( beansAPI.findNodes( Labels.MY_LABEL ) );
            nodesWithMyOtherLabel = asSet( beansAPI.findNodes( Labels.MY_OTHER_LABEL ) );
            tx.success();
        }

        // THEN
        assertEquals( asSet( node1, node3 ), nodesWithMyLabel );
        assertEquals( asSet( node1, node2 ), nodesWithMyOtherLabel );
    }

    @Test
    public void shouldListAllExistingLabels() throws Exception
    {
        // Given
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        createNode( db, Labels.MY_LABEL, Labels.MY_OTHER_LABEL );
        List<Label> labels = null;

        // When
        try ( Transaction tx = db.beginTx() )
        {
            labels = asList( db.getAllLabels() );
        }

        // Then
        assertEquals( 2, labels.size() );
        assertThat( map( Label::name, labels ), hasItems( Labels.MY_LABEL.name(), Labels.MY_OTHER_LABEL.name() ) );
    }

    @Test
    public void shouldListAllLabelsInUse() throws Exception
    {
        // Given
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        createNode( db, Labels.MY_LABEL );
        Node node = createNode( db, Labels.MY_OTHER_LABEL );
        try ( Transaction tx = db.beginTx() )
        {
            node.delete();
            tx.success();
        }
        List<Label> labels = null;

        // When
        try ( Transaction tx = db.beginTx() )
        {
            labels = asList( db.getAllLabelsInUse() );
        }

        // Then
        assertEquals( 1, labels.size() );
        assertThat( map( Label::name, labels ), hasItems( Labels.MY_LABEL.name() ) );
    }

    @Test
    public void deleteAllNodesAndTheirLabels() throws Exception
    {
        // GIVEN
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        final Label label = label( "A" );
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
            for ( final Node node : db.getAllNodes() )
            {
                node.removeLabel( label ); // remove Label ...
                node.delete(); // ... and afterwards the node
            }
            tx.success();
        } // tx.close(); - here comes the exception

        // THEN
        try ( Transaction transaction = db.beginTx() )
        {
            assertEquals( 0, Iterables.count( db.getAllNodes() ) );
        }
    }

    @Test
    public void removingLabelDoesNotBreakPreviouslyCreatedLabelsIterator()
    {
        // GIVEN
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        Label label1 = label( "A" );
        Label label2 = label( "B" );

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
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();

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
        final int TOTAL_NUMBER_OF_LABELS = 200;
        final int NUMBER_OF_PRESERVED_LABELS = 20;
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            for ( int i = 0; i < TOTAL_NUMBER_OF_LABELS; i++ )
            {
                node.addLabel( label( "label:" + i ) );
            }

            tx.success();
        }

        // when
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = NUMBER_OF_PRESERVED_LABELS; i < TOTAL_NUMBER_OF_LABELS; i++ )
            {
                node.removeLabel( label( "label:" + i ) );
            }

            tx.success();
        }

        // then
        try ( Transaction transaction = db.beginTx() )
        {
            List<String> labels = new ArrayList<>();
            for ( Label label : node.getLabels() )
            {
                labels.add( label.name() );
            }
            assertEquals( "labels on node: " + labels, NUMBER_OF_PRESERVED_LABELS, labels.size() );
        }
    }

    @Test
    public void shouldAllowManyLabelsAndPropertyCursor() throws Exception
    {
        int propertyCount = 10;
        int labelCount = 15;

        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            for ( int i = 0; i < propertyCount; i++ )
            {
                node.setProperty( "foo" + i, "bar" );
            }
            for ( int i = 0; i < labelCount; i++ )
            {
                node.addLabel( label( "label" + i ) );
            }
            tx.success();
        }

        Set<Integer> seenProperties = new HashSet<>();
        Set<Integer> seenLabels = new HashSet<>();
        try ( Transaction tx = db.beginTx() )
        {
            DependencyResolver resolver = db.getDependencyResolver();
            ThreadToStatementContextBridge bridge = resolver.resolveDependency( ThreadToStatementContextBridge.class );
            try ( Statement statement = bridge.getTopLevelTransactionBoundToThisThread( true ).acquireStatement() )
            {
                try ( Cursor<NodeItem> nodeCursor = statement.readOperations().nodeCursorById( node.getId() ) )
                {
                    try ( Cursor<PropertyItem> properties = statement.readOperations()
                            .nodeGetProperties( nodeCursor.get() ) )
                    {
                        while ( properties.next() )
                        {
                            seenProperties.add( properties.get().propertyKeyId() );
                            consume( nodeCursor.get().labels().iterator(), seenLabels::add );
                        }
                    }
                }
            }
            tx.success();
        }

        assertEquals( propertyCount, seenProperties.size() );
        assertEquals( labelCount, seenLabels.size() );
    }

    @Test
    public void nodeWithManyLabels()
    {
        int labels = 500;
        int halveLabels = labels / 2;
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        long nodeId = createNode( db ).getId();

        addLabels( nodeId, 0, halveLabels );
        addLabels( nodeId, halveLabels, halveLabels );

        verifyLabels( nodeId, 0, labels );

        removeLabels( nodeId, halveLabels, halveLabels );
        verifyLabels( nodeId, 0, halveLabels );

        removeLabels( nodeId, 0, halveLabels - 2 );
        verifyLabels( nodeId, halveLabels - 2, 2 );
    }

    private void addLabels( long nodeId, int startLabelIndex, int count )
    {
        try ( Transaction tx = dbRule.beginTx() )
        {
            Node node = dbRule.getNodeById( nodeId );
            int endLabelIndex = startLabelIndex + count;
            for ( int i = startLabelIndex; i < endLabelIndex; i++ )
            {
                node.addLabel( labelWithIndex( i ) );
            }
            tx.success();
        }
    }

    private void verifyLabels( long nodeId, int startLabelIndex, int count )
    {
        try ( Transaction tx = dbRule.beginTx() )
        {
            Node node = dbRule.getNodeById( nodeId );
            Set<String> labelNames = Iterables.asList( node.getLabels() )
                    .stream()
                    .map( Label::name )
                    .sorted()
                    .collect( toSet() );

            assertEquals( count, labelNames.size() );
            int endLabelIndex = startLabelIndex + count;
            for ( int i = startLabelIndex; i < endLabelIndex; i++ )
            {
                assertTrue( labelNames.contains( labelName( i ) ) );
            }
            tx.success();
        }
    }

    private void removeLabels( long nodeId, int startLabelIndex, int count )
    {
        try ( Transaction tx = dbRule.beginTx() )
        {
            Node node = dbRule.getNodeById( nodeId );
            int endLabelIndex = startLabelIndex + count;
            for ( int i = startLabelIndex; i < endLabelIndex; i++ )
            {
                node.removeLabel( labelWithIndex( i ) );
            }
            tx.success();
        }
    }

    private static Label labelWithIndex( int index )
    {
        return label( labelName( index ) );
    }

    private static String labelName( int index )
    {
        return "Label-" + index;
    }

    @SuppressWarnings( "deprecation" )
    private GraphDatabaseService beansAPIWithNoMoreLabelIds()
    {
        final EphemeralIdGenerator.Factory idFactory = new EphemeralIdGenerator.Factory()
        {
            private IdTypeConfigurationProvider
                    idTypeConfigurationProvider = new CommunityIdTypeConfigurationProvider();

            @Override
            public IdGenerator open( File fileName, int grabSize, IdType idType, Supplier<Long> highId, long maxId )
            {
                if ( idType == IdType.LABEL_TOKEN )
                {
                    IdGenerator generator = generators.get( idType );
                    if ( generator == null )
                    {
                        IdTypeConfiguration idTypeConfiguration =
                                idTypeConfigurationProvider.getIdTypeConfiguration( idType );
                        generator = new EphemeralIdGenerator( idType, idTypeConfiguration )
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
                return super.open( fileName, grabSize, idType, () -> Long.MAX_VALUE, Long.MAX_VALUE );
            }
        };

        TestGraphDatabaseFactory dbFactory = new TestGraphDatabaseFactory()
        {
            @Override
            protected GraphDatabaseBuilder.DatabaseCreator createImpermanentDatabaseCreator(
                    final File storeDir, final TestGraphDatabaseFactoryState state )
            {
                return new GraphDatabaseBuilder.DatabaseCreator()
                {
                    @Override
                    public GraphDatabaseService newDatabase( Map<String,String> config )
                    {
                        return newDatabase( Config.defaults( config ) );
                    }

                    @Override
                    public GraphDatabaseService newDatabase( @Nonnull Config config )
                    {
                        return new ImpermanentGraphDatabase( storeDir, config,
                                GraphDatabaseDependencies.newDependencies( state.databaseDependencies() ) )
                        {
                            @Override
                            protected void create(
                                    File storeDir,
                                    Config config,
                                    GraphDatabaseFacadeFactory.Dependencies dependencies )
                            {
                                Function<PlatformModule,EditionModule> factory =
                                        platformModule -> new CommunityEditionModule( platformModule )
                                        {
                                            @Override
                                            protected IdGeneratorFactory createIdGeneratorFactory(
                                                    FileSystemAbstraction fs,
                                                    IdTypeConfigurationProvider idTypeConfigurationProvider )
                                            {
                                                return idFactory;
                                            }
                                        };
                                new GraphDatabaseFacadeFactory( DatabaseInfo.COMMUNITY, factory )
                                {

                                    @Override
                                    protected PlatformModule createPlatform( File storeDir, Config config,
                                            Dependencies dependencies, GraphDatabaseFacade graphDatabaseFacade )
                                    {
                                        return new ImpermanentPlatformModule( storeDir, config, databaseInfo,
                                                dependencies, graphDatabaseFacade );
                                    }
                                }.initFacade( storeDir, config, dependencies, this );
                            }
                        };
                    }
                };
            }
        };

        return dbFactory.newImpermanentDatabase( testDirectory.directory( "impermanent-directory" ) );
    }

    private Node createNode( GraphDatabaseService db, Label... labels )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( labels );
            tx.success();
            return node;
        }
    }
}
