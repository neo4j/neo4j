/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.factory.module.id.IdContextFactory;
import org.neo4j.graphdb.factory.module.id.IdContextFactoryBuilder;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.util.concurrent.BinaryLatch;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.helpers.collection.Iterables.asList;
import static org.neo4j.internal.helpers.collection.Iterables.map;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier.EMPTY;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.hasLabel;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.hasLabels;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.hasNoLabels;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.hasNoNodes;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.hasNodes;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.inTx;

@ImpermanentDbmsExtension
class LabelsAcceptanceTest
{
    @Inject
    private GraphDatabaseAPI db;

    private enum Labels implements Label
    {
        MY_LABEL,
        MY_OTHER_LABEL
    }

    /** https://github.com/neo4j/neo4j/issues/1279 */
    @Test
    void shouldInsertLabelsWithoutDuplicatingThem()
    {
        // Given
        Node node;

        // When
        try ( Transaction tx = db.beginTx() )
        {
            node = tx.createNode();
            node.addLabel( Labels.MY_LABEL );

            tx.commit();
        }

        // POST "FOOBAR"
        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( node.getId() ).addLabel( Labels.MY_LABEL );
            tx.commit();
        }

        // POST ["BAZQUX"]
        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( node.getId() ).addLabel( label( "BAZQUX" ) );
            tx.commit();
        }
        // PUT ["BAZQUX"]
        try ( Transaction tx = db.beginTx() )
        {
            var labeledNode = tx.getNodeById( node.getId() );
            for ( Label label : labeledNode.getLabels() )
            {
                labeledNode.removeLabel( label );
            }
            labeledNode.addLabel( label( "BAZQUX" ) );
            tx.commit();
        }

        // GET
        List<Label> labels = new ArrayList<>();
        try ( Transaction tx = db.beginTx() )
        {
            var labeledNode = tx.getNodeById( node.getId() );
            for ( Label label : labeledNode.getLabels() )
            {
                labels.add( label );
            }
            tx.commit();
        }
        assertEquals( 1, labels.size(), labels.toString() );
        assertEquals( "BAZQUX", labels.get( 0 ).name() );
    }

    @Test
    void addingALabelUsingAValidIdentifierShouldSucceed()
    {
        // Given
        Node myNode;

        // When
        try ( Transaction tx = db.beginTx() )
        {
            myNode = tx.createNode();
            myNode.addLabel( Labels.MY_LABEL );

            tx.commit();
        }

        // Then
        assertThat( "Label should have been added to node", myNode,
                inTx( db, hasLabel( Labels.MY_LABEL ) ) );
    }

    @Test
    void addingALabelUsingAnInvalidIdentifierShouldFail()
    {
        // When I set an empty label
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode().addLabel( label( "" ) );
            fail( "Should have thrown exception" );
        }
        catch ( ConstraintViolationException ex )
        {   // Happy
        }

        // And When I set a null label
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode().addLabel( () -> null );
            fail( "Should have thrown exception" );
        }
        catch ( ConstraintViolationException ex )
        {   // Happy
        }
    }

    @Test
    void addingALabelThatAlreadyExistsBehavesAsNoOp()
    {
        // Given
        Node myNode;

        // When
        try ( Transaction tx = db.beginTx() )
        {
            myNode = tx.createNode();
            myNode.addLabel( Labels.MY_LABEL );
            myNode.addLabel( Labels.MY_LABEL );

            tx.commit();
        }

        // Then
        assertThat( "Label should have been added to node", myNode,
                inTx( db, hasLabel( Labels.MY_LABEL ) ) );
    }

    @Test
    void oversteppingMaxNumberOfLabelsShouldFailGracefully() throws IOException
    {
        JobScheduler scheduler = JobSchedulerFactory.createScheduler();
        try ( EphemeralFileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();
                Lifespan lifespan = new Lifespan( scheduler );
                PageCache pageCache = new MuninnPageCache( swapper( fileSystem ), 1_000, PageCacheTracer.NULL, EMPTY, scheduler ) )
        {
            // Given
            Dependencies dependencies = new Dependencies();
            dependencies.satisfyDependencies( createIdContextFactoryWithMaxedOutLabelTokenIds( fileSystem, scheduler ) );

            DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder()
                    .setFileSystem( fileSystem )
                    .noOpSystemGraphInitializer()
                    .setExternalDependencies( dependencies )
                    .impermanent()
                    .build();

            GraphDatabaseService graphDatabase = managementService.database( DEFAULT_DATABASE_NAME );

            // When
            try ( Transaction tx = graphDatabase.beginTx() )
            {
                tx.createNode().addLabel( Labels.MY_LABEL );
                fail( "Should have thrown exception" );
            }
            catch ( ConstraintViolationException ex )
            {   // Happy
            }

            managementService.shutdown();
        }
    }

    @Test
    void removingCommittedLabel()
    {
        // Given
        Label label = Labels.MY_LABEL;
        Node myNode = createNode( db, label );

        // When
        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( myNode.getId() ).removeLabel( label );
            tx.commit();
        }

        // Then
        assertThat( myNode, not( inTx( db, hasLabel( label ) ) ) );
    }

    @Test
    void createNodeWithLabels()
    {
        // WHEN
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = tx.createNode( Labels.values() );
            tx.commit();
        }

        // THEN

        Set<String> names = Stream.of( Labels.values() ).map( Labels::name ).collect( toSet() );
        assertThat( node, inTx( db, hasLabels( names ) ) );
    }

    @Test
    void removingNonExistentLabel()
    {
        // Given
        Label label = Labels.MY_LABEL;

        // When
        Node myNode;
        try ( Transaction tx = db.beginTx() )
        {
            myNode = tx.createNode();
            myNode.removeLabel( label );
            tx.commit();
        }

        // THEN
        assertThat( myNode, not( inTx( db, hasLabel( label ) ) ) );
    }

    @Test
    void removingExistingLabelFromUnlabeledNode()
    {
        // Given
        Label label = Labels.MY_LABEL;
        createNode( db, label );
        Node myNode = createNode( db );

        // When
        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( myNode.getId() ).removeLabel( label );
            tx.commit();
        }

        // THEN
        assertThat( myNode, not( inTx( db, hasLabel( label ) ) ) );
    }

    @Test
    void removingUncommittedLabel()
    {
        // Given
        Label label = Labels.MY_LABEL;

        // When
        Node myNode;
        try ( Transaction tx = db.beginTx() )
        {
            myNode = tx.createNode();
            myNode.addLabel( label );
            myNode.removeLabel( label );

            // THEN
            assertFalse( myNode.hasLabel( label ) );

            tx.commit();
        }
    }

    @Test
    void shouldBeAbleToListLabelsForANode()
    {
        // GIVEN
        Node node;
        Set<String> expected = asSet( Labels.MY_LABEL.name(), Labels.MY_OTHER_LABEL.name() );
        try ( Transaction tx = db.beginTx() )
        {
            node = tx.createNode();
            for ( String label : expected )
            {
                node.addLabel( label( label ) );
            }
            tx.commit();
        }

        assertThat( node, inTx( db, hasLabels( expected ) ) );
    }

    @Test
    void shouldReturnEmptyListIfNoLabels()
    {
        // GIVEN
        Node node = createNode( db );

        // WHEN THEN
        assertThat( node, inTx( db, hasNoLabels() ) );
    }

    @Test
    void getNodesWithLabelCommitted()
    {
        // When
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = tx.createNode();
            node.addLabel( Labels.MY_LABEL );
            tx.commit();
        }

        // THEN
        try ( Transaction transaction = db.beginTx() )
        {
            assertThat( transaction, hasNodes( Labels.MY_LABEL, node ) );
            assertThat( transaction, hasNoNodes( Labels.MY_OTHER_LABEL ) );
        }
    }

    @Test
    void getNodesWithLabelsWithTxAddsAndRemoves()
    {
        // GIVEN
        Node node1 = createNode( db, Labels.MY_LABEL, Labels.MY_OTHER_LABEL );
        Node node2 = createNode( db, Labels.MY_LABEL, Labels.MY_OTHER_LABEL );

        // WHEN
        Node node3;
        Set<Node> nodesWithMyLabel;
        Set<Node> nodesWithMyOtherLabel;
        try ( Transaction tx = db.beginTx() )
        {
            node3 = tx.createNode( Labels.MY_LABEL );
            tx.getNodeById( node2.getId() ).removeLabel( Labels.MY_LABEL );
            // extracted here to be asserted below
            nodesWithMyLabel = asSet( tx.findNodes( Labels.MY_LABEL ) );
            nodesWithMyOtherLabel = asSet( tx.findNodes( Labels.MY_OTHER_LABEL ) );
            tx.commit();
        }

        // THEN
        assertEquals( asSet( node1, node3 ), nodesWithMyLabel );
        assertEquals( asSet( node1, node2 ), nodesWithMyOtherLabel );
    }

    @Test
    void shouldListAllExistingLabels()
    {
        // Given
        createNode( db, Labels.MY_LABEL, Labels.MY_OTHER_LABEL );
        List<Label> labels;

        // When
        try ( Transaction transaction = db.beginTx() )
        {
            labels = asList( transaction.getAllLabels() );
        }

        // Then
        assertEquals( 2, labels.size() );
        assertThat( map( Label::name, labels ), hasItems( Labels.MY_LABEL.name(), Labels.MY_OTHER_LABEL.name() ) );
    }

    @Test
    void shouldListAllLabelsInUse()
    {
        // Given
        createNode( db, Labels.MY_LABEL );
        Node node = createNode( db, Labels.MY_OTHER_LABEL );
        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( node.getId() ).delete();
            tx.commit();
        }

        // When
        List<Label> labels;
        try ( Transaction tx = db.beginTx() )
        {
            labels = asList( tx.getAllLabelsInUse() );
        }

        // Then
        assertEquals( 1, labels.size() );
        assertThat( map( Label::name, labels ), hasItems( Labels.MY_LABEL.name() ) );
    }

    @Test
    void shouldListAllLabelsInUseEvenWhenExclusiveLabelLocksAreTaken()
    {
        assertTimeoutPreemptively( Duration.ofSeconds(30), () ->
        {
            // Given
            createNode( db, Labels.MY_LABEL );
            Node node = createNode( db, Labels.MY_OTHER_LABEL );
            try ( Transaction tx = db.beginTx() )
            {
                tx.getNodeById( node.getId() ).delete();
                tx.commit();
            }

            BinaryLatch indexCreateStarted = new BinaryLatch();
            BinaryLatch indexCreateAllowToFinish = new BinaryLatch();
            Thread indexCreator = new Thread( () ->
            {
                try ( Transaction tx = db.beginTx() )
                {
                    tx.schema().indexFor( Labels.MY_LABEL ).on( "prop" ).create();
                    indexCreateStarted.release();
                    indexCreateAllowToFinish.await();
                    tx.commit();
                }
            } );
            indexCreator.start();

            // When
            indexCreateStarted.await();
            List<Label> labels;
            try ( Transaction tx = db.beginTx() )
            {
                labels = asList( tx.getAllLabelsInUse() );
            }
            indexCreateAllowToFinish.release();
            indexCreator.join();

            // Then
            assertEquals( 1, labels.size() );
            assertThat( map( Label::name, labels ), hasItems( Labels.MY_LABEL.name() ) );
        } );
    }

    @Test
    void shouldListAllRelationshipTypesInUseEvenWhenExclusiveRelationshipTypeLocksAreTaken()
    {
        assertTimeoutPreemptively( Duration.ofSeconds( 30 ), () ->
        {
            // Given
            RelationshipType relType = RelationshipType.withName( "REL" );
            Node node = createNode( db, Labels.MY_LABEL );
            try ( Transaction tx = db.beginTx() )
            {
                tx.getNodeById( node.getId() ).createRelationshipTo( node, relType ).setProperty( "prop", "val" );
                tx.commit();
            }

            BinaryLatch indexCreateStarted = new BinaryLatch();
            BinaryLatch indexCreateAllowToFinish = new BinaryLatch();
            Thread indexCreator = new Thread( () ->
            {
                try ( Transaction tx = db.beginTx() )
                {
                    tx.execute( "CALL db.index.fulltext.createRelationshipIndex('myIndex', ['REL'], ['prop'] )" ).close();
                    indexCreateStarted.release();
                    indexCreateAllowToFinish.await();
                    tx.commit();
                }
            } );
            indexCreator.start();

            // When
            indexCreateStarted.await();
            List<RelationshipType> relTypes;
            try ( Transaction transaction = db.beginTx() )
            {
                relTypes = asList( transaction.getAllRelationshipTypesInUse() );
            }
            indexCreateAllowToFinish.release();
            indexCreator.join();

            // Then
            assertEquals( 1, relTypes.size() );
            assertThat( map( RelationshipType::name, relTypes ), hasItems( relType.name() ) );
        } );
    }

    @Test
    void deleteAllNodesAndTheirLabels()
    {
        // GIVEN
        final Label label = label( "A" );
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode();
            node.addLabel( label );
            node.setProperty( "name", "bla" );
            tx.commit();
        }

        // WHEN
        try ( Transaction tx = db.beginTx() )
        {
            for ( final Node node : tx.getAllNodes() )
            {
                node.removeLabel( label ); // remove Label ...
                node.delete(); // ... and afterwards the node
            }
            tx.commit();
        } // tx.close(); - here comes the exception

        // THEN
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 0, Iterables.count( tx.getAllNodes() ) );
        }
    }

    @Test
    void removingLabelDoesNotBreakPreviouslyCreatedLabelsIterator()
    {
        // GIVEN
        Label label1 = label( "A" );
        Label label2 = label( "B" );

        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( label1, label2 );

            for ( Label next : node.getLabels() )
            {
                node.removeLabel( next );
            }
            tx.commit();
        }
    }

    @Test
    void removingPropertyDoesNotBreakPreviouslyCreatedNodePropertyKeysIterator()
    {
        // GIVEN
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode();
            node.setProperty( "name", "Horst" );
            node.setProperty( "age", "72" );

            for ( String key : node.getPropertyKeys() )
            {
                node.removeProperty( key );
            }
            tx.commit();
        }
    }

    @Test
    void shouldCreateNodeWithLotsOfLabelsAndThenRemoveMostOfThem()
    {
        // given
        final int TOTAL_NUMBER_OF_LABELS = 200;
        final int NUMBER_OF_PRESERVED_LABELS = 20;
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = tx.createNode();
            for ( int i = 0; i < TOTAL_NUMBER_OF_LABELS; i++ )
            {
                node.addLabel( label( "label:" + i ) );
            }

            tx.commit();
        }

        // when
        try ( Transaction tx = db.beginTx() )
        {
            var labeledNode = tx.getNodeById( node.getId() );
            for ( int i = NUMBER_OF_PRESERVED_LABELS; i < TOTAL_NUMBER_OF_LABELS; i++ )
            {
                labeledNode.removeLabel( label( "label:" + i ) );
            }

            tx.commit();
        }

        // then
        try ( Transaction tx = db.beginTx() )
        {
            List<String> labels = new ArrayList<>();
            var labeledNode = tx.getNodeById( node.getId() );
            for ( Label label : labeledNode.getLabels() )
            {
                labels.add( label.name() );
            }
            assertEquals( NUMBER_OF_PRESERVED_LABELS, labels.size(), "labels on node: " + labels );
        }
    }

    @Test
    void shouldAllowManyLabelsAndPropertyCursor()
    {
        int propertyCount = 10;
        int labelCount = 15;

        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = tx.createNode();
            for ( int i = 0; i < propertyCount; i++ )
            {
                node.setProperty( "foo" + i, "bar" );
            }
            for ( int i = 0; i < labelCount; i++ )
            {
                node.addLabel( label( "label" + i ) );
            }
            tx.commit();
        }

        Set<Integer> seenProperties = new HashSet<>();
        Set<Integer> seenLabels = new HashSet<>();
        try ( Transaction tx = db.beginTx() )
        {
            DependencyResolver resolver = db.getDependencyResolver();
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            try ( NodeCursor nodes = ktx.cursors().allocateNodeCursor();
                  PropertyCursor propertyCursor = ktx.cursors().allocatePropertyCursor() )
            {
                ktx.dataRead().singleNode( node.getId(), nodes );
                while ( nodes.next() )
                {
                    nodes.properties( propertyCursor );
                    while ( propertyCursor.next() )
                    {
                        seenProperties.add( propertyCursor.propertyKey() );
                    }

                    LabelSet labels = nodes.labels();
                    for ( int i = 0; i < labels.numberOfLabels(); i++ )
                    {
                        seenLabels.add( labels.label( i ) );
                    }
                }
            }
            tx.commit();
        }

        assertEquals( propertyCount, seenProperties.size() );
        assertEquals( labelCount, seenLabels.size() );
    }

    @Test
    void nodeWithManyLabels()
    {
        int labels = 500;
        int halveLabels = labels / 2;
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
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.getNodeById( nodeId );
            int endLabelIndex = startLabelIndex + count;
            for ( int i = startLabelIndex; i < endLabelIndex; i++ )
            {
                node.addLabel( labelWithIndex( i ) );
            }
            tx.commit();
        }
    }

    private void verifyLabels( long nodeId, int startLabelIndex, int count )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.getNodeById( nodeId );
            Set<String> labelNames = Iterables.asList( node.getLabels() )
                    .stream()
                    .map( Label::name )
                    .collect( toSet() );

            assertEquals( count, labelNames.size() );
            int endLabelIndex = startLabelIndex + count;
            for ( int i = startLabelIndex; i < endLabelIndex; i++ )
            {
                assertTrue( labelNames.contains( labelName( i ) ) );
            }
            tx.commit();
        }
    }

    private void removeLabels( long nodeId, int startLabelIndex, int count )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.getNodeById( nodeId );
            int endLabelIndex = startLabelIndex + count;
            for ( int i = startLabelIndex; i < endLabelIndex; i++ )
            {
                node.removeLabel( labelWithIndex( i ) );
            }
            tx.commit();
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

    private static Node createNode( GraphDatabaseService db, Label... labels )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( labels );
            tx.commit();
            return node;
        }
    }

    private IdContextFactory createIdContextFactoryWithMaxedOutLabelTokenIds( FileSystemAbstraction fileSystem, JobScheduler jobScheduler )
    {
        return IdContextFactoryBuilder.of( fileSystem, jobScheduler, Config.defaults() ).withIdGenerationFactoryProvider(
                any -> new DefaultIdGeneratorFactory( fileSystem, immediate() )
                {
                    @Override
                    public IdGenerator open( PageCache pageCache, File fileName, IdType idType, LongSupplier highId, long maxId, boolean readOnly,
                            OpenOption... openOptions )
                    {
                        return super.open( pageCache, fileName, idType, highId, maxId( idType, maxId, highId ), readOnly, openOptions );
                    }

                    @Override
                    public IdGenerator create( PageCache pageCache, File fileName, IdType idType, long highId, boolean throwIfFileExists, long maxId,
                            boolean readOnly, OpenOption... openOptions )
                    {
                        return super
                                .create( pageCache, fileName, idType, highId, throwIfFileExists, maxId( idType, maxId, () -> highId ), readOnly, openOptions );
                    }

                    private long maxId( IdType idType, long maxId, LongSupplier highId )
                    {
                        return idType == IdType.LABEL_TOKEN ? highId.getAsLong() - 1 : maxId;
                    }
                } ).build();
    }

    private static PageSwapperFactory swapper( EphemeralFileSystemAbstraction fileSystem )
    {
        return new SingleFilePageSwapperFactory( fileSystem );
    }
}
