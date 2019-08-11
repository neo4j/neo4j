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
package org.neo4j.kernel.api.impl.fulltext;

import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipIndexCursor;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.impl.api.KernelImpl;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.newapi.ExtendedNodeValueIndexCursorAdapter;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.EmbeddedDbmsRule;
import org.neo4j.test.rule.VerboseTimeout;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.kernel.api.IndexQuery.fulltextSearch;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProviderFactory.DESCRIPTOR;
import static org.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.NODE_CREATE;
import static org.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.RELATIONSHIP_CREATE;
import static org.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.array;
import static org.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.assertQueryFindsIds;

public class FulltextIndexProviderTest
{
    private static final String NAME = "fulltext";

    @Rule
    public Timeout timeout = VerboseTimeout.builder().withTimeout( 1, TimeUnit.MINUTES ).build();

    @Rule
    public DbmsRule db = new EmbeddedDbmsRule();

    private Node node1;
    private Node node2;
    private int labelIdHej;
    private int labelIdHa;
    private int labelIdHe;
    private int propIdHej;
    private int propIdHa;
    private int propIdHe;
    private int propIdHo;

    @Before
    public void prepDB()
    {
        Label hej = label( "hej" );
        Label ha = label( "ha" );
        Label he = label( "he" );
        try ( Transaction transaction = db.beginTx() )
        {
            node1 = db.createNode( hej, ha, he );
            node1.setProperty( "hej", "value" );
            node1.setProperty( "ha", "value1" );
            node1.setProperty( "he", "value2" );
            node1.setProperty( "ho", "value3" );
            node2 = db.createNode();
            Relationship rel = node1.createRelationshipTo( node2, RelationshipType.withName( "hej" ) );
            rel.setProperty( "hej", "valuuu" );
            rel.setProperty( "ha", "value1" );
            rel.setProperty( "he", "value2" );
            rel.setProperty( "ho", "value3" );

            transaction.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            TokenRead tokenRead = tokenRead();
            labelIdHej = tokenRead.nodeLabel( hej.name() );
            labelIdHa = tokenRead.nodeLabel( ha.name() );
            labelIdHe = tokenRead.nodeLabel( he.name() );
            propIdHej = tokenRead.propertyKey( "hej" );
            propIdHa = tokenRead.propertyKey( "ha" );
            propIdHe = tokenRead.propertyKey( "he" );
            propIdHo = tokenRead.propertyKey( "ho" );
            tx.commit();
        }
    }

    @Test
    public void createFulltextIndex() throws Exception
    {
        IndexDescriptor fulltextIndex = createIndex( new int[]{labelIdHej, labelIdHa, labelIdHe}, new int[]{propIdHej, propIdHa, propIdHe} );
        try ( KernelTransactionImplementation transaction = getKernelTransaction() )
        {
            IndexDescriptor descriptor = transaction.schemaRead().indexGetForName( NAME );
            assertEquals( descriptor.schema(), fulltextIndex.schema() );
            transaction.success();
        }
    }

    @Test
    public void shouldHaveAReasonableDirectoryStructure() throws Exception
    {
        createIndex( new int[]{labelIdHej, labelIdHa, labelIdHe}, new int[]{propIdHej, propIdHa, propIdHe} );
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.HOURS );
            tx.commit();
        }
        try ( KernelTransactionImplementation transaction = getKernelTransaction() )
        {
            IndexDescriptor descriptor = transaction.schemaRead().indexGetForName( NAME );
            File indexDir = Path.of( db.databaseLayout().databaseDirectory().getAbsolutePath(),
                    "schema", "index", descriptor.getIndexProvider().name(), "" + descriptor.getId() ).toFile();
            List<File> listFiles = List.of( requireNonNull( indexDir.listFiles() ) );
            assertTrue( listFiles.contains( new File( indexDir, "failure-message" ) ) );
            assertTrue( listFiles.contains( new File( indexDir, "1" ) ) );
            assertTrue( listFiles.contains( new File( indexDir, indexDir.getName() + ".tx" ) ) );
        }
    }

    @Test
    public void createAndRetainFulltextIndex() throws Exception
    {
        IndexDescriptor fulltextIndex = createIndex( new int[]{labelIdHej, labelIdHa, labelIdHe}, new int[]{propIdHej, propIdHa, propIdHe} );
        db.restartDatabase( DbmsRule.RestartAction.EMPTY );

        verifyThatFulltextIndexIsPresent( fulltextIndex );
    }

    @Test
    public void createAndRetainRelationshipFulltextIndex() throws Exception
    {
        IndexDescriptor indexReference;
        try ( KernelTransactionImplementation transaction = getKernelTransaction() )
        {
            SchemaDescriptor schema = SchemaDescriptor.fulltext( EntityType.RELATIONSHIP, IndexConfig.empty(), new int[]{labelIdHej, labelIdHa, labelIdHe},
                    new int[]{propIdHej, propIdHa, propIdHe, propIdHo} );
            indexReference = transaction.schemaWrite().indexCreate( schema, DESCRIPTOR.name(), "fulltext" );
            transaction.success();
        }
        await( indexReference );
        db.restartDatabase( DbmsRule.RestartAction.EMPTY );

        verifyThatFulltextIndexIsPresent( indexReference );
    }

    @Test
    public void createAndQueryFulltextIndex() throws Exception
    {
        IndexDescriptor indexReference;
        indexReference = createIndex( new int[]{labelIdHej, labelIdHa, labelIdHe}, new int[]{propIdHej, propIdHa, propIdHe, propIdHo} );
        await( indexReference );
        long thirdNodeId;
        thirdNodeId = createTheThirdNode();
        verifyNodeData( thirdNodeId );
        db.restartDatabase( DbmsRule.RestartAction.EMPTY );
        verifyNodeData( thirdNodeId );
    }

    @Test
    public void createAndQueryFulltextRelationshipIndex() throws Exception
    {
        IndexDescriptor indexReference;
        try ( KernelTransactionImplementation transaction = getKernelTransaction() )
        {
            SchemaDescriptor schema = SchemaDescriptor.fulltext( EntityType.RELATIONSHIP, IndexConfig.empty(), new int[]{labelIdHej, labelIdHa, labelIdHe},
                    new int[]{propIdHej, propIdHa, propIdHe, propIdHo} );
            indexReference = transaction.schemaWrite().indexCreate( schema, DESCRIPTOR.name(), "fulltext" );
            transaction.success();
        }
        await( indexReference );
        long secondRelId;
        try ( Transaction transaction = db.beginTx() )
        {
            Relationship ho = node1.createRelationshipTo( node2, RelationshipType.withName( "ho" ) );
            secondRelId = ho.getId();
            ho.setProperty( "hej", "villa" );
            ho.setProperty( "ho", "value3" );
            transaction.commit();
        }
        verifyRelationshipData( secondRelId );
        db.restartDatabase( DbmsRule.RestartAction.EMPTY );
        verifyRelationshipData( secondRelId );
    }

    @Test
    public void multiTokenFulltextIndexesMustShowUpInSchemaGetIndexes()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, "nodeIndex", array( "Label1", "Label2" ), array( "prop1", "prop2" ) ) ).close();
            db.execute( format( RELATIONSHIP_CREATE, "relIndex", array( "RelType1", "RelType2" ), array( "prop1", "prop2" ) ) ).close();
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            for ( IndexDefinition index : db.schema().getIndexes() )
            {
                assertFalse( index.isConstraintIndex() );
                assertTrue( index.isMultiTokenIndex() );
                assertTrue( index.isCompositeIndex() );
                if ( index.isNodeIndex() )
                {
                    assertFalse( index.isRelationshipIndex() );
                    assertThat( index.getLabels(), containsInAnyOrder( Label.label( "Label1" ), Label.label( "Label2" ) ) );
                    try
                    {
                        //noinspection deprecation
                        index.getLabel();
                        fail( "index.getLabel() on multi-token IndexDefinition should have thrown." );
                    }
                    catch ( IllegalStateException ignore )
                    {
                    }
                    try
                    {
                        index.getRelationshipTypes();
                        fail( "index.getRelationshipTypes() on node IndexDefinition should have thrown." );
                    }
                    catch ( IllegalStateException ignore )
                    {
                    }
                }
                else
                {
                    assertTrue( index.isRelationshipIndex() );
                    assertThat( index.getRelationshipTypes(),
                            containsInAnyOrder( RelationshipType.withName( "RelType1" ), RelationshipType.withName( "RelType2" ) ) );
                    try
                    {
                        //noinspection deprecation
                        index.getRelationshipType();
                        fail( "index.getRelationshipType() on multi-token IndexDefinition should have thrown." );
                    }
                    catch ( IllegalStateException ignore )
                    {
                    }
                    try
                    {
                        index.getLabels();
                        fail( "index.getLabels() on node IndexDefinition should have thrown." );
                    }
                    catch ( IllegalStateException ignore )
                    {
                    }
                }
            }
            tx.commit();
        }
    }

    @Test
    public void awaitIndexesOnlineMustWorkOnFulltextIndexes()
    {
        String prop1 = "prop1";
        String prop2 = "prop2";
        String prop3 = "prop3";
        String val1 = "foo foo";
        String val2 = "bar bar";
        String val3 = "baz baz";
        Label label1 = Label.label( "FirstLabel" );
        Label label2 = Label.label( "SecondLabel" );
        Label label3 = Label.label( "ThirdLabel" );
        RelationshipType relType1 = RelationshipType.withName( "FirstRelType" );
        RelationshipType relType2 = RelationshipType.withName( "SecondRelType" );
        RelationshipType relType3 = RelationshipType.withName( "ThirdRelType" );

        LongHashSet nodes1 = new LongHashSet();
        LongHashSet nodes2 = new LongHashSet();
        LongHashSet nodes3 = new LongHashSet();
        LongHashSet rels1 = new LongHashSet();
        LongHashSet rels2 = new LongHashSet();
        LongHashSet rels3 = new LongHashSet();

        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < 100; i++ )
            {
                Node node1 = db.createNode( label1 );
                node1.setProperty( prop1, val1 );
                nodes1.add( node1.getId() );
                Relationship rel1 = node1.createRelationshipTo( node1, relType1 );
                rel1.setProperty( prop1, val1 );
                rels1.add( rel1.getId() );

                Node node2 = db.createNode( label2 );
                node2.setProperty( prop2, val2 );
                nodes2.add( node2.getId() );
                Relationship rel2 = node1.createRelationshipTo( node2, relType2 );
                rel2.setProperty( prop2, val2 );
                rels2.add( rel2.getId() );

                Node node3 = db.createNode( label3 );
                node3.setProperty( prop3, val3 );
                nodes3.add( node3.getId() );
                Relationship rel3 = node1.createRelationshipTo( node3, relType3 );
                rel3.setProperty( prop3, val3 );
                rels3.add( rel3.getId() );
            }
            tx.commit();
        }

        // Test that multi-token node indexes can be waited for.
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, "nodeIndex",
                    array( label1.name(), label2.name(), label3.name() ),
                    array( prop1, prop2, prop3 ) ) ).close();
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
            tx.commit();
        }

        assertQueryFindsIds( db, true, "nodeIndex", "foo", nodes1 );
        assertQueryFindsIds( db, true, "nodeIndex", "bar", nodes2 );
        assertQueryFindsIds( db, true, "nodeIndex", "baz", nodes3 );

        // Test that multi-token relationship indexes can be waited for.
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( RELATIONSHIP_CREATE, "relIndex",
                    array( relType1.name(), relType2.name(), relType3.name() ),
                    array( prop1, prop2, prop3 ) ) ).close();
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
            tx.commit();
        }

        assertQueryFindsIds( db, false, "relIndex", "foo", rels1 );
        assertQueryFindsIds( db, false, "relIndex", "bar", rels2 );
        assertQueryFindsIds( db, false, "relIndex", "baz", rels3 );
    }

    @Test
    public void queryingWithIndexProgressorMustProvideScore() throws Exception
    {
        long nodeId = createTheThirdNode();
        IndexDescriptor index;
        index = createIndex( new int[]{labelIdHej, labelIdHa, labelIdHe}, new int[]{propIdHej, propIdHa, propIdHe, propIdHo} );
        await( index );
        List<String> acceptedEntities = new ArrayList<>();
        try ( KernelTransactionImplementation ktx = getKernelTransaction() )
        {
            NodeValueIndexCursor cursor = new ExtendedNodeValueIndexCursorAdapter()
            {
                private long nodeReference;
                private IndexProgressor progressor;

                @Override
                public long nodeReference()
                {
                    return nodeReference;
                }

                @Override
                public boolean next()
                {
                    return progressor.next();
                }

                @Override
                public void initialize( IndexDescriptor descriptor, IndexProgressor progressor,
                        IndexQuery[] query, IndexOrder indexOrder, boolean needsValues,
                        boolean indexIncludesTransactionState )
                {
                    this.progressor = progressor;
                }

                @Override
                public boolean acceptEntity( long reference, float score, Value... values )
                {
                    this.nodeReference = reference;
                    assertFalse( "score should not be NaN", Float.isNaN( score ) );
                    assertThat( "score must be positive", score, greaterThan( 0.0f ) );
                    acceptedEntities.add( "reference = " + reference + ", score = " + score + ", " + Arrays.toString( values ) );
                    return true;
                }
            };
            Read read = ktx.dataRead();
            IndexReadSession indexSession = ktx.dataRead().indexReadSession( index );
            read.nodeIndexSeek( indexSession, cursor, IndexOrder.NONE, false, fulltextSearch( "hej:\"villa\"" ) );
            int counter = 0;
            while ( cursor.next() )
            {
                assertThat( cursor.nodeReference(), is( nodeId ) );
                counter++;
            }
            assertThat( counter, is( 1 ) );
            assertThat( acceptedEntities.size(), is( 1 ) );
            acceptedEntities.clear();
        }
    }

    private TokenRead tokenRead()
    {
        return db.getGraphDatabaseAPI().getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class )
                .getKernelTransactionBoundToThisThread( false, db.databaseId() ).tokenRead();
    }

    private KernelTransactionImplementation getKernelTransaction()
    {
        try
        {
            KernelImpl kernel = db.resolveDependency( KernelImpl.class );
            return (KernelTransactionImplementation) kernel.beginTransaction(
                    org.neo4j.internal.kernel.api.Transaction.Type.explicit, LoginContext.AUTH_DISABLED );
        }
        catch ( TransactionFailureException e )
        {
            throw new RuntimeException( "oops" );
        }
    }

    private IndexDescriptor createIndex( int[] entityTokens, int[] propertyIds ) throws KernelException
    {
        IndexDescriptor fulltext;
        try ( KernelTransactionImplementation transaction = getKernelTransaction() )
        {
            SchemaDescriptor schema = SchemaDescriptor.fulltext( EntityType.NODE, IndexConfig.empty(), entityTokens, propertyIds );
            fulltext = transaction.schemaWrite().indexCreate( schema, DESCRIPTOR.name(), NAME );
            transaction.success();
        }
        return fulltext;
    }

    private void verifyThatFulltextIndexIsPresent( IndexDescriptor fulltextIndexDescriptor ) throws TransactionFailureException
    {
        try ( KernelTransactionImplementation transaction = getKernelTransaction() )
        {
            IndexDescriptor descriptor = transaction.schemaRead().indexGetForName( NAME );
            assertEquals( fulltextIndexDescriptor.schema(), descriptor.schema() );
            assertEquals( fulltextIndexDescriptor.isUnique(), descriptor.isUnique() );
            transaction.success();
        }
    }

    private long createTheThirdNode()
    {
        long nodeId;
        try ( Transaction transaction = db.beginTx() )
        {
            Node hej = db.createNode( label( "hej" ) );
            nodeId = hej.getId();
            hej.setProperty( "hej", "villa" );
            hej.setProperty( "ho", "value3" );
            transaction.commit();
        }
        return nodeId;
    }

    private void verifyNodeData( long thirdNodeId ) throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = LuceneFulltextTestSupport.kernelTransaction( tx );
            IndexReadSession index = ktx.dataRead().indexReadSession( ktx.schemaRead().indexGetForName( "fulltext" ) );
            try ( NodeValueIndexCursor cursor = ktx.cursors().allocateNodeValueIndexCursor() )
            {
                ktx.dataRead().nodeIndexSeek( index, cursor, IndexOrder.NONE, false, fulltextSearch( "value" ) );
                assertTrue( cursor.next() );
                assertEquals( 0L, cursor.nodeReference() );
                assertFalse( cursor.next() );

                ktx.dataRead().nodeIndexSeek( index, cursor, IndexOrder.NONE, false, fulltextSearch( "villa" ) );
                assertTrue( cursor.next() );
                assertEquals( thirdNodeId, cursor.nodeReference() );
                assertFalse( cursor.next() );

                ktx.dataRead().nodeIndexSeek( index, cursor, IndexOrder.NONE, false, fulltextSearch( "value3" ) );
                MutableLongSet ids = LongSets.mutable.empty();
                ids.add( 0L );
                ids.add( thirdNodeId );
                assertTrue( cursor.next() );
                assertTrue( ids.remove( cursor.nodeReference() ) );
                assertTrue( cursor.next() );
                assertTrue( ids.remove( cursor.nodeReference() ) );
                assertFalse( cursor.next() );
            }
            tx.commit();
        }
    }

    private void verifyRelationshipData( long secondRelId ) throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = LuceneFulltextTestSupport.kernelTransaction( tx );
            IndexDescriptor index = ktx.schemaRead().indexGetForName( "fulltext" );
            try ( RelationshipIndexCursor cursor = ktx.cursors().allocateRelationshipIndexCursor() )
            {
                ktx.dataRead().relationshipIndexSeek( index, cursor, fulltextSearch( "valuuu" ) );
                assertTrue( cursor.next() );
                assertEquals( 0L, cursor.relationshipReference() );
                assertFalse( cursor.next() );

                ktx.dataRead().relationshipIndexSeek( index, cursor, fulltextSearch( "villa" ) );
                assertTrue( cursor.next() );
                assertEquals( secondRelId, cursor.relationshipReference() );
                assertFalse( cursor.next() );

                ktx.dataRead().relationshipIndexSeek( index, cursor, fulltextSearch( "value3" ) );
                assertTrue( cursor.next() );
                assertEquals( 0L, cursor.relationshipReference() );
                assertTrue( cursor.next() );
                assertEquals( secondRelId, cursor.relationshipReference() );
                assertFalse( cursor.next() );
            }
            tx.commit();
        }
    }

    private void await( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        try ( Transaction ignore = db.beginTx() )
        {
            while ( getKernelTransaction().schemaRead().indexGetState( descriptor ) != InternalIndexState.ONLINE )
            {
                Thread.sleep( 100 );
            }
        }
        catch ( InterruptedException e )
        {
            e.printStackTrace();
        }
    }
}
