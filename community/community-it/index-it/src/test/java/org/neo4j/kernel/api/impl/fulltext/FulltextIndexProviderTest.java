/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.FulltextSettings;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipIndexCursor;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.recordstorage.SchemaStorage;
import org.neo4j.internal.recordstorage.StoreTokens;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.impl.api.KernelImpl;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.newapi.ExtendedNodeValueIndexCursorAdapter;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.extension.DbmsController;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueCategory;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.kernel.api.IndexQuery.fulltextSearch;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.schema.IndexType.FULLTEXT;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.NODE_CREATE;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.RELATIONSHIP_CREATE;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.asStrList;
import static org.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.assertQueryFindsIds;
import static org.neo4j.kernel.impl.index.schema.FulltextIndexProviderFactory.DESCRIPTOR;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@DbmsExtension
class FulltextIndexProviderTest
{
    private static final String NAME = "fulltext";

    @Inject
    DbmsController controller;
    @Inject
    GraphDatabaseAPI db;
    @Inject
    KernelImpl kernel;

    private Node node1;
    private Node node2;
    private int labelIdHej;
    private int labelIdHa;
    private int labelIdHe;
    private int propIdHej;
    private int propIdHa;
    private int propIdHe;
    private int propIdHo;

    @BeforeEach
    void prepDB()
    {
        Label hej = label( "hej" );
        Label ha = label( "ha" );
        Label he = label( "he" );
        try ( Transaction transaction = db.beginTx() )
        {
            node1 = transaction.createNode( hej, ha, he );
            node1.setProperty( "hej", "value" );
            node1.setProperty( "ha", "value1" );
            node1.setProperty( "he", "value2" );
            node1.setProperty( "ho", "value3" );
            node2 = transaction.createNode();
            Relationship rel = node1.createRelationshipTo( node2, RelationshipType.withName( "hej" ) );
            rel.setProperty( "hej", "valuuu" );
            rel.setProperty( "ha", "value1" );
            rel.setProperty( "he", "value2" );
            rel.setProperty( "ho", "value3" );

            transaction.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            TokenRead tokenRead = tokenRead( tx );
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
    void createFulltextIndex() throws Exception
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
    void shouldHaveAReasonableDirectoryStructure() throws Exception
    {
        createIndex( new int[]{labelIdHej, labelIdHa, labelIdHe}, new int[]{propIdHej, propIdHa, propIdHe} );
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 1, TimeUnit.HOURS );
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
    void createAndRetainFulltextIndex() throws Exception
    {
        IndexDescriptor fulltextIndex = createIndex( new int[]{labelIdHej, labelIdHa, labelIdHe}, new int[]{propIdHej, propIdHa, propIdHe} );
        controller.restartDbms();
        verifyThatFulltextIndexIsPresent( fulltextIndex );
    }

    @Test
    void createAndRetainRelationshipFulltextIndex() throws Exception
    {
        IndexDescriptor indexReference;
        try ( KernelTransactionImplementation transaction = getKernelTransaction() )
        {
            SchemaDescriptor schema = SchemaDescriptor.fulltext( EntityType.RELATIONSHIP, new int[]{labelIdHej, labelIdHa, labelIdHe},
                    new int[]{propIdHej, propIdHa, propIdHe, propIdHo} );
            IndexPrototype prototype = IndexPrototype.forSchema( schema, DESCRIPTOR ).withIndexType( FULLTEXT ).withName( "fulltext" );
            indexReference = transaction.schemaWrite().indexCreate( prototype );
            transaction.success();
        }
        await( indexReference );
        controller.restartDbms();

        verifyThatFulltextIndexIsPresent( indexReference );
    }

    @Test
    void createAndQueryFulltextIndex() throws Exception
    {
        IndexDescriptor indexReference;
        indexReference = createIndex( new int[]{labelIdHej, labelIdHa, labelIdHe}, new int[]{propIdHej, propIdHa, propIdHe, propIdHo} );
        await( indexReference );
        long thirdNodeId;
        thirdNodeId = createTheThirdNode();
        verifyNodeData( thirdNodeId );
        controller.restartDbms();
        verifyNodeData( thirdNodeId );
    }

    @Test
    void createAndQueryFulltextRelationshipIndex() throws Exception
    {
        IndexDescriptor indexReference;
        try ( KernelTransactionImplementation transaction = getKernelTransaction() )
        {
            SchemaDescriptor schema = SchemaDescriptor.fulltext( EntityType.RELATIONSHIP, new int[]{labelIdHej, labelIdHa, labelIdHe},
                    new int[]{propIdHej, propIdHa, propIdHe, propIdHo} );
            IndexPrototype prototype = IndexPrototype.forSchema( schema, DESCRIPTOR ).withIndexType( FULLTEXT ).withName( "fulltext" );
            indexReference = transaction.schemaWrite().indexCreate( prototype );
            transaction.success();
        }
        await( indexReference );
        long secondRelId;
        try ( Transaction transaction = db.beginTx() )
        {
            Relationship ho = transaction.getNodeById( node1.getId() )
                    .createRelationshipTo( transaction.getNodeById( node2.getId() ),
                            RelationshipType.withName( "ho" ) );
            secondRelId = ho.getId();
            ho.setProperty( "hej", "villa" );
            ho.setProperty( "ho", "value3" );
            transaction.commit();
        }
        verifyRelationshipData( secondRelId );
        controller.restartDbms();
        verifyRelationshipData( secondRelId );
    }

    @Test
    void multiTokenFulltextIndexesMustShowUpInSchemaGetIndexes()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( NODE_CREATE, "nodeIndex",
                    asStrList( "Label1", "Label2" ),
                    asStrList( "prop1", "prop2" ) ) ).close();
            tx.execute( format( RELATIONSHIP_CREATE, "relIndex",
                    asStrList( "RelType1", "RelType2" ),
                    asStrList( "prop1", "prop2" ) ) ).close();
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            for ( IndexDefinition index : tx.schema().getIndexes() )
            {
                assertFalse( index.isConstraintIndex() );
                assertTrue( index.isMultiTokenIndex() );
                assertTrue( index.isCompositeIndex() );
                if ( index.isNodeIndex() )
                {
                    assertFalse( index.isRelationshipIndex() );
                    assertThat( index.getLabels() ).contains( Label.label( "Label1" ), Label.label( "Label2" ) );
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
                    assertThat( index.getRelationshipTypes() ).contains( RelationshipType.withName( "RelType1" ), RelationshipType.withName( "RelType2" ) );
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
    void awaitIndexesOnlineMustWorkOnFulltextIndexes()
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
                Node node1 = tx.createNode( label1 );
                node1.setProperty( prop1, val1 );
                nodes1.add( node1.getId() );
                Relationship rel1 = node1.createRelationshipTo( node1, relType1 );
                rel1.setProperty( prop1, val1 );
                rels1.add( rel1.getId() );

                Node node2 = tx.createNode( label2 );
                node2.setProperty( prop2, val2 );
                nodes2.add( node2.getId() );
                Relationship rel2 = node1.createRelationshipTo( node2, relType2 );
                rel2.setProperty( prop2, val2 );
                rels2.add( rel2.getId() );

                Node node3 = tx.createNode( label3 );
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
            tx.execute( format( NODE_CREATE, "nodeIndex",
                    asStrList( label1.name(), label2.name(), label3.name() ),
                    asStrList( prop1, prop2, prop3 ) ) ).close();
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
            tx.commit();
        }

        assertQueryFindsIds( db, true, "nodeIndex", "foo", nodes1 );
        assertQueryFindsIds( db, true, "nodeIndex", "bar", nodes2 );
        assertQueryFindsIds( db, true, "nodeIndex", "baz", nodes3 );

        // Test that multi-token relationship indexes can be waited for.
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( RELATIONSHIP_CREATE, "relIndex",
                    asStrList( relType1.name(), relType2.name(), relType3.name() ),
                    asStrList( prop1, prop2, prop3 ) ) ).close();
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
            tx.commit();
        }

        assertQueryFindsIds( db, false, "relIndex", "foo", rels1 );
        assertQueryFindsIds( db, false, "relIndex", "bar", rels2 );
        assertQueryFindsIds( db, false, "relIndex", "baz", rels3 );
    }

    @Test
    void queryingWithIndexProgressorMustProvideScore() throws Exception
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
                        IndexQuery[] query, IndexQueryConstraints constraints,
                        boolean indexIncludesTransactionState )
                {
                    this.progressor = progressor;
                }

                @Override
                public boolean acceptEntity( long reference, float score, Value... values )
                {
                    this.nodeReference = reference;
                    assertFalse( Float.isNaN( score ), "score should not be NaN" );
                    assertThat( score ).as( "score must be positive" ).isGreaterThan( 0.0f );
                    acceptedEntities.add( "reference = " + reference + ", score = " + score + ", " + Arrays.toString( values ) );
                    return true;
                }
            };
            Read read = ktx.dataRead();
            IndexReadSession indexSession = ktx.dataRead().indexReadSession( index );
            read.nodeIndexSeek( indexSession, cursor, unconstrained(), fulltextSearch( "hej:\"villa\"" ) );
            int counter = 0;
            while ( cursor.next() )
            {
                assertThat( cursor.nodeReference() ).isEqualTo( nodeId );
                counter++;
            }
            assertThat( counter ).isEqualTo( 1 );
            assertThat( acceptedEntities.size() ).isEqualTo( 1 );
            acceptedEntities.clear();
        }
    }

    @Test
    void validateMustThrowIfSchemaIsNotFulltext() throws Exception
    {
        try ( KernelTransactionImplementation transaction = getKernelTransaction() )
        {
            int[] propertyIds = {propIdHa};
            SchemaDescriptor schema = SchemaDescriptor.forLabel( labelIdHa, propertyIds );
            IndexPrototype prototype = IndexPrototype.forSchema( schema ).withIndexType( FULLTEXT ).withName( NAME );
            SchemaWrite schemaWrite = transaction.schemaWrite();
            var e = assertThrows( IllegalArgumentException.class, () -> schemaWrite.indexCreate( prototype ) );
            assertThat( e.getMessage() ).contains( "schema is not a full-text index schema" );
            transaction.success();
        }
    }

    @Test
    void indexWithUnknownAnalyzerWillBeMarkedAsFailedOnStartup() throws Exception
    {
        // Create a full-text index.
        long indexId;
        try ( KernelTransactionImplementation transaction = getKernelTransaction() )
        {
            int[] propertyIds = {propIdHa};
            SchemaDescriptor schema = SchemaDescriptor.fulltext( EntityType.NODE, new int[]{labelIdHa}, propertyIds );
            IndexPrototype prototype = IndexPrototype.forSchema( schema ).withIndexType( FULLTEXT ).withName( NAME );
            SchemaWrite schemaWrite = transaction.schemaWrite();
            IndexDescriptor index = schemaWrite.indexCreate( prototype );
            indexId = index.getId();
            transaction.success();
        }

        // Modify the full-text index such that it has an analyzer configured that does not exist.
        controller.restartDbms( builder ->
        {
            var cacheTracer = NULL;
            FileSystemAbstraction fs = builder.getFileSystem();
            DatabaseLayout databaseLayout = Neo4jLayout.of( builder.getHomeDirectory() ).databaseLayout( DEFAULT_DATABASE_NAME );
            DefaultIdGeneratorFactory idGenFactory = new DefaultIdGeneratorFactory( fs, RecoveryCleanupWorkCollector.ignore() );
            try ( JobScheduler scheduler = JobSchedulerFactory.createInitialisedScheduler();
                  PageCache pageCache = StandalonePageCacheFactory.createPageCache( fs, scheduler, cacheTracer ) )
            {

                StoreFactory factory =
                        new StoreFactory( databaseLayout, Config.defaults(), idGenFactory, pageCache, fs, NullLogProvider.getInstance(), cacheTracer );
                var cursorTracer = PageCursorTracer.NULL;
                try ( NeoStores neoStores = factory.openAllNeoStores( false ) )
                {
                    TokenHolders tokens = StoreTokens.readOnlyTokenHolders( neoStores, PageCursorTracer.NULL );
                    SchemaStore schemaStore = neoStores.getSchemaStore();
                    SchemaStorage storage = new SchemaStorage( schemaStore, tokens );
                    IndexDescriptor index = (IndexDescriptor) storage.loadSingleSchemaRule( indexId, PageCursorTracer.NULL );
                    Map<String,Value> indexConfigMap = new HashMap<>( index.getIndexConfig().asMap() );
                    for ( Map.Entry<String,Value> entry : indexConfigMap.entrySet() )
                    {
                        if ( entry.getKey().contains( "analyzer" ) )
                        {
                            entry.setValue( Values.stringValue( "bla-bla-lyzer" ) ); // This analyzer does not exist!
                        }
                    }
                    index = index.withIndexConfig( IndexConfig.with( indexConfigMap ) );
                    storage.writeSchemaRule( index, cursorTracer, INSTANCE );
                    schemaStore.flush( cursorTracer );
                }
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
            return builder;
        } );

        // Verify that the index comes up in a failed state.
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = tx.schema().getIndexByName( NAME );

            Schema.IndexState indexState = tx.schema().getIndexState( index );
            assertThat( indexState ).isEqualTo( Schema.IndexState.FAILED );

            String indexFailure = tx.schema().getIndexFailure( index );
            assertThat( indexFailure ).contains( "bla-bla-lyzer" );
        }

        // Verify that the failed index can be dropped.
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().getIndexByName( NAME ).drop();
            assertThrows( IllegalArgumentException.class, () -> tx.schema().getIndexByName( NAME ) );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            assertThrows( IllegalArgumentException.class, () -> tx.schema().getIndexByName( NAME ) );
        }
        controller.restartDbms();
        try ( Transaction tx = db.beginTx() )
        {
            assertThrows( IllegalArgumentException.class, () -> tx.schema().getIndexByName( NAME ) );
        }
    }

    @ResourceLock( "BrokenAnalyzerProvider" )
    @Test
    void indexWithAnalyzerThatThrowsWillNotBeCreated()
    {
        BrokenAnalyzerProvider.shouldThrow = true;
        BrokenAnalyzerProvider.shouldReturnNull = false;
        try ( Transaction tx = db.beginTx() )
        {
            IndexCreator creator = tx.schema().indexFor( label( "Label" ) )
                    .withIndexType( org.neo4j.graphdb.schema.IndexType.FULLTEXT )
                    .withIndexConfiguration( Map.of( IndexSetting.fulltext_Analyzer(), BrokenAnalyzerProvider.NAME ) )
                    .on( "prop" )
                    .withName( NAME );

            // Validation must initially prevent this index from being created.
            var e = assertThrows( RuntimeException.class, creator::create );
            assertThat( e.getMessage() ).contains( "boom" );

            // Create the index anyway.
            BrokenAnalyzerProvider.shouldThrow = false;
            creator.create();
            BrokenAnalyzerProvider.shouldThrow = true;

            // The analyzer will now throw during the index population, and the index should then enter a FAILED state.
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            var e = assertThrows( IllegalStateException.class, () -> tx.schema().awaitIndexOnline( NAME, 10, TimeUnit.SECONDS ) );
            assertThat( e.getMessage() ).contains( "FAILED" );
            IndexDefinition index = tx.schema().getIndexByName( NAME );
            assertThat( tx.schema().getIndexState( index ) ).isEqualTo( Schema.IndexState.FAILED );
            index.drop();
            tx.commit();
        }

        BrokenAnalyzerProvider.shouldThrow = false;
        try ( Transaction tx = db.beginTx() )
        {
            IndexCreator creator = tx.schema().indexFor( label( "Label" ) )
                    .withIndexType( org.neo4j.graphdb.schema.IndexType.FULLTEXT )
                    .withIndexConfiguration( Map.of( IndexSetting.fulltext_Analyzer(), BrokenAnalyzerProvider.NAME ) )
                    .on( "prop" )
                    .withName( NAME );

            // The analyzer no longer throws.
            creator.create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexOnline( NAME, 10, TimeUnit.SECONDS );
            IndexDefinition index = tx.schema().getIndexByName( NAME );
            Schema.IndexState indexState = tx.schema().getIndexState( index );
            assertThat( indexState ).isEqualTo( Schema.IndexState.ONLINE );
        }
        controller.restartDbms();
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = tx.schema().getIndexByName( NAME );
            Schema.IndexState indexState = tx.schema().getIndexState( index );
            assertThat( indexState ).isEqualTo( Schema.IndexState.ONLINE );
        }
    }

    @ResourceLock( "BrokenAnalyzerProvider" )
    @Test
    void indexWithAnalyzerThatReturnsNullWillNotBeCreated()
    {
        BrokenAnalyzerProvider.shouldThrow = false;
        BrokenAnalyzerProvider.shouldReturnNull = true;
        try ( Transaction tx = db.beginTx() )
        {
            IndexCreator creator = tx.schema().indexFor( label( "Label" ) )
                    .withIndexType( org.neo4j.graphdb.schema.IndexType.FULLTEXT )
                    .withIndexConfiguration( Map.of( IndexSetting.fulltext_Analyzer(), BrokenAnalyzerProvider.NAME ) )
                    .on( "prop" )
                    .withName( NAME );

            // Validation must initially prevent this index from being created.
            var e = assertThrows( RuntimeException.class, creator::create );
            assertThat( e.getMessage() ).contains( "null" );

            // Create the index anyway.
            BrokenAnalyzerProvider.shouldReturnNull = false;
            creator.create();
            BrokenAnalyzerProvider.shouldReturnNull = true;

            // The analyzer will now return null during the index population, and the index should then enter a FAILED state.
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            var e = assertThrows( IllegalStateException.class, () -> tx.schema().awaitIndexOnline( NAME, 10, TimeUnit.SECONDS ) );
            assertThat( e.getMessage() ).contains( "FAILED" );
            IndexDefinition index = tx.schema().getIndexByName( NAME );
            assertThat( tx.schema().getIndexState( index ) ).isEqualTo( Schema.IndexState.FAILED );
            index.drop();
            tx.commit();
        }

        BrokenAnalyzerProvider.shouldReturnNull = false;
        try ( Transaction tx = db.beginTx() )
        {
            IndexCreator creator = tx.schema().indexFor( label( "Label" ) )
                    .withIndexType( org.neo4j.graphdb.schema.IndexType.FULLTEXT )
                    .withIndexConfiguration( Map.of( IndexSetting.fulltext_Analyzer(), BrokenAnalyzerProvider.NAME ) )
                    .on( "prop" )
                    .withName( NAME );

            // The analyzer no longer returns null.
            creator.create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexOnline( NAME, 10, TimeUnit.SECONDS );
            IndexDefinition index = tx.schema().getIndexByName( NAME );
            Schema.IndexState indexState = tx.schema().getIndexState( index );
            assertThat( indexState ).isEqualTo( Schema.IndexState.ONLINE );
        }
        controller.restartDbms();
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = tx.schema().getIndexByName( NAME );
            Schema.IndexState indexState = tx.schema().getIndexState( index );
            assertThat( indexState ).isEqualTo( Schema.IndexState.ONLINE );
        }
    }

    @ResourceLock( "BrokenAnalyzerProvider" )
    @Test
    void indexWithAnalyzerProviderThatThrowsAnExceptionOnStartupWillBeMarkedAsFailedOnStartup()
    {
        BrokenAnalyzerProvider.shouldThrow = false;
        BrokenAnalyzerProvider.shouldReturnNull = false;
        try ( Transaction tx = db.beginTx() )
        {
            IndexCreator creator = tx.schema().indexFor( label( "Label" ) )
                    .withIndexType( org.neo4j.graphdb.schema.IndexType.FULLTEXT )
                    .withIndexConfiguration( Map.of( IndexSetting.fulltext_Analyzer(), BrokenAnalyzerProvider.NAME ) )
                    .on( "prop" )
                    .withName( NAME );

            // The analyzer no longer throws.
            creator.create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexOnline( NAME, 10, TimeUnit.SECONDS );
            IndexDefinition index = tx.schema().getIndexByName( NAME );
            Schema.IndexState indexState = tx.schema().getIndexState( index );
            assertThat( indexState ).isEqualTo( Schema.IndexState.ONLINE );
        }

        BrokenAnalyzerProvider.shouldThrow = true;
        controller.restartDbms();
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = tx.schema().getIndexByName( NAME );
            Schema.IndexState indexState = tx.schema().getIndexState( index );
            assertThat( indexState ).isEqualTo( Schema.IndexState.FAILED );
            String indexFailure = tx.schema().getIndexFailure( index );
            assertThat( indexFailure ).contains( "boom" );
            index.drop();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            assertThrows( IllegalArgumentException.class, () -> tx.schema().getIndexByName( NAME ) );
            tx.commit();
        }
    }

    @ResourceLock( "BrokenAnalyzerProvider" )
    @Test
    void indexWithAnalyzerProviderThatReturnsNullWillBeMarkedAsFailedOnStartup()
    {
        BrokenAnalyzerProvider.shouldThrow = false;
        BrokenAnalyzerProvider.shouldReturnNull = false;
        try ( Transaction tx = db.beginTx() )
        {
            IndexCreator creator = tx.schema().indexFor( label( "Label" ) )
                    .withIndexType( org.neo4j.graphdb.schema.IndexType.FULLTEXT )
                    .withIndexConfiguration( Map.of( IndexSetting.fulltext_Analyzer(), BrokenAnalyzerProvider.NAME ) )
                    .on( "prop" )
                    .withName( NAME );

            // The analyzer no longer returns null.
            creator.create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexOnline( NAME, 10, TimeUnit.SECONDS );
            IndexDefinition index = tx.schema().getIndexByName( NAME );
            Schema.IndexState indexState = tx.schema().getIndexState( index );
            assertThat( indexState ).isEqualTo( Schema.IndexState.ONLINE );
        }

        BrokenAnalyzerProvider.shouldReturnNull = true;
        controller.restartDbms();
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = tx.schema().getIndexByName( NAME );
            Schema.IndexState indexState = tx.schema().getIndexState( index );
            assertThat( indexState ).isEqualTo( Schema.IndexState.FAILED );
            String indexFailure = tx.schema().getIndexFailure( index );
            assertThat( indexFailure ).contains( "null" );
            index.drop();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            assertThrows( IllegalArgumentException.class, () -> tx.schema().getIndexByName( NAME ) );
            tx.commit();
        }
    }

    @Test
    void shouldAnswerContainsIfCypherCompatible() throws KernelException
    {
        IndexDescriptor indexReference;
        Label containsLabel = label( "containsLabel" );
        String containsProp = "containsProp";
        long nodea;
        long nodeapa1;
        long nodeapa2;
        long nodeapalong;
        long nodeapaapa;
        try ( Transaction tx = db.beginTx() )
        {
            nodea = createNode( tx, containsLabel, containsProp, "a" );
            createNode( tx, containsLabel, containsProp, "A" );
            nodeapa1 = createNode( tx, containsLabel, containsProp, "apa" );
            nodeapa2 = createNode( tx, containsLabel, containsProp, "apa" );
            nodeapalong = createNode( tx, containsLabel, containsProp, "apalong" );
            nodeapaapa = createNode( tx, containsLabel, containsProp, "apa apa" );
            tx.commit();
        }

        int containsLabelId;
        int containsPropertyId;
        try ( Transaction tx = db.beginTx() )
        {
            TokenRead tokenRead = tokenRead( tx );
            containsLabelId = tokenRead.nodeLabel( containsLabel.name() );
            containsPropertyId = tokenRead.propertyKey( containsProp );
        }

        indexReference = createIndex( new int[]{containsLabelId}, new int[]{containsPropertyId}, "cypher" );
        await( indexReference );

        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = LuceneFulltextTestSupport.kernelTransaction( tx );
            assertQueryResult( ktx, containsQuery( containsPropertyId, "a" ), nodea, nodeapa1, nodeapa2, nodeapalong, nodeapaapa );
            assertQueryResult( ktx, containsQuery( containsPropertyId, "apa" ), nodeapa1, nodeapa2, nodeapalong, nodeapaapa );
            assertQueryResult( ktx, containsQuery( containsPropertyId, "apa*" ) );
            assertQueryResult( ktx, containsQuery( containsPropertyId, "pa ap" ), nodeapaapa );
        }
        controller.restartDbms();
        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = LuceneFulltextTestSupport.kernelTransaction( tx );
            assertQueryResult( ktx, containsQuery( containsPropertyId, "a" ), nodea, nodeapa1, nodeapa2, nodeapalong, nodeapaapa );
            assertQueryResult( ktx, containsQuery( containsPropertyId, "apa" ), nodeapa1, nodeapa2, nodeapalong, nodeapaapa );
            assertQueryResult( ktx, containsQuery( containsPropertyId, "apa*" ) );
            assertQueryResult( ktx, containsQuery( containsPropertyId, "pa ap" ), nodeapaapa );
        }
    }

    @Test
    void shouldAnswerEndsWithIfCypherCompatible() throws KernelException
    {
        IndexDescriptor indexReference;
        Label containsLabel = label( "containsLabel" );
        String containsProp = "containsProp";
        long nodea;
        long nodeapa1;
        long nodeapa2;
        long nodelongapa;
        long nodeapaapa;
        try ( Transaction tx = db.beginTx() )
        {
            nodea = createNode( tx, containsLabel, containsProp, "a" );
            createNode( tx, containsLabel, containsProp, "A" );
            nodeapa1 = createNode( tx, containsLabel, containsProp, "apa" );
            nodeapa2 = createNode( tx, containsLabel, containsProp, "apa" );
            nodelongapa = createNode( tx, containsLabel, containsProp, "longapa" );
            nodeapaapa = createNode( tx, containsLabel, containsProp, "apa apa" );
            createNode( tx, containsLabel, containsProp, "apalong" );
            tx.commit();
        }

        int containsLabelId;
        int containsPropertyId;
        try ( Transaction tx = db.beginTx() )
        {
            TokenRead tokenRead = tokenRead( tx );
            containsLabelId = tokenRead.nodeLabel( containsLabel.name() );
            containsPropertyId = tokenRead.propertyKey( containsProp );
        }

        indexReference = createIndex( new int[]{containsLabelId}, new int[]{containsPropertyId}, "cypher" );
        await( indexReference );

        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = LuceneFulltextTestSupport.kernelTransaction( tx );
            assertQueryResult( ktx, endsWithQuery( containsPropertyId, "a" ), nodea, nodeapa1, nodeapa2, nodelongapa, nodeapaapa );
            assertQueryResult( ktx, endsWithQuery( containsPropertyId, "apa" ), nodeapa1, nodeapa2, nodelongapa, nodeapaapa );
            assertQueryResult( ktx, endsWithQuery( containsPropertyId, "apa*" ) );
            assertQueryResult( ktx, endsWithQuery( containsPropertyId, "a apa" ), nodeapaapa );
            assertQueryResult( ktx, endsWithQuery( containsPropertyId, "*apa" ) );
        }
        controller.restartDbms();
        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = LuceneFulltextTestSupport.kernelTransaction( tx );
            assertQueryResult( ktx, endsWithQuery( containsPropertyId, "a" ), nodea, nodeapa1, nodeapa2, nodelongapa, nodeapaapa );
            assertQueryResult( ktx, endsWithQuery( containsPropertyId, "apa" ), nodeapa1, nodeapa2, nodelongapa, nodeapaapa );
            assertQueryResult( ktx, endsWithQuery( containsPropertyId, "apa*" ) );
            assertQueryResult( ktx, endsWithQuery( containsPropertyId, "a apa" ), nodeapaapa );
            assertQueryResult( ktx, endsWithQuery( containsPropertyId, "*apa" ) );
        }
    }

    @Test
    void shouldAnswerStartsWithIfCypherCompatible() throws KernelException
    {
        IndexDescriptor indexReference;
        Label containsLabel = label( "containsLabel" );
        String containsProp = "containsProp";
        long nodea;
        long nodeapa1;
        long nodeapa2;
        long nodeapaapa;
        long nodeapalong;
        try ( Transaction tx = db.beginTx() )
        {
            nodea = createNode( tx, containsLabel, containsProp, "a" );
            createNode( tx, containsLabel, containsProp, "A" );
            nodeapa1 = createNode( tx, containsLabel, containsProp, "apa" );
            nodeapa2 = createNode( tx, containsLabel, containsProp, "apa" );
            createNode( tx, containsLabel, containsProp, "longapa" );
            nodeapaapa = createNode( tx, containsLabel, containsProp, "apa apa" );
            nodeapalong = createNode( tx, containsLabel, containsProp, "apalong" );
            tx.commit();
        }

        int containsLabelId;
        int containsPropertyId;
        try ( Transaction tx = db.beginTx() )
        {
            TokenRead tokenRead = tokenRead( tx );
            containsLabelId = tokenRead.nodeLabel( containsLabel.name() );
            containsPropertyId = tokenRead.propertyKey( containsProp );
        }

        indexReference = createIndex( new int[]{containsLabelId}, new int[]{containsPropertyId}, "cypher" );
        await( indexReference );

        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = LuceneFulltextTestSupport.kernelTransaction( tx );
            assertQueryResult( ktx, startsWithQuery( containsPropertyId, "a" ), nodea, nodeapa1, nodeapa2, nodeapaapa, nodeapalong );
            assertQueryResult( ktx, startsWithQuery( containsPropertyId, "apa" ), nodeapa1, nodeapa2, nodeapalong, nodeapaapa );
            assertQueryResult( ktx, startsWithQuery( containsPropertyId, "apa*" ) );
            assertQueryResult( ktx, startsWithQuery( containsPropertyId, "apa a" ), nodeapaapa );
            assertQueryResult( ktx, startsWithQuery( containsPropertyId, "*apa" ) );
        }
        controller.restartDbms();
        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = LuceneFulltextTestSupport.kernelTransaction( tx );
            assertQueryResult( ktx, startsWithQuery( containsPropertyId, "a" ), nodea, nodeapa1, nodeapa2, nodeapaapa, nodeapalong );
            assertQueryResult( ktx, startsWithQuery( containsPropertyId, "apa" ), nodeapa1, nodeapa2, nodeapalong, nodeapaapa );
            assertQueryResult( ktx, startsWithQuery( containsPropertyId, "apa*" ) );
            assertQueryResult( ktx, startsWithQuery( containsPropertyId, "apa a" ), nodeapaapa );
            assertQueryResult( ktx, startsWithQuery( containsPropertyId, "*apa" ) );
        }
    }

    @Test
    void shouldAnswerExactIfCypherCompatible() throws KernelException
    {
        IndexDescriptor indexReference;
        Label containsLabel = label( "containsLabel" );
        String containsProp = "containsProp";
        long nodea;
        long nodeapa1;
        long nodeapa2;
        long nodeapaapa;
        try ( Transaction tx = db.beginTx() )
        {
            nodea = createNode( tx, containsLabel, containsProp, "a" );
            createNode( tx, containsLabel, containsProp, "A" );
            nodeapa1 = createNode( tx, containsLabel, containsProp, "apa" );
            nodeapa2 = createNode( tx, containsLabel, containsProp, "apa" );
            createNode( tx, containsLabel, containsProp, "longapa" );
            nodeapaapa = createNode( tx, containsLabel, containsProp, "apa apa" );
            createNode( tx, containsLabel, containsProp, "apalong" );
            tx.commit();
        }

        int containsLabelId;
        int containsPropertyId;
        try ( Transaction tx = db.beginTx() )
        {
            TokenRead tokenRead = tokenRead( tx );
            containsLabelId = tokenRead.nodeLabel( containsLabel.name() );
            containsPropertyId = tokenRead.propertyKey( containsProp );
        }

        indexReference = createIndex( new int[]{containsLabelId}, new int[]{containsPropertyId}, "cypher" );
        await( indexReference );

        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = LuceneFulltextTestSupport.kernelTransaction( tx );
            assertQueryResult( ktx, exactQuery( containsPropertyId, "a" ), nodea );
            assertQueryResult( ktx, exactQuery( containsPropertyId, "apa" ), nodeapa1, nodeapa2 );
            assertQueryResult( ktx, exactQuery( containsPropertyId, "apa*" ) );
            assertQueryResult( ktx, exactQuery( containsPropertyId, "apa a" ) );
            assertQueryResult( ktx, exactQuery( containsPropertyId, "apa apa" ), nodeapaapa );
            assertQueryResult( ktx, exactQuery( containsPropertyId, "*apa" ) );
            assertQueryResult( ktx, exactQuery( containsPropertyId, "apa*" ) );
            IndexNotApplicableKernelException e =
                    assertThrows( IndexNotApplicableKernelException.class, () -> assertQueryResult( ktx, exactQuery( containsPropertyId, 1 ) ) );
            assertThat( e.getMessage() ).contains(
                    "A fulltext schema index cannot answer " + IndexQuery.IndexQueryType.exact + " queries on " + ValueCategory.NUMBER + " values." );
        }
        controller.restartDbms();
        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = LuceneFulltextTestSupport.kernelTransaction( tx );
            assertQueryResult( ktx, exactQuery( containsPropertyId, "a" ), nodea );
            assertQueryResult( ktx, exactQuery( containsPropertyId, "apa" ), nodeapa1, nodeapa2 );
            assertQueryResult( ktx, exactQuery( containsPropertyId, "apa*" ) );
            assertQueryResult( ktx, exactQuery( containsPropertyId, "apa a" ) );
            assertQueryResult( ktx, exactQuery( containsPropertyId, "apa apa" ), nodeapaapa );
            assertQueryResult( ktx, exactQuery( containsPropertyId, "*apa" ) );
            assertQueryResult( ktx, exactQuery( containsPropertyId, "apa*" ) );
            IndexNotApplicableKernelException e =
                    assertThrows( IndexNotApplicableKernelException.class, () -> assertQueryResult( ktx, exactQuery( containsPropertyId, 1 ) ) );
            assertThat( e.getMessage() ).contains(
                    "A fulltext schema index cannot answer " + IndexQuery.IndexQueryType.exact + " queries on " + ValueCategory.NUMBER + " values." );
        }
    }

    @Test
    void shouldAnswerRangeIfCypherCompatible() throws KernelException
    {
        IndexDescriptor indexReference;
        Label containsLabel = label( "containsLabel" );
        String containsProp = "containsProp";
        long nodea;
        long nodeaa;
        long nodeaapa;
        long nodeapa1;
        long nodeapa2;
        try ( Transaction tx = db.beginTx() )
        {
            createNode( tx, containsLabel, containsProp, "1" );
            nodea = createNode( tx, containsLabel, containsProp, "a" );
            nodeaa = createNode( tx, containsLabel, containsProp, "aa" );
            nodeaapa = createNode( tx, containsLabel, containsProp, "aapa" );
            nodeapa1 = createNode( tx, containsLabel, containsProp, "apa" );
            nodeapa2 = createNode( tx, containsLabel, containsProp, "apa" );
            createNode( tx, containsLabel, containsProp, "bpa" );
            createNode( tx, containsLabel, containsProp, "A" );
            tx.commit();
        }

        int containsLabelId;
        int containsPropertyId;
        try ( Transaction tx = db.beginTx() )
        {
            TokenRead tokenRead = tokenRead( tx );
            containsLabelId = tokenRead.nodeLabel( containsLabel.name() );
            containsPropertyId = tokenRead.propertyKey( containsProp );
        }

        indexReference = createIndex( new int[]{containsLabelId}, new int[]{containsPropertyId}, "cypher" );
        await( indexReference );

        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = LuceneFulltextTestSupport.kernelTransaction( tx );
            assertQueryResult( ktx, rangeQuery( containsPropertyId, "a", true, "apa", true ), nodea, nodeaa, nodeaapa, nodeapa1, nodeapa2 );
            assertQueryResult( ktx, rangeQuery( containsPropertyId, "a", false, "apa", true ), nodeaa, nodeaapa, nodeapa1, nodeapa2 );
            assertQueryResult( ktx, rangeQuery( containsPropertyId, "a", true, "apa", false ), nodea, nodeaa, nodeaapa );
            assertQueryResult( ktx, rangeQuery( containsPropertyId, "a", false, "apa", false ), nodeaa, nodeaapa );
        }
        controller.restartDbms();
        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = LuceneFulltextTestSupport.kernelTransaction( tx );
            assertQueryResult( ktx, rangeQuery( containsPropertyId, "a", true, "apa", true ), nodea, nodeaa, nodeaapa, nodeapa1, nodeapa2 );
            assertQueryResult( ktx, rangeQuery( containsPropertyId, "a", false, "apa", true ), nodeaa, nodeaapa, nodeapa1, nodeapa2 );
            assertQueryResult( ktx, rangeQuery( containsPropertyId, "a", true, "apa", false ), nodea, nodeaa, nodeaapa );
            assertQueryResult( ktx, rangeQuery( containsPropertyId, "a", false, "apa", false ), nodeaa, nodeaapa );
        }
    }

    @Test
    void shouldThrowOnCypherFulltextQueryIfNotCypherCompatibleAnalyzer() throws KernelException
    {
        IndexDescriptor indexReference = createIndex( new int[]{1}, new int[]{1}, "english" );
        await( indexReference );
        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = LuceneFulltextTestSupport.kernelTransaction( tx );
            IllegalStateException e = assertThrows( IllegalStateException.class, () -> assertQueryResult( ktx, endsWithQuery( 1, "a" ) ) );
            assertThat( e.getMessage() ).contains(
                    "This fulltext index does not have support for Cypher semantics because configured analyzer 'english' is not Cypher compatible." );
        }
    }

    @Test
    void shouldThrowOnCypherFulltextQueryIfNotCypherCompatibleComposite() throws KernelException
    {
        IndexDescriptor indexReference = createIndex( new int[]{1}, new int[]{1, 2}, "cypher" );
        await( indexReference );
        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = LuceneFulltextTestSupport.kernelTransaction( tx );
            IllegalStateException e = assertThrows( IllegalStateException.class, () -> assertQueryResult( ktx, endsWithQuery( 1, "a" ) ) );
            assertThat( e.getMessage() )
                    .contains( "This fulltext index does not have support for Cypher semantics because index is composite." );
        }
    }

    @Test
    void shouldThrowOnCypherFulltextQueryIfNotCypherCompatibleMultipleEntities() throws KernelException
    {
        IndexDescriptor indexReference = createIndex( new int[]{1, 2}, new int[]{1}, "cypher" );
        await( indexReference );
        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = LuceneFulltextTestSupport.kernelTransaction( tx );
            IllegalStateException e = assertThrows( IllegalStateException.class, () -> assertQueryResult( ktx, endsWithQuery( 1, "a" ) ) );
            assertThat( e.getMessage() )
                    .contains( "This fulltext index does not have support for Cypher semantics because index target more than one label." );
        }
    }

    @Test
    void shouldThrowOnCypherFulltextQueryIfNotCypherCompatibleNotNodeIndex() throws KernelException
    {
        IndexDescriptor indexReference = createIndex( new int[]{1}, new int[]{1}, "cypher", EntityType.RELATIONSHIP );
        await( indexReference );
        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = LuceneFulltextTestSupport.kernelTransaction( tx );
            IndexNotApplicableKernelException e =
                    assertThrows( IndexNotApplicableKernelException.class, () -> assertQueryResult( ktx, endsWithQuery( 1, "a" ) ) );
            assertThat( e.getMessage() ).contains( "Node index seek can only be performed on node indexes" );
        }
    }

    @Test
    void shouldThrowOnCypherFulltextQueryIfNotCypherCompatibleEventuallyConsistent() throws KernelException
    {
        IndexDescriptor indexReference = createIndex( new int[]{1}, new int[]{1}, "cypher", EntityType.NODE, true );
        await( indexReference );
        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = LuceneFulltextTestSupport.kernelTransaction( tx );
            IllegalStateException e = assertThrows( IllegalStateException.class, () -> assertQueryResult( ktx, endsWithQuery( 1, "a" ) ) );
            assertThat( e.getMessage() )
                    .contains( "This fulltext index does not have support for Cypher semantics because index is eventually consistent." );
        }
    }

    private void assertQueryResult( KernelTransaction ktx, IndexQuery query, Long... expectedResultArray ) throws KernelException
    {
        List<Long> expectedResult = Arrays.asList( expectedResultArray );
        IndexReadSession index = ktx.dataRead().indexReadSession( ktx.schemaRead().indexGetForName( NAME ) );
        try ( NodeValueIndexCursor cursor = ktx.cursors().allocateNodeValueIndexCursor( ktx.pageCursorTracer() ) )
        {
            List<Long> actualResult = new ArrayList<>();
            ktx.dataRead().nodeIndexSeek( index, cursor, unconstrained(), query );
            while ( cursor.next() )
            {
                actualResult.add( cursor.nodeReference() );
            }
            actualResult.sort( Long::compareTo );
            expectedResult.sort( Long::compareTo );
            assertThat( actualResult ).isEqualTo( expectedResult );
        }
    }

    private IndexQuery.StringContainsPredicate containsQuery( int containsPropertyId, String string )
    {
        return IndexQuery.stringContains( containsPropertyId, Values.stringValue( string ) );
    }

    private IndexQuery.StringSuffixPredicate endsWithQuery( int containsPropertyId, String string )
    {
        return IndexQuery.stringSuffix( containsPropertyId, Values.stringValue( string ) );
    }

    private IndexQuery.StringPrefixPredicate startsWithQuery( int containsPropertyId, String string )
    {
        return IndexQuery.stringPrefix( containsPropertyId, Values.stringValue( string ) );
    }

    private IndexQuery.ExactPredicate exactQuery( int containsPropertyId, Object object )
    {
        return IndexQuery.exact( containsPropertyId, Values.of( object ) );
    }

    private IndexQuery.RangePredicate<?> rangeQuery( int containsPropertyId, Object from, boolean fromInclusive, Object to, boolean toInclusive )
    {
        return IndexQuery.range( containsPropertyId, Values.of( from ), fromInclusive, Values.of( to ), toInclusive );
    }

    private long createNode( Transaction tx, Label containsLabel, String containsProp, String propertyValue )
    {
        Node node = tx.createNode( containsLabel );
        node.setProperty( containsProp, propertyValue );
        return node.getId();
    }

    private TokenRead tokenRead( Transaction tx )
    {
        return ((InternalTransaction) tx).kernelTransaction().tokenRead();
    }

    private KernelTransactionImplementation getKernelTransaction()
    {
        try
        {
            return (KernelTransactionImplementation) kernel.beginTransaction(
                    KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED );
        }
        catch ( TransactionFailureException e )
        {
            throw new RuntimeException( "oops" );
        }
    }

    private IndexDescriptor createIndex( int[] entityTokens, int[] propertyIds ) throws KernelException
    {
        return createIndex( entityTokens, propertyIds, FulltextSettings.fulltext_default_analyzer.defaultValue() );
    }

    private IndexDescriptor createIndex( int[] entityTokens, int[] propertyIds, String analyzer ) throws KernelException
    {
        return createIndex( entityTokens, propertyIds, analyzer, EntityType.NODE );
    }

    private IndexDescriptor createIndex( int[] entityTokens, int[] propertyIds, String analyzer, EntityType entityType ) throws KernelException
    {
        return createIndex( entityTokens, propertyIds, analyzer, entityType, false );
    }

    private IndexDescriptor createIndex( int[] entityTokens, int[] propertyIds, String analyzer, EntityType entityType, boolean eventuallyConsistent )
            throws KernelException
    {
        IndexDescriptor fulltext;
        try ( KernelTransactionImplementation transaction = getKernelTransaction() )
        {
            SchemaDescriptor schema = SchemaDescriptor.fulltext( entityType, entityTokens, propertyIds );
            IndexConfig config = IndexConfig
                    .with( FulltextIndexSettingsKeys.ANALYZER, Values.stringValue( analyzer ) )
                    .withIfAbsent( FulltextIndexSettingsKeys.EVENTUALLY_CONSISTENT, Values.of( eventuallyConsistent ) );
            IndexPrototype prototype = IndexPrototype.forSchema( schema, DESCRIPTOR ).withIndexType( IndexType.FULLTEXT ).withName( NAME )
                    .withIndexConfig( config );
            fulltext = transaction.schemaWrite().indexCreate( prototype );
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
            Node hej = transaction.createNode( label( "hej" ) );
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
            try ( NodeValueIndexCursor cursor = ktx.cursors().allocateNodeValueIndexCursor( ktx.pageCursorTracer() ) )
            {
                ktx.dataRead().nodeIndexSeek( index, cursor, unconstrained(), fulltextSearch( "value" ) );
                assertTrue( cursor.next() );
                assertEquals( 0L, cursor.nodeReference() );
                assertFalse( cursor.next() );

                ktx.dataRead().nodeIndexSeek( index, cursor, unconstrained(), fulltextSearch( "villa" ) );
                assertTrue( cursor.next() );
                assertEquals( thirdNodeId, cursor.nodeReference() );
                assertFalse( cursor.next() );

                ktx.dataRead().nodeIndexSeek( index, cursor, unconstrained(), fulltextSearch( "value3" ) );
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
            try ( RelationshipIndexCursor cursor = ktx.cursors().allocateRelationshipIndexCursor( ktx.pageCursorTracer() ) )
            {
                ktx.dataRead().relationshipIndexSeek( index, cursor, unconstrained(), fulltextSearch( "valuuu" ) );
                assertTrue( cursor.next() );
                assertEquals( 0L, cursor.relationshipReference() );
                assertFalse( cursor.next() );

                ktx.dataRead().relationshipIndexSeek( index, cursor, unconstrained(), fulltextSearch( "villa" ) );
                assertTrue( cursor.next() );
                assertEquals( secondRelId, cursor.relationshipReference() );
                assertFalse( cursor.next() );

                ktx.dataRead().relationshipIndexSeek( index, cursor, unconstrained(), fulltextSearch( "value3" ) );
                assertTrue( cursor.next() );
                assertEquals( 0L, cursor.relationshipReference() );
                assertTrue( cursor.next() );
                assertEquals( secondRelId, cursor.relationshipReference() );
                assertFalse( cursor.next() );
            }
            tx.commit();
        }
    }

    private void await( IndexDescriptor index )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexOnline( index.getName(), 30, TimeUnit.SECONDS );
        }
    }
}
