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
package org.neo4j.consistency.newchecker;

import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.checking.ByteArrayBitsManipulator;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.cache.CacheSlots;
import org.neo4j.consistency.checking.cache.DefaultCacheAccess;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReporter;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.consistency.report.InconsistencyMessageLogger;
import org.neo4j.consistency.report.InconsistencyReport;
import org.neo4j.consistency.statistics.Counts;
import org.neo4j.consistency.store.DirectRecordAccess;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.batchimport.cache.NumberArrayFactory;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.index.label.LabelScanStore;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.recordstorage.SchemaStorage;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.impl.api.index.AbstractDelegatingIndexProxy;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.OnlineIndexProxy;
import org.neo4j.kernel.impl.store.InlineNodeLabels;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLog;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.consistency.newchecker.ParallelExecution.NOOP_EXCEPTION_HANDLER;
import static org.neo4j.consistency.newchecker.RecordStorageConsistencyChecker.DEFAULT_SLOT_SIZES;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;
import static org.neo4j.values.storable.Values.intArray;
import static org.neo4j.values.storable.Values.stringValue;

@PageCacheExtension
class CheckerTestBase
{
    static final int NUMBER_OF_THREADS = 4;
    static final long NULL = NULL_REFERENCE.longValue();

    @Inject
    PageCache pageCache;

    @Inject
    TestDirectory directory;

    MutableIntObjectMap<MutableIntSet> noMandatoryProperties = IntObjectMaps.mutable.empty();
    GraphDatabaseAPI db;
    NeoStores neoStores;
    NodeStore nodeStore;
    PropertyStore propertyStore;
    RelationshipGroupStore relationshipGroupStore;
    RelationshipStore relationshipStore;
    SchemaStore schemaStore;
    LabelScanStore labelIndex;
    ConsistencyReporter reporter;
    ConsistencyReporter.Monitor monitor;
    SchemaStorage schemaStorage;

    private DatabaseManagementService dbms;
    private CheckerContext context;
    private CountsState countsState;
    private CacheAccess cacheAccess;
    private TokenHolders tokenHolders;

    @BeforeEach
    void setUpDb() throws Exception
    {
        dbms = new TestDatabaseManagementServiceBuilder( directory.homeDir() ).build();
        db = (GraphDatabaseAPI) dbms.database( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );

        // Create our tokens
        Kernel kernel = db.getDependencyResolver().resolveDependency( Kernel.class );
        try ( KernelTransaction tx = kernel.beginTransaction( KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED ) )
        {
            initialData( tx );
            tx.commit();
        }

        DependencyResolver dependencies = db.getDependencyResolver();
        neoStores = dependencies.resolveDependency( RecordStorageEngine.class ).testAccessNeoStores();
        nodeStore = neoStores.getNodeStore();
        relationshipGroupStore = neoStores.getRelationshipGroupStore();
        propertyStore = neoStores.getPropertyStore();
        relationshipStore = neoStores.getRelationshipStore();
        schemaStore = neoStores.getSchemaStore();
        tokenHolders = dependencies.resolveDependency( TokenHolders.class );
        schemaStorage = new SchemaStorage( schemaStore, tokenHolders );
        labelIndex = dependencies.resolveDependency( LabelScanStore.class );
        cacheAccess = new DefaultCacheAccess( NumberArrayFactory.HEAP.newDynamicByteArray( 10_000, new byte[ByteArrayBitsManipulator.MAX_BYTES] ),
                Counts.NONE, NUMBER_OF_THREADS );
        cacheAccess.setCacheSlotSizes( DEFAULT_SLOT_SIZES );
    }

    @AfterEach
    void tearDownDb()
    {
        countsState.close();
        dbms.shutdown();
    }

    void initialData( KernelTransaction tx ) throws KernelException
    {
    }

    CheckerContext context() throws Exception
    {
        return context( NUMBER_OF_THREADS );
    }

    CheckerContext context( int numberOfThreads ) throws Exception
    {
        if ( context != null )
        {
            return context;
        }

        // We do this as late as possible because of how it eagerly caches which indexes exist so if the test creates an index
        // this lazy instantiation allows the context to pick it up
        Config config = Config.defaults( neo4j_home, directory.homeDir().toPath() );
        DependencyResolver dependencies = db.getDependencyResolver();
        IndexProviderMap indexProviders = dependencies.resolveDependency( IndexProviderMap.class );
        IndexingService indexingService = dependencies.resolveDependency( IndexingService.class );
        IndexAccessors indexAccessors = new IndexAccessors( indexProviders, neoStores, new IndexSamplingConfig( config ),
                new LookupAccessorsFromRunningDb( indexingService ) );
        ConsistencySummaryStatistics inconsistenciesSummary = new ConsistencySummaryStatistics();
        InconsistencyReport report = new InconsistencyReport( new InconsistencyMessageLogger( NullLog.getInstance() ), inconsistenciesSummary );
        monitor = mock( ConsistencyReporter.Monitor.class );
        reporter = new ConsistencyReporter( new DirectRecordAccess( new StoreAccess( neoStores ), cacheAccess ), report, monitor );
        countsState = new CountsState( neoStores, cacheAccess );
        NodeBasedMemoryLimiter limiter = new NodeBasedMemoryLimiter( pageCache.pageSize() * pageCache.maxCachedPages(),
                Runtime.getRuntime().maxMemory(), Long.MAX_VALUE, CacheSlots.CACHE_LINE_SIZE_BYTES, nodeStore.getHighId() );
        ProgressMonitorFactory.MultiPartBuilder progress = ProgressMonitorFactory.NONE.multipleParts( "Test" );
        ParallelExecution execution = new ParallelExecution( numberOfThreads, NOOP_EXCEPTION_HANDLER, 100 );
        context = new CheckerContext( neoStores, indexAccessors, labelIndex, execution, reporter, cacheAccess, tokenHolders, new RecordLoading( neoStores ),
                countsState, limiter, progress, pageCache, false, ConsistencyFlags.DEFAULT );
        context.initialize();
        return context;
    }

    static Value stringValueOfLength( int length )
    {
        char[] chars = new char[length];
        for ( int i = 0; i < length; i++ )
        {
            chars[i] = (char) ('a' + (i % 10));
        }
        return stringValue( String.valueOf( chars ) );
    }

    static Value intArrayValueOfLength( int length )
    {
        int[] array = new int[length];
        for ( int i = 0; i < length; i++ )
        {
            array[i] = Integer.MAX_VALUE - i;
        }
        return intArray( array );
    }

    static Value stringArrayValueOfLength( int stringLength, int arrayLength )
    {
        String[] array = new String[arrayLength];
        for ( int i = 0; i < arrayLength; i++ )
        {
            char c = (char) ('a' + i % 20);
            array[i] = String.valueOf( c ).repeat( stringLength );
        }
        return Values.stringArray( array );
    }

    /**
     * Magic for extracting a ConsistencyReport method name compile-time safe.
     */
    <T extends ConsistencyReport> void expect( Class<T> cls, Consumer<T> methodCall )
    {
        methodCall.accept( mock( cls, invocation ->
        {
            expect( cls, invocation.getMethod().getName() );
            return null;
        } ) );
    }

    private void expect( Class<? extends ConsistencyReport> reportClass, String reportMethod )
    {
        verify( monitor, atLeastOnce() ).reported( eq( reportClass ), eq( reportMethod ), anyString() );
    }

    long index( SchemaDescriptor descriptor ) throws KernelException
    {
        long indexId;
        try ( KernelTransaction tx = ktx() )
        {
            IndexDescriptor index = tx.schemaWrite().indexCreate( descriptor, "the index" );
            tx.commit();
            indexId = index.getId();
        }
        awaitIndexesOnline();
        return indexId;
    }

    long uniqueIndex( SchemaDescriptor descriptor ) throws KernelException
    {
        long indexId;
        String constraintName = "me";
        try ( KernelTransaction tx = ktx() )
        {
            tx.schemaWrite().uniquePropertyConstraintCreate( IndexPrototype.uniqueForSchema( descriptor ).withName( constraintName ) );
            tx.commit();
        }
        try ( KernelTransaction tx = ktx() )
        {
            ConstraintDescriptor constraint = tx.schemaRead().constraintGetForName( constraintName );
            indexId = constraint.asUniquenessConstraint().ownedIndexId();
        }
        awaitIndexesOnline();
        return indexId;
    }

    private void awaitIndexesOnline()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
            tx.commit();
        }
    }

    KernelTransaction ktx() throws TransactionFailureException
    {
        Kernel kernel = db.getDependencyResolver().resolveDependency( Kernel.class );
        return kernel.beginTransaction( KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED );
    }

    /**
     * Convenience for a transaction that auto-commits.
     */
    AutoCloseable tx() throws TransactionFailureException
    {
        Kernel kernel = db.getDependencyResolver().resolveDependency( Kernel.class );
        KernelTransaction tx = kernel.beginTransaction( KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED );
        return () ->
        {
            tx.commit();
            tx.close();
        };
    }

    PropertyBlock propertyValue( int propertyKey, Value value )
    {
        PropertyBlock propertyBlock = new PropertyBlock();
        neoStores.getPropertyStore().encodeValue( propertyBlock, propertyKey, value );
        return propertyBlock;
    }

    long[] nodeLabels( NodeRecord node )
    {
        return NodeLabelsField.get( node, neoStores.getNodeStore() );
    }

    NodeRecord loadNode( long id )
    {
        return neoStores.getNodeStore().getRecord( id, neoStores.getNodeStore().newRecord(), RecordLoad.NORMAL );
    }

    long node( long id, long nextProp, long nextRel, int... labels )
    {
        NodeRecord node = new NodeRecord( id ).initialize( true, nextProp, false, NULL, 0 );
        long[] labelIds = toLongs( labels );
        InlineNodeLabels.putSorted( node, labelIds, nodeStore, null /*<-- intentionally prevent dynamic labels here*/ );
        nodeStore.updateRecord( node );
        return id;
    }

    long relationship( long id, long startNode, long endNode, int type, long startPrev, long startNext, long endPrev, long endNext, boolean firstInStart,
            boolean firstInEnd )
    {
        RelationshipRecord relationship = new RelationshipRecord( id ).initialize( true, NULL, startNode, endNode, type, startPrev, startNext, endPrev,
                endNext, firstInStart, firstInEnd );
        relationshipStore.updateRecord( relationship );
        return id;
    }

    long relationshipGroup( long id, long next, long owningNode, int type, long firstOut, long firstIn, long firstLoop )
    {
        RelationshipGroupRecord group = new RelationshipGroupRecord( id ).initialize( true, type, firstOut, firstIn, firstLoop, owningNode, next );
        relationshipGroupStore.updateRecord( group );
        return id;
    }

    long nodePlusCached( long id, long nextProp, long nextRel, int... labels )
    {
        long node = node( id, nextProp, NULL, labels );
        CacheAccess.Client client = cacheAccess.client();
        client.putToCacheSingle( id, CacheSlots.NodeLink.SLOT_IN_USE, 1 );
        client.putToCacheSingle( id, CacheSlots.NodeLink.SLOT_RELATIONSHIP_ID, nextRel );
        return node;
    }

    static long[] toLongs( int[] ints )
    {
        long[] longs = new long[ints.length];
        for ( int i = 0; i < ints.length; i++ )
        {
            longs[i] = ints[i];
        }
        return longs;
    }

    protected void property( long id, long prev, long next, PropertyBlock... properties )
    {
        PropertyRecord prop = new PropertyRecord( id ).initialize( true, prev, next );
        for ( PropertyBlock property : properties )
        {
            prop.addPropertyBlock( property );
        }
        propertyStore.updateRecord( prop );
    }

    private static class LookupAccessorsFromRunningDb implements ThrowingFunction<IndexDescriptor,IndexAccessor,IOException>
    {
        private final IndexingService indexingService;

        LookupAccessorsFromRunningDb( IndexingService indexingService )
        {
            this.indexingService = indexingService;
        }

        @Override
        public IndexAccessor apply( IndexDescriptor indexDescriptor )
        {
            try
            {
                IndexProxy proxy = indexingService.getIndexProxy( indexDescriptor.getId() );
                while ( proxy instanceof AbstractDelegatingIndexProxy )
                {
                    proxy = ((AbstractDelegatingIndexProxy) proxy).getDelegate();
                }
                assertEquals( InternalIndexState.ONLINE, proxy.getState() );
                return ((OnlineIndexProxy) proxy).accessor();
            }
            catch ( IndexNotFoundKernelException e )
            {
                throw new RuntimeException( e );
            }
        }
    }
}
