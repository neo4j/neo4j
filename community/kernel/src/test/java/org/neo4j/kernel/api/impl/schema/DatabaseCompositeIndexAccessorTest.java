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
package org.neo4j.kernel.api.impl.schema;

import org.assertj.core.api.Condition;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.function.IOFunction;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexQueryHelper;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.extension.DatabaseExtensions;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionFailureStrategies;
import org.neo4j.kernel.extension.context.DatabaseExtensionContext;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProviderFactory;
import org.neo4j.kernel.impl.index.schema.NodeValueIterator;
import org.neo4j.kernel.impl.index.schema.fusion.NativeLuceneFusionIndexProviderFactory30;
import org.neo4j.kernel.impl.pagecache.ConfigurableStandalonePageCacheFactory;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.rule.CleanupRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.concurrent.ThreadingRule;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.anyOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.collection.PrimitiveLongCollections.toSet;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.kernel.api.IndexQuery.exact;
import static org.neo4j.internal.kernel.api.QueryContext.NULL_CONTEXT;
import static org.neo4j.io.IOUtils.closeAll;
import static org.neo4j.io.memory.ByteBufferFactory.heapBufferFactory;
import static org.neo4j.test.rule.concurrent.ThreadingRule.waitingWhileIn;

@RunWith( Parameterized.class )
public class DatabaseCompositeIndexAccessorTest
{
    private static final int PROP_ID1 = 1;
    private static final int PROP_ID2 = 2;
    private static final Config CONFIG = Config.defaults();
    private static final IndexSamplingConfig SAMPLING_CONFIG = new IndexSamplingConfig( CONFIG );

    @Rule
    public final ThreadingRule threading = new ThreadingRule();

    private static final EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();
    private static final TestDirectory dir = TestDirectory.testDirectory( DatabaseCompositeIndexAccessorTest.class, fileSystemRule );
    private static final CleanupRule cleanup = new CleanupRule();
    private static final AssertableLogProvider logProvider = new AssertableLogProvider();
    @ClassRule
    public static final RuleChain rules = RuleChain.outerRule( fileSystemRule ).around( dir ).around( cleanup ).around( logProvider );

    @Parameterized.Parameter( 0 )
    public String testName;
    @Parameterized.Parameter( 1 )
    public IOFunction<DirectoryFactory,IndexAccessor> accessorFactory;

    private IndexAccessor accessor;
    private final long nodeId = 1;
    private final long nodeId2 = 2;
    private final Object[] values = {"value1", "values2"};
    private final Object[] values2 = {40, 42};
    private DirectoryFactory.InMemoryDirectoryFactory dirFactory;
    private static final IndexPrototype SCHEMA_INDEX_DESCRIPTOR = IndexPrototype.forSchema( SchemaDescriptor.forLabel( 0, PROP_ID1, PROP_ID2 ) );
    private static final IndexPrototype UNIQUE_SCHEMA_INDEX_DESCRIPTOR = IndexPrototype.uniqueForSchema( SchemaDescriptor.forLabel( 1, PROP_ID1, PROP_ID2 ) );

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> implementations() throws IOException
    {
        Collection<ExtensionFactory<?>> indexProviderFactories = Arrays.asList(
                new GenericNativeIndexProviderFactory(),
                new NativeLuceneFusionIndexProviderFactory30() );

        Dependencies deps = new Dependencies();
        JobScheduler jobScheduler = cleanup.add( JobSchedulerFactory.createInitialisedScheduler() );
        PageCache pageCache = cleanup.add( ConfigurableStandalonePageCacheFactory.createPageCache( fileSystemRule, jobScheduler ) );
        deps.satisfyDependencies( pageCache, jobScheduler, fileSystemRule, new SimpleLogService( logProvider ), new Monitors(), CONFIG,
                RecoveryCleanupWorkCollector.ignore() );
        dir.prepareDirectory( DatabaseCompositeIndexAccessorTest.class, "null" );
        Config config = Config.defaults( neo4j_home, dir.homeDir().toPath() );
        DatabaseExtensionContext context = new DatabaseExtensionContext( DatabaseLayout.of( config ), DatabaseInfo.UNKNOWN, deps );
        DatabaseExtensions extensions = new DatabaseExtensions( context, indexProviderFactories, deps, ExtensionFailureStrategies.fail() );

        extensions.init();
        var providers = deps.resolveTypeDependencies( IndexProvider.class );

        Collection<Object[]> params = new ArrayList<>();
        for ( IndexProvider provider : providers )
        {
            params.add( parameterSetup( provider,
                    provider.completeConfiguration( SCHEMA_INDEX_DESCRIPTOR.withName( "index_" + params.size() ).materialise( params.size() ) ) ) );
            params.add( parameterSetup( provider,
                    provider.completeConfiguration( UNIQUE_SCHEMA_INDEX_DESCRIPTOR.withName( "constraint_" + params.size() ).materialise( params.size() ) ) ) );
        }

        return params;
    }

    private static Object[] parameterSetup( IndexProvider provider, IndexDescriptor descriptor )
    {
        IOFunction function = dirFactory1 ->
        {
            IndexPopulator populator = provider.getPopulator( descriptor, SAMPLING_CONFIG, heapBufferFactory( 1024 ) );
            populator.create();
            populator.close( true );

            return provider.getOnlineAccessor( descriptor, SAMPLING_CONFIG );
        };
        return new Object[]{ provider.getClass().getSimpleName() + " / " + descriptor, function};
    }

    @Before
    public void before() throws IOException
    {
        dirFactory = new DirectoryFactory.InMemoryDirectoryFactory();
        accessor = accessorFactory.apply( dirFactory );
    }

    @After
    public void after() throws IOException
    {
        closeAll( accessor, dirFactory );
    }

    @Test
    public void indexReaderShouldSupportScan() throws Exception
    {
        // GIVEN
        updateAndCommit( asList( add( nodeId, values ), add( nodeId2, values2 ) ) );
        IndexReader reader = accessor.newReader();

        // WHEN
        Set<Long> results = resultSet( reader, IndexQuery.exists( PROP_ID1 ), IndexQuery.exists( PROP_ID2 ) );
        Set<Long> results2 = resultSet( reader, exact( PROP_ID1, values[0] ), exact( PROP_ID2, values[1] ) );

        // THEN
        assertEquals( asSet( nodeId, nodeId2 ), results );
        assertEquals( asSet( nodeId ), results2 );
        reader.close();
    }

    @Test
    public void multipleIndexReadersFromDifferentPointsInTimeCanSeeDifferentResults() throws Exception
    {
        // WHEN
        updateAndCommit( singletonList( add( nodeId, values ) ) );
        IndexReader firstReader = accessor.newReader();
        updateAndCommit( singletonList( add( nodeId2, values2 ) ) );
        IndexReader secondReader = accessor.newReader();

        // THEN
        assertEquals( asSet( nodeId ), resultSet( firstReader, exact( PROP_ID1, values[0] ), exact( PROP_ID2, values[1] ) ) );
        assertThat( resultSet( firstReader, exact( PROP_ID1, values2[0] ), exact( PROP_ID2, values2[1] ) ) ).
                is( anyOf( new Condition<>( s -> s.equals( asSet() ), "empty set" ),
                          new Condition<>( s -> s.equals( asSet( nodeId2 ) ), "one element" ) ) );
        assertEquals( asSet( nodeId ), resultSet( secondReader, exact( PROP_ID1, values[0] ), exact( PROP_ID2, values[1] ) ) );
        assertEquals( asSet( nodeId2 ), resultSet( secondReader, exact( PROP_ID1, values2[0] ), exact( PROP_ID2, values2[1] ) ) );
        firstReader.close();
        secondReader.close();
    }

    @Test
    public void canAddNewData() throws Exception
    {
        // WHEN
        updateAndCommit( asList( add( nodeId, values ), add( nodeId2, values2 ) ) );
        IndexReader reader = accessor.newReader();

        // THEN
        assertEquals( asSet( nodeId ), resultSet( reader, exact( PROP_ID1, values[0] ), exact( PROP_ID2, values[1] ) ) );
        reader.close();
    }

    @Test
    public void canChangeExistingData() throws Exception
    {
        // GIVEN
        updateAndCommit( singletonList( add( nodeId, values ) ) );

        // WHEN
        updateAndCommit( singletonList( change( nodeId, values, values2 ) ) );
        IndexReader reader = accessor.newReader();

        // THEN
        assertEquals( asSet( nodeId ), resultSet( reader, exact( PROP_ID1, values2[0] ), exact( PROP_ID2, values2[1] ) ) );
        assertEquals( emptySet(), resultSet( reader, exact( PROP_ID1, values[0] ), exact( PROP_ID2, values[1] ) ) );
        reader.close();
    }

    @Test
    public void canRemoveExistingData() throws Exception
    {
        // GIVEN
        updateAndCommit( asList( add( nodeId, values ), add( nodeId2, values2 ) ) );

        // WHEN
        updateAndCommit( singletonList( remove( nodeId, values ) ) );
        IndexReader reader = accessor.newReader();

        // THEN
        assertEquals( asSet( nodeId2 ), resultSet( reader, exact( PROP_ID1, values2[0] ), exact( PROP_ID2, values2[1] ) ) );
        assertEquals( asSet(), resultSet( reader, exact( PROP_ID1, values[0] ), exact( PROP_ID2, values[1] ) ) );
        reader.close();
    }

    @Test( timeout = 60_000 )
    public void shouldStopSamplingWhenIndexIsDropped() throws Exception
    {
        // given
        updateAndCommit( asList( add( nodeId, values ), add( nodeId2, values2 ) ) );

        // when
        IndexReader indexReader = accessor.newReader(); // needs to be acquired before drop() is called
        IndexSampler indexSampler = indexReader.createSampler();

        AtomicBoolean droppedLatch = new AtomicBoolean();
        AtomicReference<Thread> dropper = new AtomicReference<>();
        Predicate<Thread> awaitCompletion = waitingWhileIn( TaskCoordinator.class, "awaitCompletion" );
        Future<Void> drop = threading.execute( nothing ->
        {
            dropper.set( Thread.currentThread() );
            try
            {
                accessor.drop();
            }
            finally
            {
                droppedLatch.set( true );
            }
            return null;
        }, null );

        try ( IndexReader reader = indexReader /* do not inline! */;
              IndexSampler sampler = indexSampler /* do not inline! */ )
        {
            while ( !droppedLatch.get() && !awaitCompletion.test( dropper.get() ) )
            {
                Thread.onSpinWait();
            }
            sampler.sampleIndex();
            fail( "expected exception" );
        }
        catch ( IndexNotFoundKernelException e )
        {
            assertEquals( "Index dropped while sampling.", e.getMessage() );
        }
        finally
        {
            drop.get();
        }
    }

    private Set<Long> resultSet( IndexReader reader, IndexQuery... queries ) throws IndexNotApplicableKernelException
    {
        try ( NodeValueIterator results = new NodeValueIterator() )
        {
            reader.query( NULL_CONTEXT, results, IndexOrder.NONE, false, queries );
            return toSet( results );
        }
    }

    private IndexEntryUpdate<?> add( long nodeId, Object... values )
    {
        return IndexQueryHelper.add( nodeId, SCHEMA_INDEX_DESCRIPTOR.schema(), values );
    }

    private IndexEntryUpdate<?> remove( long nodeId, Object... values )
    {
        return IndexQueryHelper.remove( nodeId, SCHEMA_INDEX_DESCRIPTOR.schema(), values );
    }

    private IndexEntryUpdate<?> change( long nodeId, Object[] valuesBefore, Object[] valuesAfter )
    {
        return IndexQueryHelper.change( nodeId, SCHEMA_INDEX_DESCRIPTOR.schema(), valuesBefore, valuesAfter );
    }

    private void updateAndCommit( List<IndexEntryUpdate<?>> nodePropertyUpdates )
            throws IndexEntryConflictException
    {
        try ( IndexUpdater updater = accessor.newUpdater( IndexUpdateMode.ONLINE ) )
        {
            for ( IndexEntryUpdate<?> update : nodePropertyUpdates )
            {
                updater.process( update );
            }
        }
    }
}
