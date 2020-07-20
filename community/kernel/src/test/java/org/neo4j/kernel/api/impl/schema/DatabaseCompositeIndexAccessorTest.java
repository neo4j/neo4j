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
package org.neo4j.kernel.api.impl.schema;

import org.assertj.core.api.Condition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Predicate;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
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
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProviderFactory;
import org.neo4j.kernel.impl.index.schema.NodeValueIterator;
import org.neo4j.kernel.impl.index.schema.fusion.NativeLuceneFusionIndexProviderFactory30;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.concurrent.ThreadingExtension;
import org.neo4j.test.rule.concurrent.ThreadingRule;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.anyOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.collection.PrimitiveLongCollections.toSet;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.kernel.api.IndexQuery.exact;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.kernel.api.QueryContext.NULL_CONTEXT;
import static org.neo4j.io.IOUtils.closeAll;
import static org.neo4j.io.memory.ByteBufferFactory.heapBufferFactory;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.test.rule.concurrent.ThreadingRule.waitingWhileIn;

@EphemeralPageCacheExtension
@ExtendWith( ThreadingExtension.class )
@TestInstance( TestInstance.Lifecycle.PER_CLASS )
public class DatabaseCompositeIndexAccessorTest
{
    private static final int PROP_ID1 = 1;
    private static final int PROP_ID2 = 2;
    private static final Config CONFIG = Config.defaults();
    private static final IndexSamplingConfig SAMPLING_CONFIG = new IndexSamplingConfig( CONFIG );
    private static final AssertableLogProvider logProvider = new AssertableLogProvider();

    @Inject
    private ThreadingRule threading;
    @Inject
    private PageCache pageCache;
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private TestDirectory testDirectory;

    private final long nodeId = 1;
    private final long nodeId2 = 2;
    private final Object[] values = {"value1", "values2"};
    private final Object[] values2 = {40, 42};
    private DirectoryFactory.InMemoryDirectoryFactory dirFactory;
    private static final IndexPrototype SCHEMA_INDEX_DESCRIPTOR = IndexPrototype.forSchema( SchemaDescriptor.forLabel( 0, PROP_ID1, PROP_ID2 ) );
    private static final IndexPrototype UNIQUE_SCHEMA_INDEX_DESCRIPTOR = IndexPrototype.uniqueForSchema( SchemaDescriptor.forLabel( 1, PROP_ID1, PROP_ID2 ) );
    private final JobScheduler jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
    private Iterable<IndexProvider> providers;

    @BeforeAll
    void prepareProviders() throws IOException
    {
        dirFactory = new DirectoryFactory.InMemoryDirectoryFactory();
        providers = getIndexProviders( pageCache, jobScheduler, fileSystem, testDirectory );
    }

    @AfterAll
    public void after() throws IOException
    {
        closeAll( dirFactory, jobScheduler );
    }

    @Nested
    @TestInstance( TestInstance.Lifecycle.PER_CLASS )
    class CompositeTests
    {
        private List<IndexAccessor> indexAccessors() throws IOException
        {
            List<IndexAccessor> accessors = new ArrayList<>();
            for ( IndexProvider p : providers )
            {
                accessors.add( indexAccessor( p, p.completeConfiguration( SCHEMA_INDEX_DESCRIPTOR.withName( "index_" + 0 ).materialise( 0 ) ) ) );
                accessors.add( indexAccessor( p, p.completeConfiguration( UNIQUE_SCHEMA_INDEX_DESCRIPTOR.withName( "constraint_" + 1 ).materialise( 1 ) ) ) );
            }
            return accessors;
        }

        @ParameterizedTest
        @MethodSource( "indexAccessors" )
        void indexReaderShouldSupportScan( IndexAccessor accessor ) throws Exception
        {
            // GIVEN
            try ( accessor )
            {
                updateAndCommit( accessor, asList( add( nodeId, values ), add( nodeId2, values2 ) ) );
                try ( IndexReader reader = accessor.newReader() )
                {

                    // WHEN
                    Set<Long> results = resultSet( reader, IndexQuery.exists( PROP_ID1 ), IndexQuery.exists( PROP_ID2 ) );
                    Set<Long> results2 = resultSet( reader, exact( PROP_ID1, values[0] ), exact( PROP_ID2, values[1] ) );

                    // THEN
                    assertEquals( asSet( nodeId, nodeId2 ), results );
                    assertEquals( asSet( nodeId ), results2 );
                }
            }
        }

        @ParameterizedTest
        @MethodSource( "indexAccessors" )
        void multipleIndexReadersFromDifferentPointsInTimeCanSeeDifferentResults( IndexAccessor accessor ) throws Exception
        {
            // WHEN
            try ( accessor )
            {
                updateAndCommit( accessor, singletonList( add( nodeId, values ) ) );
                IndexReader firstReader = accessor.newReader();
                updateAndCommit( accessor, singletonList( add( nodeId2, values2 ) ) );
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
        }

        @ParameterizedTest
        @MethodSource( "indexAccessors" )
        void canAddNewData( IndexAccessor accessor ) throws Exception
        {
            // WHEN
            try ( accessor )
            {
                updateAndCommit( accessor, asList( add( nodeId, values ), add( nodeId2, values2 ) ) );
                try ( IndexReader reader = accessor.newReader() )
                {
                    assertEquals( asSet( nodeId ), resultSet( reader, exact( PROP_ID1, values[0] ), exact( PROP_ID2, values[1] ) ) );
                }
            }
        }

        @ParameterizedTest
        @MethodSource( "indexAccessors" )
        void canChangeExistingData( IndexAccessor accessor ) throws Exception
        {
            // GIVEN
            try ( accessor )
            {
                updateAndCommit( accessor, singletonList( add( nodeId, values ) ) );

                // WHEN
                updateAndCommit( accessor, singletonList( change( nodeId, values, values2 ) ) );
                try ( IndexReader reader = accessor.newReader() )
                {
                    // THEN
                    assertEquals( asSet( nodeId ), resultSet( reader, exact( PROP_ID1, values2[0] ), exact( PROP_ID2, values2[1] ) ) );
                    assertEquals( emptySet(), resultSet( reader, exact( PROP_ID1, values[0] ), exact( PROP_ID2, values[1] ) ) );
                }
            }
        }

        @ParameterizedTest
        @MethodSource( "indexAccessors" )
        void canRemoveExistingData( IndexAccessor accessor ) throws Exception
        {
            // GIVEN
            try ( accessor )
            {
                updateAndCommit( accessor, asList( add( nodeId, values ), add( nodeId2, values2 ) ) );

                // WHEN
                updateAndCommit( accessor, singletonList( remove( nodeId, values ) ) );
                try ( IndexReader reader = accessor.newReader() )
                {
                    // THEN
                    assertEquals( asSet( nodeId2 ), resultSet( reader, exact( PROP_ID1, values2[0] ), exact( PROP_ID2, values2[1] ) ) );
                    assertEquals( asSet(), resultSet( reader, exact( PROP_ID1, values[0] ), exact( PROP_ID2, values[1] ) ) );
                }
            }
        }

        @ParameterizedTest
        @MethodSource( "indexAccessors" )
        void shouldStopSamplingWhenIndexIsDropped( IndexAccessor accessor ) throws Exception
        {
            // given
            try ( accessor )
            {
                updateAndCommit( accessor, asList( add( nodeId, values ), add( nodeId2, values2 ) ) );

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

                var e = assertThrows( IndexNotFoundKernelException.class, () ->
                {
                    try ( var reader = indexReader /* do not inline! */; IndexSampler sampler = indexSampler /* do not inline! */ )
                    {
                        while ( !droppedLatch.get() && !awaitCompletion.test( dropper.get() ) )
                        {
                            LockSupport.parkNanos( MILLISECONDS.toNanos( 10 ) );
                        }
                        sampler.sampleIndex( NULL );
                    }
                    finally
                    {
                        drop.get();
                    }
                } );
                assertThat( e ).hasMessage( "Index dropped while sampling." );
            }
        }
    }

    private static Iterable<IndexProvider> getIndexProviders( PageCache pageCache, JobScheduler jobScheduler, FileSystemAbstraction fileSystem,
            TestDirectory testDirectory ) throws IOException
    {
        Collection<ExtensionFactory<?>> indexProviderFactories = Arrays.asList(
                new GenericNativeIndexProviderFactory(),
                new NativeLuceneFusionIndexProviderFactory30() );

        Dependencies deps = new Dependencies();
        deps.satisfyDependencies( pageCache, jobScheduler, fileSystem, new SimpleLogService( logProvider ), new Monitors(), CONFIG,
                RecoveryCleanupWorkCollector.ignore() );
        testDirectory.prepareDirectory( DatabaseCompositeIndexAccessorTest.class, "null" );
        Config config = Config.defaults( neo4j_home, testDirectory.homePath() );
        DatabaseExtensionContext context = new DatabaseExtensionContext( DatabaseLayout.of( config ), DbmsInfo.UNKNOWN, deps );
        DatabaseExtensions extensions = new DatabaseExtensions( context, indexProviderFactories, deps, ExtensionFailureStrategies.fail() );

        extensions.init();
        return deps.resolveTypeDependencies( IndexProvider.class );
    }

    private static IndexAccessor indexAccessor( IndexProvider provider, IndexDescriptor descriptor ) throws IOException
    {
        IndexPopulator populator = provider.getPopulator( descriptor, SAMPLING_CONFIG, heapBufferFactory( 1024 ), INSTANCE );
        populator.create();
        populator.close( true, NULL );

        return provider.getOnlineAccessor( descriptor, SAMPLING_CONFIG );
    }

    private Set<Long> resultSet( IndexReader reader, IndexQuery... queries ) throws IndexNotApplicableKernelException
    {
        try ( NodeValueIterator results = new NodeValueIterator() )
        {
            reader.query( NULL_CONTEXT, results, unconstrained(), queries );
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

    private void updateAndCommit( IndexAccessor accessor, List<IndexEntryUpdate<?>> nodePropertyUpdates ) throws IndexEntryConflictException
    {
        try ( IndexUpdater updater = accessor.newUpdater( IndexUpdateMode.ONLINE, NULL ) )
        {
            for ( IndexEntryUpdate<?> update : nodePropertyUpdates )
            {
                updater.process( update );
            }
        }
    }
}
