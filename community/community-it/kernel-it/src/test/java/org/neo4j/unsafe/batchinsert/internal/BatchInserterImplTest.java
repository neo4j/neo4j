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
package org.neo4j.unsafe.batchinsert.internal;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.function.ThrowingAction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.StoreLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.kernel.StoreLockException;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.internal.locker.StoreLocker;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.ReflectionUtil;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.io.ByteUnit.kibiBytes;

public class BatchInserterImplTest
{
    private static final String PAGE_CACHE_SIZE = "280K";
    private final TestDirectory testDirectory = TestDirectory.testDirectory();
    private final ExpectedException expected = ExpectedException.none();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( testDirectory )
                                                .around( expected ).around( fileSystemRule );

    @Test
    public void testHonorsPassedInParams() throws Exception
    {
        BatchInserter inserter = BatchInserters.inserter( testDirectory.databaseDir(), fileSystemRule.get(),
                inserterConfig() );
        NeoStores neoStores = ReflectionUtil.getPrivateField( inserter, "neoStores", NeoStores.class );
        PageCache pageCache = ReflectionUtil.getPrivateField( neoStores, "pageCache", PageCache.class );
        inserter.shutdown();
        long mappedMemoryTotalSize = MuninnPageCache.memoryRequiredForPages( pageCache.maxCachedPages() );
        assertThat( "memory mapped config is active", mappedMemoryTotalSize,
                is( allOf( greaterThan( kibiBytes( 270 ) ), lessThan( kibiBytes( 290 ) ) ) ) );
    }

    @Test
    public void testCreatesStoreLockFile() throws Exception
    {
        // Given
        DatabaseLayout databaseLayout = testDirectory.databaseLayout();

        // When
        BatchInserter inserter = BatchInserters.inserter( databaseLayout.databaseDirectory(), fileSystemRule.get() );

        // Then
        assertThat( databaseLayout.getStoreLayout().storeLockFile().exists(), equalTo( true ) );
        inserter.shutdown();
    }

    @Test
    public void testFailsOnExistingStoreLockFile() throws IOException
    {
        // Given
        StoreLayout storeLayout = testDirectory.storeLayout();
        try ( FileSystemAbstraction fileSystemAbstraction = new DefaultFileSystemAbstraction();
              StoreLocker lock = new StoreLocker( fileSystemAbstraction, storeLayout ) )
        {
            lock.checkLock();

            // Then
            expected.expect( StoreLockException.class );
            expected.expectMessage( "Unable to obtain lock on store lock file" );
            // When
            BatchInserters.inserter( storeLayout.databaseLayout( "any" ).databaseDirectory(), fileSystemAbstraction );
        }
    }

    @Test
    public void readOnlyInserterMustRefuseToCreateNode() throws IOException
    {
        BatchInserter inserter = readOnlyInserter();
        Label label = Label.label( "LABEL" );
        HashMap<String,Object> props = new HashMap<>();
        try
        {
            assertThrows( IllegalStateException.class, () -> inserter.createNode( props ) );
            assertThrows( IllegalStateException.class, () -> inserter.createNode( 53, props ) );
            assertThrows( IllegalStateException.class, () -> inserter.createNode( props, label ) );
            assertThrows( IllegalStateException.class, () -> inserter.createNode( 313, props, label ) );
            props.put( "a", 1 );
            assertThrows( IllegalStateException.class, () -> inserter.createNode( props ) );
            assertThrows( IllegalStateException.class, () -> inserter.createNode( 711, props ) );
            assertThrows( IllegalStateException.class, () -> inserter.createNode( props, label ) );
            assertThrows( IllegalStateException.class, () -> inserter.createNode( 1536, props, label ) );
        }
        finally
        {
            inserter.shutdown();
        }
    }

    @Test
    public void readOnlyInserterMustRefuseToCreateRelationship() throws IOException
    {
        BatchInserter inserter = readOnlyInserter();
        RelationshipType type = RelationshipType.withName( "REL" );
        HashMap<String,Object> props = new HashMap<>();
        try
        {
            assertThrows( IllegalStateException.class, () -> inserter.createRelationship( 1, 2, type, props ) );
            props.put( "a", 1 );
            assertThrows( IllegalStateException.class, () -> inserter.createRelationship( 1, 2, type, props ) );
        }
        finally
        {
            inserter.shutdown();
        }
    }

    @Test
    public void readOnlyInserterMustRefuseToChangeNode() throws IOException
    {
        BatchInserter inserter = readOnlyInserter();
        try
        {
            assertThrows( IllegalStateException.class, () -> inserter.setNodeLabels( 313, Label.label( "LABEL" ) ) );
            assertThrows( IllegalStateException.class, () -> inserter.setNodeProperty( 313, "prop", 13 ) );
            assertThrows( IllegalStateException.class, () -> inserter.setNodeProperties( 313, new HashMap<>() ) );
            assertThrows( IllegalStateException.class, () -> inserter.setNodeProperties( 313, singletonMap( "prop", 13 ) ) );
            assertThrows( IllegalStateException.class, () -> inserter.removeNodeProperty( 313, "prop" ) );
        }
        finally
        {
            inserter.shutdown();
        }
    }

    @Test
    public void readOnlyInserterMustRefuseToChangeRelationship() throws IOException
    {
        BatchInserter inserter = readOnlyInserter();
        try
        {
            assertThrows( IllegalStateException.class, () -> inserter.setRelationshipProperty( 313, "prop", 1 ) );
            assertThrows( IllegalStateException.class, () -> inserter.setRelationshipProperties( 313, new HashMap<>() ) );
            assertThrows( IllegalStateException.class, () -> inserter.setRelationshipProperties( 313, singletonMap( "prop", 1 ) ) );
            assertThrows( IllegalStateException.class, () -> inserter.removeRelationshipProperty( 313, "prop" ) );
        }
        finally
        {
            inserter.shutdown();
        }
    }

    @Test
    public void readOnlyInserterMustRefuseToChangeSchema() throws IOException
    {
        BatchInserter inserter = readOnlyInserter();
        Label label = Label.label( "Label" );
        try
        {
            assertThrows( IllegalStateException.class, () -> inserter.createDeferredConstraint( label ) );
            assertThrows( IllegalStateException.class, () -> inserter.createDeferredSchemaIndex( label ) );
        }
        finally
        {
            inserter.shutdown();
        }
    }

    @Test
    public void readOnlyInserterMustNotRebuildLabelScanStoreOnShutDown() throws IOException
    {
        BatchInserterImpl inserter = (BatchInserterImpl) readOnlyInserter();
        Monitors monitors = inserter.getMonitors();
        AtomicBoolean startedRebuilding = new AtomicBoolean();
        LabelScanStore.Monitor listener = new LabelScanStore.Monitor.Adaptor()
        {
            @Override
            public void rebuilding()
            {
                startedRebuilding.set( true );
            }
        };
        monitors.addMonitorListener( listener );
        inserter.shutdown();
        assertFalse( startedRebuilding.get() );
    }

    @Test
    public void readOnlyInserterMustNotRebuildIndexesOnShutDown() throws IOException
    {
        {
            DatabaseLayout layout = testDirectory.databaseLayout();
            File dir = layout.databaseDirectory();
            DefaultFileSystemAbstraction fsa = fileSystemRule.get();
            BatchInserter inserter = BatchInserters.inserter( dir, fsa, inserterConfig() );
            Label label = Label.label( "LABEL" );
            inserter.createDeferredSchemaIndex( label ).on( "prop" ).create();
            inserter.createNode( singletonMap( "prop", 1 ), label );
            inserter.createNode( singletonMap( "prop", 2 ), label );
            inserter.createNode( singletonMap( "prop", 3 ), label );
            inserter.shutdown();
        }

        BatchInserterImpl inserter = (BatchInserterImpl) readOnlyInserter();
        Monitors monitors = inserter.getMonitors();
        AtomicBoolean startedRebuilding = new AtomicBoolean();
        IndexingService.Monitor listener = new IndexingService.MonitorAdapter()
        {
            @Override
            public void indexPopulationScanStarting()
            {
                startedRebuilding.set( true );
            }
        };
        monitors.addMonitorListener( listener );
        inserter.shutdown();
        assertFalse( startedRebuilding.get() );
    }

    private BatchInserter readOnlyInserter() throws IOException
    {
        DatabaseLayout layout = testDirectory.databaseLayout();
        File dir = layout.databaseDirectory();
        DefaultFileSystemAbstraction fsa = fileSystemRule.get();
        Map<String,String> config = inserterConfig();
        config.put( GraphDatabaseSettings.read_only.name(), "true" );
        return BatchInserters.inserter( dir, fsa, config );
    }

    private Map<String,String> inserterConfig()
    {
        Map<String,String> config = new HashMap<>();
        config.put( GraphDatabaseSettings.pagecache_memory.name(), PAGE_CACHE_SIZE );
        return config;
    }

    private <E extends Exception> void assertThrows( Class<E> cls, ThrowingAction<E> action )
    {
        try
        {
            action.apply();
            fail( "Expected a " + cls + " exception to be thrown." );
        }
        catch ( Exception e )
        {
            if ( !cls.isInstance( e ) )
            {
                AssertionError error = new AssertionError( "Expected " + e + " to be an instance of " + cls );
                error.addSuppressed( e );
                throw error;
            }
        }
    }
}
