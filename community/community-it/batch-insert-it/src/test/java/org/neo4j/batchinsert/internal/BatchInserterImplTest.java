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
package org.neo4j.batchinsert.internal;

import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.batchinsert.BatchInserters;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.kernel.internal.locker.DatabaseLocker;
import org.neo4j.kernel.internal.locker.FileLockException;
import org.neo4j.kernel.internal.locker.Locker;
import org.neo4j.lock.LockTracer;
import org.neo4j.service.Services;
import org.neo4j.test.ReflectionUtil;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.TestLabels;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;

import static org.apache.commons.lang3.RandomStringUtils.randomAscii;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.Config.defaults;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.logical_log_rotation_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.io.ByteUnit.kibiBytes;

@Neo4jLayoutExtension
class BatchInserterImplTest
{
    private final Config config = defaults( Map.of(
            pagecache_memory, "280K",
            logical_log_rotation_threshold, ByteUnit.mebiBytes( 1 ) ) );

    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private DatabaseLayout databaseLayout;

    @Test
    void testHonorsPassedInParams() throws Exception
    {
        try ( var inserter = BatchInserters.inserter( databaseLayout, fileSystem, config ) )
        {
            NeoStores neoStores = ReflectionUtil.getPrivateField( inserter, "neoStores", NeoStores.class );
            PageCache pageCache = ReflectionUtil.getPrivateField( neoStores, "pageCache", PageCache.class );
            long mappedMemoryTotalSize = MuninnPageCache.memoryRequiredForPages( pageCache.maxCachedPages() );
            assertThat( mappedMemoryTotalSize ).as( "memory mapped config is active" ).isGreaterThan( kibiBytes( 270 ) ).isLessThan( kibiBytes( 290 ) );
        }
    }

    @Test
    void testCreatesLockFile() throws Exception
    {
        try ( var inserter = BatchInserters.inserter( databaseLayout, fileSystem ) )
        {
            assertTrue( Files.exists( databaseLayout.databaseLockFile() ) );
        }
    }

    @Test
    void testFailsOnExistingStoreLockFile() throws IOException
    {
        // Given

        try ( Locker lock = new DatabaseLocker( fileSystem, databaseLayout ) )
        {
            lock.checkLock();

            var e = assertThrows( FileLockException.class,
                () -> BatchInserters.inserter( databaseLayout, fileSystem ) );
            assertThat( e.getMessage() ).startsWith( "Unable to obtain lock on file" );
        }
    }

    @Test
    void tracePageCacheAccessInBatchInserter() throws IOException
    {
        var pageCacheTracer = new DefaultPageCacheTracer();
        var databaseTracers = new DatabaseTracers( DatabaseTracer.NULL, LockTracer.NONE, pageCacheTracer );
        try ( var inserter = new BatchInserterImpl( databaseLayout, fileSystem, config, loadExtensions(), databaseTracers ) )
        {
            for ( int i = 0; i < 10; i++ )
            {
                inserter.createNode( Map.of( "name", randomAscii( 10 ) ), label( "marker" ) );
            }
        }

        long pins = pageCacheTracer.pins();
        assertThat( pins ).isGreaterThan( 0 );
        assertThat( pageCacheTracer.unpins() ).isEqualTo( pins );
        assertThat( pageCacheTracer.hits() ).isGreaterThan( 0 ).isLessThanOrEqualTo( pins );
        assertThat( pageCacheTracer.faults() ).isGreaterThan( 0 ).isLessThanOrEqualTo( pins );
        assertThat( pageCacheTracer.flushes() ).isGreaterThan( 0 );
    }

    @Test
    void shouldCorrectlyMarkHighIds() throws Exception
    {
        // given
        DatabaseLayout layout = databaseLayout;
        long[] nodeIds = new long[10];
        try ( var inserter = BatchInserters.inserter( layout, fileSystem, config ) )
        {
            Map<String,Object> properties = new HashMap<>();
            properties.put( "name", "Just some name" );
            properties.put( "some_array", new String[]{"this", "is", "a", "string", "which", "really", "is", "an", "array"} );
            for ( int i = 0; i < nodeIds.length; i++ )
            {
                nodeIds[i] = inserter.createNode( properties );
            }
        }
        MutableLongSet nodeIdsSet = LongSets.mutable.of( nodeIds );

        // when/then
        DatabaseManagementService dbms =
                new TestDatabaseManagementServiceBuilder( layout.getNeo4jLayout().homeDirectory() ).setFileSystem( fileSystem ).build();
        try
        {
            GraphDatabaseService db = dbms.database( DEFAULT_DATABASE_NAME );
            for ( long nodeId : nodeIds )
            {
                try ( Transaction tx = db.beginTx() )
                {
                    tx.getNodeById( nodeId ).addLabel( TestLabels.LABEL_ONE );
                    tx.commit();
                }
            }
            for ( int i = 0; i < 5; i++ )
            {
                try ( Transaction tx = db.beginTx() )
                {
                    Node node = tx.createNode();
                    assertFalse( nodeIdsSet.contains( node.getId() ) );
                    tx.commit();
                }
            }

            for ( long nodeId : nodeIds )
            {
                try ( Transaction tx = db.beginTx() )
                {
                    tx.getNodeById( nodeId ).delete();
                    tx.commit();
                }
            }
        }
        finally
        {
            dbms.shutdown();
        }
    }

    private Iterable loadExtensions()
    {
        return Services.loadAll( ExtensionFactory.class );
    }
}
