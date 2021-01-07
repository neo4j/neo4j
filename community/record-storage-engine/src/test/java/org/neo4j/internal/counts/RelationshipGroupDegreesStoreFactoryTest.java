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
package org.neo4j.internal.counts;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseInternalSettings.FeatureState;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.FeatureState.AUTO;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.FeatureState.DISABLED;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.FeatureState.ENABLED;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.ignore;
import static org.neo4j.internal.counts.GBPTreeGenericCountsStore.NO_MONITOR;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.storageengine.api.RelationshipDirection.OUTGOING;

@EphemeralPageCacheExtension
class RelationshipGroupDegreesStoreFactoryTest
{
    @Inject
    private PageCache pageCache;
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private TestDirectory directory;

    private Config config;
    private Path storePath;
    private DatabaseLayout layout;

    @BeforeEach
    void setUp() throws IOException
    {
        config = Config.defaults( GraphDatabaseSettings.neo4j_home, directory.homePath() );
        layout = DatabaseLayout.of( config );
        storePath = layout.relationshipGroupDegreesStore();
        fs.mkdirs( storePath.getParent() );
    }

    @Test
    void shouldCreateTheStoreWhenRelaxedLockingIsEnabled()
    {
        //When
        createAndCloseStore( ENABLED );
        //Then
        assertThat( fs.fileExists( storePath ) ).isTrue();
    }

    @Test
    void shouldNotCreateTheStoreWhenRelaxedLockingIsDisabled()
    {
        //When
        createAndCloseStore( DISABLED );
        //Then
        assertThat( fs.fileExists( storePath ) ).isFalse();
    }

    @Test
    void shouldNotAllowTheDegreesStoreToBeDisabledOnceEnabled()
    {
        //When
        createAndCloseStore( ENABLED );
        //Then
        assertThatThrownBy( () -> createAndCloseStore( DISABLED ) ).hasMessageContaining( "Can not disable this feature once enabled" );
    }

    @Test
    void shouldNotAllowDegreesStoreReadsOrWritesWhenDisabled()
    {
        //Given
        try ( RelationshipGroupDegreesStore store = createAndStartStore( DISABLED ) )
        {
            //Then (read)
            assertThatThrownBy( () -> store.degree( 0, OUTGOING, PageCursorTracer.NULL ) )
                    .hasMessageContaining( "This store does not support any operations" );
            //Then (write)
            assertThatThrownBy( () -> store.apply(1, PageCursorTracer.NULL ).increment( 0, OUTGOING, 3 ) )
                    .hasMessageContaining( "This store does not support any operations" );
        }
    }

    @Test
    void shouldBeDisabledForNewDatabaseOnAuto()
    {
        //When
        createAndCloseStore( AUTO );
        //Then
        assertThat( fs.fileExists( storePath ) ).isFalse();
    }

    @Test
    void shouldKeepTheStateOnExistingDatabaseOnAuto()
    {
        //When
        createAndCloseStore( DISABLED );
        try ( RelationshipGroupDegreesStore store = createAndStartStore( AUTO ) )
        {
            //Then
            assertThat( fs.fileExists( storePath ) ).isFalse();
            assertThatCode( () -> store.degree( 0, OUTGOING, PageCursorTracer.NULL ) )
                                .hasMessageContaining( "This store does not support any operations" );
        }

        //When
        createAndCloseStore( ENABLED );
        try ( RelationshipGroupDegreesStore store = createAndStartStore( AUTO ) )
        {
            //Then
            assertThat( fs.fileExists( storePath ) ).isTrue();
            assertThatCode( () -> store.degree( 0, OUTGOING, PageCursorTracer.NULL ) ).doesNotThrowAnyException();
        }
    }

    private void createAndCloseStore( FeatureState state )
    {
        try ( var store = createAndStartStore( state ) )
        { //do nothing
        }
    }

    private RelationshipGroupDegreesStore createAndStartStore( FeatureState state )
    {
        config.set( GraphDatabaseInternalSettings.relaxed_dense_node_locking, state );
        RelationshipGroupDegreesStore store = null;
        try
        {
            NeoStores neoStores = mock( NeoStores.class );
            MetaDataStore metaDataStore = mock( MetaDataStore.class );
            when( neoStores.getMetaDataStore() ).thenReturn( metaDataStore );
            RelationshipGroupStore groupStore = mock( RelationshipGroupStore.class );
            when( neoStores.getRelationshipGroupStore() ).thenReturn( groupStore );
            store = RelationshipGroupDegreesStoreFactory.create( config, pageCache, layout, fs, ignore(), neoStores, NULL, NO_MONITOR );
            store.start( PageCursorTracer.NULL, EmptyMemoryTracker.INSTANCE );
            return store;
        }
        catch ( IOException e )
        {
            if ( store != null )
            {
                store.close();
            }
            throw new UncheckedIOException( e );
        }
    }
}
