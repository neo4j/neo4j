/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.store.AbstractStore;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.kernel.impl.storemigration.SchemaIndexMigrator.SchemaStoreProvider;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader.UnexpectedUpgradingStoreVersionException;
import org.neo4j.kernel.impl.storemigration.legacystore.v19.Legacy19Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v20.Legacy20Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v21.Legacy21Store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.index.SchemaIndexProvider.getRootDirectory;

public class SchemaIndexMigratorTest
{
    @Test
    public void shouldNotMigrateFor1_9Version() throws Exception
    {
        // given
        when( upgradableDatabase.hasCurrentVersion( fs, storeDir )).thenReturn( false );
        when( upgradableDatabase.checkUpgradeable( storeDir ) ).thenReturn( Legacy19Store.LEGACY_VERSION );

        // when
        boolean needsMigration = migrator.needsMigration( storeDir );

        // then
        assertFalse( needsMigration );
    }

    @Test
    public void shouldMigrateFor2_0Version() throws Exception
    {
        // given
        when( upgradableDatabase.hasCurrentVersion( fs, storeDir )).thenReturn( false );
        when( upgradableDatabase.checkUpgradeable( storeDir ) ).thenReturn( Legacy20Store.LEGACY_VERSION );

        // when
        boolean needsMigration = migrator.needsMigration( storeDir );

        // then
        assertTrue( needsMigration );
    }

    @Test
    public void shouldMigrateFor2_1Version() throws Exception
    {
        // given
        when( upgradableDatabase.hasCurrentVersion( fs, storeDir )).thenReturn( false );
        when( upgradableDatabase.checkUpgradeable( storeDir ) ).thenReturn( Legacy21Store.LEGACY_VERSION );

        // when
        boolean needsMigration = migrator.needsMigration( storeDir );

        // then
        assertTrue( needsMigration );
    }

    @Test
    public void shouldNotMigrateFor2_2Version() throws Exception
    {
        // given
        when( upgradableDatabase.hasCurrentVersion( fs, storeDir )).thenReturn( true );
        when( upgradableDatabase.checkUpgradeable( storeDir ) ).thenThrow( new UnexpectedUpgradingStoreVersionException(
                NeoStore.DEFAULT_NAME, Legacy21Store.LEGACY_VERSION, AbstractStore.ALL_STORES_VERSION ) );

        // when
        boolean needsMigration = migrator.needsMigration( storeDir );

        // then
        assertFalse( needsMigration );
    }

    @Test
    public void shouldThrowWhenTryingToMigrateAnUnexpectedVersion() throws Exception
    {
        // given
        when( upgradableDatabase.checkUpgradeable( storeDir ) ).thenReturn( Legacy19Store.LEGACY_VERSION );

        // when
        try
        {
            migrator.migrate( storeDir, migrationDir, schemaIndexProvider, pageCache );
            fail( "should have thrown" );
        }
        catch ( IllegalStateException ex )
        {
            // then
            assertEquals( "Unknown version to upgrade from: " + Legacy19Store.LEGACY_VERSION, ex.getMessage() );
        }
    }

    @Test
    public void shouldDeleteTheIndexIfItContainsIndexedArrayValues() throws Exception
    {
        // given
        when( upgradableDatabase.checkUpgradeable( storeDir ) ).thenReturn( Legacy21Store.LEGACY_VERSION );
        when( indexReader.valueTypesInIndex() ).thenReturn( asSet( String.class, Array.class ) );

        // when
        migrator.migrate( storeDir, migrationDir, schemaIndexProvider, pageCache );

        // then
        verify( fs ).deleteRecursively( new File( getRootDirectory( storeDir, schemaIndexProvider.getProviderDescriptor().getKey() ), "" + indexRuleId ) );
    }

    @Test
    public void shouldNotDeleteTheIndexIfItDoesNotContainIndexedArrayValues() throws Exception
    {
        // given
        when( upgradableDatabase.checkUpgradeable( storeDir ) ).thenReturn( Legacy21Store.LEGACY_VERSION );
        when( indexReader.valueTypesInIndex() ).thenReturn( asSet( String.class, Boolean.class ) );

        // when
        migrator.migrate( storeDir, migrationDir, schemaIndexProvider, pageCache );

        // then
        verify( fs, never() ).deleteRecursively( new File( getRootDirectory( storeDir, schemaIndexProvider.getProviderDescriptor().getKey() ), "" + indexRuleId ) );
    }

    @Test
    public void shouldHandleMixedIndexesContainingOrNotArrayValues() throws Exception
    {
        // given
        int anotherIndexRuleId = 1;
        int otherIndexRuleId = 2;
        int yetAnotherIndexRuleId = 3;

        when( upgradableDatabase.checkUpgradeable( storeDir ) ).thenReturn( Legacy21Store.LEGACY_VERSION );
        Iterator<SchemaRule> iterator = Arrays.asList(
                schemaRule( indexRuleId, 42, 21 ), schemaRule( anotherIndexRuleId, 41, 20 ),
                schemaRule( otherIndexRuleId, 40, 19 ), schemaRule( yetAnotherIndexRuleId, 40, 19 )
        ).iterator();
        when( schemaStore.loadAllSchemaRules() ).thenReturn( iterator );
        when( schemaIndexProvider.getOnlineAccessor( indexRuleId, indexConfig, samplingConfig )).thenReturn( accessor );
        IndexAccessor anotherAccessor = mock( IndexAccessor.class );
        when( schemaIndexProvider.getOnlineAccessor( anotherIndexRuleId, indexConfig, samplingConfig )).thenReturn( anotherAccessor );
        IndexAccessor otherAccessor = mock( IndexAccessor.class );
        when( schemaIndexProvider.getOnlineAccessor( otherIndexRuleId, indexConfig, samplingConfig )).thenReturn( otherAccessor );
        IndexAccessor yetAnotherIndexAccessor = mock( IndexAccessor.class );
        when( schemaIndexProvider.getOnlineAccessor( yetAnotherIndexRuleId, indexConfig, samplingConfig )).thenReturn( yetAnotherIndexAccessor );
        when( accessor.newReader() ).thenReturn( indexReader );
        IndexReader anotherIndexReader = mock( IndexReader.class );
        when( anotherAccessor.newReader() ).thenReturn( anotherIndexReader );
        IndexReader otherIndexReader = mock( IndexReader.class );
        when( otherAccessor.newReader() ).thenReturn( otherIndexReader );
        IndexReader yetAnotherIndexReader = mock( IndexReader.class );
        when( yetAnotherIndexAccessor.newReader() ).thenReturn( yetAnotherIndexReader );

        when( indexReader.valueTypesInIndex() ).thenReturn( asSet( String.class, Boolean.class ) );
        when( anotherIndexReader.valueTypesInIndex() ).thenReturn( asSet( Number.class, Array.class) );
        when( otherIndexReader.valueTypesInIndex() ).thenReturn( asSet(String.class ) );
        when( yetAnotherIndexReader.valueTypesInIndex() ).thenReturn( asSet( Array.class ) );

        // when
        migrator.migrate( storeDir, migrationDir, schemaIndexProvider, pageCache );

        // then
        verify( fs, never() ).deleteRecursively( new File( getRootDirectory( storeDir, schemaIndexProvider.getProviderDescriptor().getKey() ), "" + indexRuleId ) );
        verify( fs ).deleteRecursively( new File( getRootDirectory( storeDir, schemaIndexProvider.getProviderDescriptor().getKey() ), "" + anotherIndexRuleId ) );
        verify( fs, never() ).deleteRecursively( new File( getRootDirectory( storeDir, schemaIndexProvider.getProviderDescriptor().getKey() ), "" + otherIndexRuleId ) );
        verify( fs ).deleteRecursively( new File( getRootDirectory( storeDir, schemaIndexProvider.getProviderDescriptor().getKey() ), "" + yetAnotherIndexRuleId ) );
    }

    private Set<Class> asSet( Class... classes)
    {
        return new HashSet<>( Arrays.asList( classes ) );
    }

    private SchemaRule schemaRule( int id, int labelId, int propertyKeyId )
    {
        return IndexRule.indexRule( id, labelId, propertyKeyId, schemaIndexProvider.getProviderDescriptor() );
    }

    @Before
    public void setup() throws IOException
    {
        when( schemaIndexProvider.getProviderDescriptor() ).thenReturn( new SchemaIndexProvider.Descriptor( "key", "version" ) );
        when( schemaStoreProvider.provide( storeDir, pageCache ) ).thenReturn( schemaStore );
        Iterator<SchemaRule> iterator = Arrays.asList( schemaRule( indexRuleId, 42, 21 ) ).iterator();
        when( schemaStore.loadAllSchemaRules() ).thenReturn( iterator );
        when( schemaIndexProvider.getOnlineAccessor( indexRuleId, indexConfig, samplingConfig )).thenReturn( accessor );
        when( accessor.newReader() ).thenReturn( indexReader );
    }

    private final FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
    private final UpgradableDatabase upgradableDatabase = mock( UpgradableDatabase.class );
    private final SchemaIndexProvider schemaIndexProvider = mock( SchemaIndexProvider.class );
    private final SchemaStoreProvider schemaStoreProvider = mock( SchemaStoreProvider.class );
    private final PageCache pageCache = mock( PageCache.class );
    private final SchemaIndexMigrator migrator = new SchemaIndexMigrator( fs, upgradableDatabase, schemaStoreProvider );
    private final SchemaStore schemaStore = mock( SchemaStore.class );
    private final IndexAccessor accessor = mock( IndexAccessor.class );
    private final IndexReader indexReader = mock( IndexReader.class );
    private final File storeDir = new File( "store" );
    private final File migrationDir = new File( "migration" );
    private final IndexConfiguration indexConfig = new IndexConfiguration( false );
    private final IndexSamplingConfig samplingConfig = new IndexSamplingConfig( new Config() );
    private final int indexRuleId = 0;
}
