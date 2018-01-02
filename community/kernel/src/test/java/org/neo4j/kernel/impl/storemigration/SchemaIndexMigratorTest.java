/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.kernel.impl.storemigration.legacystore.v21.Legacy21Store;

import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.index.SchemaIndexProvider.getRootDirectory;

public class SchemaIndexMigratorTest
{
    private final FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
    private final SchemaIndexProvider schemaIndexProvider = mock( SchemaIndexProvider.class );
    private final StoreFactory storeFactory = mock( StoreFactory.class );
    private final NeoStores neoStores = mock( NeoStores.class );
    private final PageCache pageCache = mock( PageCache.class );
    private final SchemaIndexMigrator migrator = new SchemaIndexMigrator( fs, pageCache, storeFactory );
    private final SchemaStore schemaStore = mock( SchemaStore.class );
    private final IndexAccessor accessor = mock( IndexAccessor.class );
    private final IndexReader indexReader = mock( IndexReader.class );
    private final File storeDir = new File( "store" );
    private final File migrationDir = new File( "migration" );
    private final IndexConfiguration indexConfig = new IndexConfiguration( false );
    private final IndexSamplingConfig samplingConfig = new IndexSamplingConfig( new Config() );
    private final int indexRuleId = 0;

    @Before
    public void setup() throws IOException
    {
        when( schemaIndexProvider.getProviderDescriptor() ).thenReturn( new SchemaIndexProvider.Descriptor( "key", "version" ) );

        when( storeFactory.openNeoStores( NeoStores.StoreType.SCHEMA) ).thenReturn( neoStores );
        when( storeFactory.openAllNeoStores( anyBoolean() ) ).thenReturn( neoStores );
        when( neoStores.getSchemaStore() ).thenReturn( schemaStore );
        Iterator<SchemaRule> iterator = Arrays.asList( schemaRule( indexRuleId, 42, 21 ) ).iterator();
        when( schemaStore.loadAllSchemaRules() ).thenReturn( iterator );
        when( schemaIndexProvider.getOnlineAccessor( indexRuleId, indexConfig, samplingConfig )).thenReturn( accessor );
        when( accessor.newReader() ).thenReturn( indexReader );
    }

    @Test
    public void shouldDeleteTheIndexIfItContainsIndexedArrayValues() throws Exception
    {
        // given
        when( indexReader.valueTypesInIndex() ).thenReturn( asSet( String.class, Array.class ) );

        // when
        migrator.migrate( storeDir, migrationDir, schemaIndexProvider, Legacy21Store.LEGACY_VERSION );

        // then
        verify( fs ).deleteRecursively( new File( getRootDirectory( storeDir, schemaIndexProvider.getProviderDescriptor().getKey() ), "" + indexRuleId ) );
    }

    @Test
    public void shouldNotDeleteTheIndexIfItDoesNotContainIndexedArrayValues() throws Exception
    {
        // given
        when( indexReader.valueTypesInIndex() ).thenReturn( asSet( String.class, Boolean.class ) );

        // when
        migrator.migrate( storeDir, migrationDir, schemaIndexProvider, Legacy21Store.LEGACY_VERSION );

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
        migrator.migrate( storeDir, migrationDir, schemaIndexProvider, Legacy21Store.LEGACY_VERSION );

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


}
