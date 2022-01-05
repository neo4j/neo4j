/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.storemigration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.common.ProgressReporter;
import org.neo4j.internal.batchimport.IndexImporterFactory;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.context.EmptyVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.impl.index.SchemaIndexMigrator;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.format.CapabilityType;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;

@TestDirectoryExtension
class SchemaIndexMigratorTest
{
    @Inject
    private TestDirectory testDirectory;

    private final FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
    private final ProgressReporter progressReporter = mock( ProgressReporter.class );
    private final IndexProvider indexProvider = mock( IndexProvider.class );
    private final PageCache pageCache = mock( PageCache.class );
    private DatabaseLayout databaseLayout;
    private DatabaseLayout migrationLayout;

    @BeforeEach
    void setup()
    {
        databaseLayout = Neo4jLayout.of( testDirectory.directory( "store" ) ).databaseLayout( DEFAULT_DATABASE_NAME );
        migrationLayout = Neo4jLayout.of( testDirectory.directory( "migrationDir" ) ).databaseLayout( DEFAULT_DATABASE_NAME );
    }

    @Test
    void schemaAndLabelIndexesRemovedAfterSuccessfulMigration() throws IOException
    {
        StorageEngineFactory storageEngineFactory = mock( StorageEngineFactory.class );
        StoreVersion version = mock( StoreVersion.class );
        when( version.hasCompatibleCapabilities( any(), eq( CapabilityType.INDEX ) ) ).thenReturn( false );
        when( storageEngineFactory.versionInformation( anyString() ) ).thenReturn( version );
        IndexImporterFactory indexImporterFactory = mock( IndexImporterFactory.class );
        IndexDirectoryStructure directoryStructure = mock( IndexDirectoryStructure.class );
        Path indexProviderRootDirectory = databaseLayout.file( "just-some-directory" );
        when( directoryStructure.rootDirectory() ).thenReturn( indexProviderRootDirectory );
        var contextFactory = new CursorContextFactory( PageCacheTracer.NULL, EmptyVersionContextSupplier.EMPTY );
        SchemaIndexMigrator migrator = new SchemaIndexMigrator( "Test migrator", fs, pageCache, directoryStructure, storageEngineFactory, true,
                contextFactory );
        when( indexProvider.getProviderDescriptor() )
                .thenReturn( new IndexProviderDescriptor( "key", "version" ) );

        migrator.migrate( databaseLayout, migrationLayout, progressReporter, "from", "to", indexImporterFactory );
        migrator.moveMigratedFiles( migrationLayout, databaseLayout, "from", "to" );

        verify( fs ).deleteRecursively( indexProviderRootDirectory );
    }

    @Test
    void shouldDeleteRelationshipIndexesAfterCrossFormatFamilyMigration() throws IOException
    {
        // given
        IndexProviderDescriptor provider = new IndexProviderDescriptor( "k", "v" );
        IndexDirectoryStructure directoryStructure = directoriesByProvider( databaseLayout.databaseDirectory() ).forProvider( provider );
        StorageEngineFactory storageEngineFactory = mock( StorageEngineFactory.class );
        StoreVersion fromVersion = mock( StoreVersion.class );
        StoreVersion toVersion = mock( StoreVersion.class );
        when( fromVersion.hasCompatibleCapabilities( toVersion, CapabilityType.FORMAT ) ).thenReturn( false );
        when( storageEngineFactory.versionInformation( "from" ) ).thenReturn( fromVersion );
        when( storageEngineFactory.versionInformation( "to" ) ).thenReturn( toVersion );
        List<SchemaRule> schemaRules = new ArrayList<>();
        schemaRules.add( forSchema( SchemaDescriptors.forLabel( 1, 2, 3 ) ).withName( "n1" ).materialise( 1L ) );
        schemaRules.add( forSchema( SchemaDescriptors.forRelType( 5, 3 ) ).withName( "r1" ).materialise( 2L ) );
        schemaRules.add( forSchema( SchemaDescriptors.fulltext( RELATIONSHIP, new int[]{1, 2, 3}, new int[]{4, 5, 6} ) ).withName( "r2" ).materialise( 3L ) );
        schemaRules.add( forSchema( SchemaDescriptors.fulltext( NODE, new int[]{1, 2, 3}, new int[]{4, 5, 6} ) ).withName( "n2" ).materialise( 4L ) );
        when( storageEngineFactory.loadSchemaRules( any(), any(), any(), any(), anyBoolean(), any(), any(), any() ) ).thenReturn( schemaRules );
        var contextFactory = new CursorContextFactory( PageCacheTracer.NULL, EmptyVersionContextSupplier.EMPTY );
        SchemaIndexMigrator migrator = new SchemaIndexMigrator( "Test migrator", fs, pageCache, directoryStructure, storageEngineFactory, false,
                contextFactory );

        // when
        migrator.migrate( databaseLayout, migrationLayout, progressReporter, "from", "to", IndexImporterFactory.EMPTY );
        migrator.moveMigratedFiles( databaseLayout, migrationLayout, "from", "to" );

        // then
        verify( fs, never() ).deleteRecursively( directoryStructure.directoryForIndex( 1L ) );
        verify( fs ).deleteRecursively( directoryStructure.directoryForIndex( 2L ) );
        verify( fs ).deleteRecursively( directoryStructure.directoryForIndex( 3L ) );
        verify( fs, never() ).deleteRecursively( directoryStructure.directoryForIndex( 4L ) );
    }
}
