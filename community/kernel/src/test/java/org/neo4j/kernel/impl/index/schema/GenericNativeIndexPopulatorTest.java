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
package org.neo4j.kernel.impl.index.schema;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.gis.spatial.index.curves.SpaceFillingCurveConfiguration;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettingsCache;
import org.neo4j.storageengine.api.schema.IndexDescriptorFactory;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.test.rule.PageCacheAndDependenciesRule;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.kernel.api.index.IndexProvider.Monitor.EMPTY;

public class GenericNativeIndexPopulatorTest
{
    @Rule
    public final PageCacheAndDependenciesRule storage = new PageCacheAndDependenciesRule().with( new DefaultFileSystemRule() );

    @Test
    public void dropShouldDeleteEntireIndexFolder()
    {
        // given
        File root = storage.directory().directory( "root" );
        IndexDirectoryStructure directoryStructure = IndexDirectoryStructure.directoriesByProvider( root ).forProvider( GenericNativeIndexProvider.DESCRIPTOR );
        long indexId = 8;
        File indexDirectory = directoryStructure.directoryForIndex( indexId );
        StoreIndexDescriptor descriptor = IndexDescriptorFactory.forSchema( SchemaDescriptorFactory.forLabel( 1, 1 ) ).withId( indexId );
        IndexSpecificSpaceFillingCurveSettingsCache spatialSettings = mock( IndexSpecificSpaceFillingCurveSettingsCache.class );
        PageCache pageCache = storage.pageCache();
        FileSystemAbstraction fs = storage.fileSystem();
        File indexFile = new File( indexDirectory, "my-index" );
        GenericLayout layout = new GenericLayout( 1, spatialSettings );
        RecoveryCleanupWorkCollector immediate = immediate();
        IndexDropAction dropAction = new FileSystemIndexDropAction( fs, directoryStructure );
        GenericNativeIndexPopulator populator = new GenericNativeIndexPopulator( pageCache, fs, indexFile, layout,
                EMPTY, descriptor, spatialSettings, directoryStructure, mock( SpaceFillingCurveConfiguration.class ), dropAction, false );
        populator.create();

        // when
        assertTrue( fs.listFiles( indexDirectory ).length > 0 );
        populator.drop();

        // then
        assertFalse( fs.fileExists( indexDirectory ) );
    }
}
