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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;

import org.neo4j.gis.spatial.index.curves.SpaceFillingCurveConfiguration;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettingsCache;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettingsWriter;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;

import static org.neo4j.kernel.impl.index.schema.NativeIndexes.deleteIndex;

class GenericNativeIndexPopulator extends NativeIndexPopulator<GenericKey,NativeIndexValue>
{
    private final IndexSpecificSpaceFillingCurveSettingsCache spatialSettings;
    private final IndexDirectoryStructure directoryStructure;
    private final SpaceFillingCurveConfiguration configuration;
    private final boolean archiveFailedIndex;
    private final boolean temporary;

    GenericNativeIndexPopulator( PageCache pageCache, FileSystemAbstraction fs, File storeFile, IndexLayout<GenericKey,NativeIndexValue> layout,
            IndexProvider.Monitor monitor, StoreIndexDescriptor descriptor, IndexSpecificSpaceFillingCurveSettingsCache spatialSettings,
            IndexDirectoryStructure directoryStructure, SpaceFillingCurveConfiguration configuration, boolean archiveFailedIndex, boolean temporary )
    {
        super( pageCache, fs, storeFile, layout, monitor, descriptor, new SpaceFillingCurveSettingsWriter( spatialSettings ),
                temporary ? new OpenOption[] {StandardOpenOption.DELETE_ON_CLOSE} : new OpenOption[0] );
        this.spatialSettings = spatialSettings;
        this.directoryStructure = directoryStructure;
        this.configuration = configuration;
        this.archiveFailedIndex = archiveFailedIndex;
        this.temporary = temporary;
    }

    @Override
    public void create()
    {
        try
        {
            // Archive and delete the index, if it exists. The reason why this isn't done in the generic implementation is that for all other cases a
            // native index populator lives under a fusion umbrella and the archive function sits on the top-level fusion folder, not every single sub-folder.
            if ( !temporary )
            {
                deleteIndex( fileSystem, directoryStructure, descriptor.getId(), archiveFailedIndex );
            }

            // Now move on to do the actual creation.
            super.create();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    NativeIndexReader<GenericKey,NativeIndexValue> newReader()
    {
        return new GenericNativeIndexReader( tree, layout, descriptor, spatialSettings, configuration );
    }
}
