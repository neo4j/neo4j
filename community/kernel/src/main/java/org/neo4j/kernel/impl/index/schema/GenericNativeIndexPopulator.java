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

import java.util.HashMap;
import java.util.Map;

import org.neo4j.gis.spatial.index.curves.SpaceFillingCurveConfiguration;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;
import org.neo4j.values.storable.Value;

import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_WRITER;

public class GenericNativeIndexPopulator extends NativeIndexPopulator<GenericKey,NativeIndexValue>
{
    private final IndexSpecificSpaceFillingCurveSettings spatialSettings;
    private final SpaceFillingCurveConfiguration configuration;
    private final boolean archiveFailedIndex;

    GenericNativeIndexPopulator( PageCache pageCache, FileSystemAbstraction fs, IndexFiles indexFiles, IndexLayout<GenericKey,NativeIndexValue> layout,
            IndexProvider.Monitor monitor, IndexDescriptor descriptor, IndexSpecificSpaceFillingCurveSettings spatialSettings,
            SpaceFillingCurveConfiguration configuration, boolean archiveFailedIndex )
    {
        super( pageCache, fs, indexFiles, layout, monitor, descriptor, NO_HEADER_WRITER );
        this.spatialSettings = spatialSettings;
        this.configuration = configuration;
        this.archiveFailedIndex = archiveFailedIndex;
    }

    @Override
    public void create()
    {
        // Archive index if it exists. The reason why this isn't done in the generic implementation is that for all other cases a
        // native index populator lives under a fusion umbrella and the archive function sits on the top-level fusion folder, not every single sub-folder.
        if ( archiveFailedIndex )
        {
            indexFiles.archiveIndex();
        }
        indexFiles.clear();

        // Now move on to do the actual creation.
        super.create();
    }

    @Override
    NativeIndexReader<GenericKey,NativeIndexValue> newReader()
    {
        return new GenericNativeIndexReader( tree, layout, descriptor, spatialSettings, configuration );
    }

    @Override
    public Map<String,Value> indexConfig()
    {
        Map<String,Value> map = new HashMap<>();
        spatialSettings.visitIndexSpecificSettings( new SpatialConfigVisitor( map ) );
        return map;
    }
}
