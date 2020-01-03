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
package org.neo4j.kernel.impl.index.schema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.common.Validator;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurveConfiguration;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexEntriesReader;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;
import org.neo4j.values.storable.Value;

import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_WRITER;

class GenericNativeIndexAccessor extends NativeIndexAccessor<GenericKey,NativeIndexValue>
{
    private final IndexSpecificSpaceFillingCurveSettings spaceFillingCurveSettings;
    private final SpaceFillingCurveConfiguration configuration;
    private Validator<Value[]> validator;

    GenericNativeIndexAccessor( PageCache pageCache, FileSystemAbstraction fs, IndexFiles indexFiles, IndexLayout<GenericKey,NativeIndexValue> layout,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, IndexProvider.Monitor monitor, IndexDescriptor descriptor,
            IndexSpecificSpaceFillingCurveSettings spaceFillingCurveSettings, SpaceFillingCurveConfiguration configuration,
            boolean readOnly )
    {
        super( pageCache, fs, indexFiles, layout, monitor, descriptor, NO_HEADER_WRITER, readOnly );
        this.spaceFillingCurveSettings = spaceFillingCurveSettings;
        this.configuration = configuration;
        instantiateTree( recoveryCleanupWorkCollector, headerWriter );
    }

    @Override
    protected void afterTreeInstantiation( GBPTree<GenericKey,NativeIndexValue> tree )
    {
        validator = new GenericIndexKeyValidator( tree.keyValueSizeCap(), layout );
    }

    @Override
    public IndexReader newReader()
    {
        assertOpen();
        return new GenericNativeIndexReader( tree, layout, descriptor, spaceFillingCurveSettings, configuration );
    }

    @Override
    public void validateBeforeCommit( Value[] tuple )
    {
        validator.validate( tuple );
    }

    @Override
    public void force( IOLimiter ioLimiter )
    {
        // This accessor needs to use the header writer here because coordinate reference systems may have changed since last checkpoint.
        tree.checkpoint( ioLimiter, headerWriter );
    }

    @Override
    public Map<String,Value> indexConfig()
    {
        Map<String,Value> map = new HashMap<>();
        spaceFillingCurveSettings.visitIndexSpecificSettings( new SpatialConfigVisitor( map ) );
        return map;
    }

    @Override
    public IndexEntriesReader[] newAllIndexEntriesReader( int partitions )
    {
        GenericKey lowest = layout.newKey();
        lowest.initialize( Long.MIN_VALUE );
        lowest.initValuesAsLowest();
        GenericKey highest = layout.newKey();
        highest.initialize( Long.MAX_VALUE );
        highest.initValuesAsHighest();
        try
        {
            Collection<Seeker<GenericKey,NativeIndexValue>> seekers = tree.partitionedSeek( lowest, highest, partitions );
            Collection<IndexEntriesReader> readers = new ArrayList<>();
            for ( Seeker<GenericKey,NativeIndexValue> seeker : seekers )
            {
                readers.add( new IndexEntriesReader()
                {
                    @Override
                    public long next()
                    {
                        return seeker.key().getEntityId();
                    }

                    @Override
                    public boolean hasNext()
                    {
                        try
                        {
                            return seeker.next();
                        }
                        catch ( IOException e )
                        {
                            throw new UncheckedIOException( e );
                        }
                    }

                    @Override
                    public Value[] values()
                    {
                        return seeker.key().asValues();
                    }

                    @Override
                    public void close()
                    {
                        try
                        {
                            seeker.close();
                        }
                        catch ( IOException e )
                        {
                            throw new UncheckedIOException( e );
                        }
                    }
                } );
            }
            return readers.toArray( new IndexEntriesReader[0] );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
