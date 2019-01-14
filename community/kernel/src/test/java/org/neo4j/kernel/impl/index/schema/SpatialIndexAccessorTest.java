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

import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.gis.spatial.index.curves.StandardConfiguration;
import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.schema.config.ConfiguredSpaceFillingCurveSettingsCache;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;

public class SpatialIndexAccessorTest extends NativeIndexAccessorTests<SpatialIndexKey,NativeIndexValue>
{
    private static final CoordinateReferenceSystem crs = CoordinateReferenceSystem.WGS84;
    private static final ConfiguredSpaceFillingCurveSettingsCache configuredSettings = new ConfiguredSpaceFillingCurveSettingsCache( Config.defaults() );

    private SpatialIndexFiles.SpatialFile spatialFile;

    @Override
    NativeIndexAccessor<SpatialIndexKey,NativeIndexValue> makeAccessor() throws IOException
    {
        spatialFile = new SpatialIndexFiles.SpatialFile( CoordinateReferenceSystem.WGS84, configuredSettings, super.getIndexFile() );
        return new SpatialIndexAccessor.PartAccessor( pageCache, fs, spatialFile.getLayoutForNewIndex(), immediate(), monitor, indexDescriptor,
                new StandardConfiguration() );
    }

    @Override
    IndexCapability indexCapability()
    {
        return SpatialIndexProvider.CAPABILITY;
    }

    @Override
    protected ValueCreatorUtil<SpatialIndexKey,NativeIndexValue> createValueCreatorUtil()
    {
        return new SpatialValueCreatorUtil( TestIndexDescriptorFactory.forLabel( 42, 666 ).withId( 0 ), ValueCreatorUtil.FRACTION_DUPLICATE_NON_UNIQUE );
    }

    @Override
    IndexLayout<SpatialIndexKey,NativeIndexValue> createLayout()
    {
        return new SpatialLayout( crs, configuredSettings.forCRS( crs ).curve() );
    }

    @Override
    public File getIndexFile()
    {
        return spatialFile.indexFile;
    }

    @Override
    public void shouldNotSeeFilteredEntries()
    {
        // This test doesn't make sense for spatial, since it needs a proper store for the values
    }

    @Test
    public void shouldReturnMatchingEntriesForRangePredicateWithInclusiveStartAndInclusiveEnd() throws Exception
    {
        // given
        IndexEntryUpdate<IndexDescriptor>[] updates = valueCreatorUtil.generateAddUpdatesFor( new Value[]{
                Values.pointValue( WGS84, -90, -90 ),
                Values.pointValue( WGS84, -70, -70 ),
                Values.pointValue( WGS84, -50, -50 ),
                Values.pointValue( WGS84, 0, 0 ),
                Values.pointValue( WGS84, 50, 50 ),
                Values.pointValue( WGS84, 70, 70 ),
                Values.pointValue( WGS84, 90, 90 )
        } );
        shouldReturnMatchingEntriesForRangePredicateWithInclusiveStartAndInclusiveEnd( updates );
    }

    @Override
    public void shouldReturnMatchingEntriesForRangePredicateWithExclusiveStartAndExclusiveEnd()
    {
        // Exclusive is handled via a postfilter for spatial
    }

    @Override
    public void shouldReturnMatchingEntriesForRangePredicateWithExclusiveStartAndInclusiveEnd()
    {
        // Exclusive is handled via a postfilter for spatial
    }

    @Override
    public void shouldReturnMatchingEntriesForRangePredicateWithInclusiveStartAndExclusiveEnd()
    {
        // Exclusive is handled via a postfilter for spatial
    }

    @Override
    public void mustHandleNestedQueries() throws IndexEntryConflictException, IndexNotApplicableKernelException
    {
        // It ok to not use random values here because we are only testing nesting of queries
        //noinspection unchecked
        IndexEntryUpdate<IndexDescriptor>[] updates = valueCreatorUtil.generateAddUpdatesFor( new Value[]{
                Values.pointValue( WGS84, -90, -90 ),
                Values.pointValue( WGS84, -70, -70 ),
                Values.pointValue( WGS84, -50, -50 ),
                Values.pointValue( WGS84, 0, 0 ),
                Values.pointValue( WGS84, 50, 50 ),
                Values.pointValue( WGS84, 70, 70 ),
                Values.pointValue( WGS84, 90, 90 )
        } );
        mustHandleNestedQueries( updates );
    }

    @Override
    public void mustHandleMultipleNestedQueries() throws Exception
    {
        // It ok to not use random values here because we are only testing nesting of queries
        //noinspection unchecked
        IndexEntryUpdate<IndexDescriptor>[] updates = valueCreatorUtil.generateAddUpdatesFor( new Value[]{
                Values.pointValue( WGS84, -90, -90 ),
                Values.pointValue( WGS84, -70, -70 ),
                Values.pointValue( WGS84, -50, -50 ),
                Values.pointValue( WGS84, 0, 0 ),
                Values.pointValue( WGS84, 50, 50 ),
                Values.pointValue( WGS84, 70, 70 ),
                Values.pointValue( WGS84, 90, 90 )
        } );
        mustHandleMultipleNestedQueries( updates );
    }

    @Override
    public void shouldReturnNoEntriesForRangePredicateOutsideAnyMatch()
    {
        // Accidental hits outside range is handled via a postfilter for spatial
    }

    @Override
    public void respectIndexOrder()
    {   // Spatial is non-orderable so test does not make sense
    }
}
