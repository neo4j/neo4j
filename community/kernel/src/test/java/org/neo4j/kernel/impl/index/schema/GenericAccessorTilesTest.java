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
package org.neo4j.kernel.impl.index.schema;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.gis.spatial.index.curves.StandardConfiguration;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.values.storable.PointArray;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.kernel.api.schema.SchemaTestUtil.SIMPLE_NAME_LOOKUP;
import static org.neo4j.kernel.impl.index.schema.GenericNativeIndexProvider.DESCRIPTOR;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS_84;

class GenericAccessorTilesTest extends BaseAccessorTilesTest<BtreeKey>
{
    /**
     * This test verify that we correctly handle unique point arrays where every point in every array belong to the same tile on the space filling curve.
     * We verify this by asserting that we always get exactly one hit on an exact match and that the value is what we expect.
     */
    @Test
    void mustHandlePointArraysWithinSameTile() throws IndexEntryConflictException, IndexNotApplicableKernelException
    {
        // given
        // Many random points that all are close enough to each other to belong to the same tile on the space filling curve.
        int nbrOfValues = 10000;
        PointValue origin = Values.pointValue( WGS_84, 0.0, 0.0 );
        Long derivedValueForCenterPoint = curve.derivedValueFor( origin.coordinate() );
        double[] centerPoint = curve.centerPointFor( derivedValueForCenterPoint );
        double xWidthMultiplier = curve.getTileWidth( 0, curve.getMaxLevel() ) / 2;
        double yWidthMultiplier = curve.getTileWidth( 1, curve.getMaxLevel() ) / 2;

        List<Value> pointArrays = new ArrayList<>();
        List<IndexEntryUpdate<IndexDescriptor>> updates = new ArrayList<>();
        for ( int i = 0; i < nbrOfValues; i++ )
        {
            int arrayLength = random.nextInt( 1, 6 );
            PointValue[] pointValues = new PointValue[arrayLength];
            for ( int j = 0; j < arrayLength; j++ )
            {
                double x = (random.nextDouble() * 2 - 1) * xWidthMultiplier;
                double y = (random.nextDouble() * 2 - 1) * yWidthMultiplier;
                PointValue value = Values.pointValue( WGS_84, centerPoint[0] + x, centerPoint[1] + y );

                assertDerivedValue( derivedValueForCenterPoint, value );
                pointValues[j] = value;
            }
            PointArray array = Values.pointArray( pointValues );
            pointArrays.add( array );
            updates.add( IndexEntryUpdate.add( i, descriptor, array ) );
        }

        processAll( updates );

        // then
        exactMatchOnAllValues( pointArrays );
    }

    @Override
    IndexDescriptor createDescriptor()
    {
        return TestIndexDescriptorFactory.forLabel( 1, 1 );
    }

    @Override
    NativeIndexAccessor<BtreeKey> createAccessor()
    {
        IndexDirectoryStructure directoryStructure = IndexDirectoryStructure.directoriesByProvider( directory.homePath() ).forProvider( DESCRIPTOR );
        IndexFiles indexFiles = new IndexFiles.Directory( fs, directoryStructure, descriptor.getId() );
        GenericLayout layout = new GenericLayout( 1, indexSettings );
        RecoveryCleanupWorkCollector collector = RecoveryCleanupWorkCollector.ignore();
        DatabaseIndexContext databaseIndexContext = DatabaseIndexContext.builder( pageCache, fs, contextFactory, DEFAULT_DATABASE_NAME ).build();
        StandardConfiguration configuration = new StandardConfiguration();
        return new GenericNativeIndexAccessor( databaseIndexContext, indexFiles, layout, collector, descriptor, indexSettings, configuration,
                SIMPLE_NAME_LOOKUP );
    }
}
