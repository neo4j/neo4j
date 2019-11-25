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
package org.neo4j.consistency.newchecker;

import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.junit.jupiter.api.Test;

import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.internal.schema.SchemaDescriptor.forLabel;
import static org.neo4j.kernel.impl.api.index.IndexUpdateMode.ONLINE;
import static org.neo4j.storageengine.api.IndexEntryUpdate.add;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.pointValue;
import static org.neo4j.values.storable.Values.stringValue;

class SchemaComplianceCheckerTest extends CheckerTestBase
{
    private int propertyKey1;
    private int propertyKey2;
    private int propertyKey3;
    private int label1;
    private int label2;
    private int label3;

    @Override
    void initialData( KernelTransaction tx ) throws KernelException
    {
        TokenWrite tokenWrite = tx.tokenWrite();
        propertyKey1 = tokenWrite.propertyKeyGetOrCreateForName( "1" );
        propertyKey2 = tokenWrite.propertyKeyGetOrCreateForName( "2" );
        propertyKey3 = tokenWrite.propertyKeyGetOrCreateForName( "3" );
        label1 = tokenWrite.labelGetOrCreateForName( "A" );
        label2 = tokenWrite.labelGetOrCreateForName( "B" );
        label3 = tokenWrite.labelGetOrCreateForName( "C" );
    }

    @Test
    void shouldReportMissingMandatoryProperty() throws Exception
    {
        // given
        long nodeId = 0;
        MutableIntObjectMap<Value> propertyValues = new IntObjectHashMap<>();
        propertyValues.put( propertyKey2, intValue( 99 ) );
        long[] labels = new long[]{label1, label3};
        MutableIntObjectMap<MutableIntSet> mandatoryProperties = IntObjectMaps.mutable.empty();
        mandatoryProperties.put( label1, IntSets.mutable.of( propertyKey1, propertyKey2 ) );
        mandatoryProperties.put( label2, IntSets.mutable.of( propertyKey1, propertyKey3 ) );
        mandatoryProperties.put( label3, IntSets.mutable.of( propertyKey1 ) );

        // when
        try ( SchemaComplianceChecker checker = new SchemaComplianceChecker( context(), mandatoryProperties, context().indexAccessors.onlineRules( NODE ) ) )
        {
            checker.checkContainsMandatoryProperties( new NodeRecord( nodeId ), labels, propertyValues, reporter::forNode );
        }

        // then
        expect( ConsistencyReport.NodeConsistencyReport.class, report -> report.missingMandatoryProperty( anyInt() ) );
    }

    @Test
    void shouldReportNotUniquelyIndexed() throws Exception
    {
        // given
        LabelSchemaDescriptor descriptor = forLabel( label1, propertyKey1 );
        long indexId = uniqueIndex( descriptor );
        long nodeId;
        try ( AutoCloseable ignored = tx() )
        {
            TextValue value = stringValue( "a" );
            // (N1) indexed w/ property A
            {
                long propId = propertyStore.nextId();
                nodeId = node( nodeStore.nextId(), propId, NULL, label1 );
                property( propId, NULL, NULL, propertyValue( propertyKey1, value ) );
                indexValue( descriptor, indexId, nodeId, value );
            }
            // (N2) indexed w/ property A
            {
                long propId = propertyStore.nextId();
                long nodeId2 = node( nodeStore.nextId(), propId, NULL, label1 );
                property( propId, NULL, NULL, propertyValue( propertyKey1, value ) );
                indexValue( descriptor, indexId, nodeId2, value );
            }
        }

        // when
        checkIndexed( nodeId );

        // then
        expect( ConsistencyReport.NodeConsistencyReport.class, report -> report.uniqueIndexNotUnique( any(), any(), anyLong() ) );
    }

    @Test
    void shouldReportNotIndexed() throws Exception
    {
        // given
        LabelSchemaDescriptor descriptor = forLabel( label1, propertyKey1 );
        index( descriptor );
        long nodeId;
        try ( AutoCloseable ignored = tx() )
        {
            // (N1) w/ property A (NOT indexed)
            long propId = propertyStore.nextId();
            nodeId = node( nodeStore.nextId(), propId, NULL, label1 );
            property( propId, NULL, NULL, propertyValue( propertyKey1, stringValue( "a" ) ) );
        }

        // when
        checkIndexed( nodeId );

        // then
        expect( ConsistencyReport.NodeConsistencyReport.class, report -> report.notIndexed( any(), any() ) );
    }

    @Test
    void shouldCheckIndexesWithLookupFiltering() throws Exception
    {
        // given
        LabelSchemaDescriptor descriptor = forLabel( label1, propertyKey1 );
        long indexId = uniqueIndex( descriptor );
        long nodeId;
        try ( AutoCloseable ignored = tx() )
        {
            PointValue value = pointValue( CoordinateReferenceSystem.WGS84, 2, 4 );

            // (N1) w/ property
            {
                long propId = propertyStore.nextId();
                nodeId = node( nodeStore.nextId(), propId, NULL, label1 );
                property( propId, NULL, NULL, propertyValue( propertyKey1, value ) );
                indexValue( descriptor, indexId, nodeId, value );
            }

            // (N2) w/ property
            {
                long propId = propertyStore.nextId();
                long nodeId2 = node( nodeStore.nextId(), propId, NULL, label1 );
                property( propId, NULL, NULL, propertyValue( propertyKey1, value ) );
                indexValue( descriptor, indexId, nodeId2, value );
            }
        }

        // when
        checkIndexed( nodeId );

        // then it should be successful
        expect( ConsistencyReport.NodeConsistencyReport.class, report -> report.uniqueIndexNotUnique( any(), any(), anyLong() ) );
    }

    private void indexValue( LabelSchemaDescriptor descriptor, long indexId, long nodeId, Value value )
            throws IndexNotFoundKernelException, IndexEntryConflictException
    {
        IndexingService indexingService = db.getDependencyResolver().resolveDependency( IndexingService.class );
        try ( IndexUpdater indexUpdater = indexingService.getIndexProxy( indexId ).newUpdater( ONLINE ) )
        {
            indexUpdater.process( add( nodeId, descriptor, value ) );
        }
    }

    private void checkIndexed( long nodeId ) throws Exception
    {
        try ( SchemaComplianceChecker checker = new SchemaComplianceChecker( context(), new IntObjectHashMap<>(),
                context().indexAccessors.onlineRules( NODE ) ) )
        {
            NodeRecord node = loadNode( nodeId );
            checker.checkCorrectlyIndexed( node, nodeLabels( node ), readPropertyValues( nodeId ), reporter::forNode );
        }
    }

    private MutableIntObjectMap<Value> readPropertyValues( long nodeId ) throws Exception
    {
        try ( SafePropertyChainReader reader = new SafePropertyChainReader( context().withoutReporting() ) )
        {
            NodeRecord node = loadNode( nodeId );
            MutableIntObjectMap<Value> values = new IntObjectHashMap<>();
            reader.read( values, node, reporter::forNode );
            return values;
        }
    }
}
