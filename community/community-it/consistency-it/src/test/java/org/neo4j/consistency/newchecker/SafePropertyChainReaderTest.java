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

import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.function.Consumer;

import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.DynamicConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.NodeConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.PrimitiveConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.PropertyConsistencyReport;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Value;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.reset;
import static org.neo4j.internal.helpers.collection.Iterables.single;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.pointValue;
import static org.neo4j.values.storable.Values.stringValue;

class SafePropertyChainReaderTest extends CheckerTestBase
{
    private int propertyKey1;
    private int propertyKey2;
    private int propertyKey3;

    @Override
    void initialData( KernelTransaction tx ) throws KernelException
    {
        TokenWrite tokenWrite = tx.tokenWrite();
        propertyKey1 = tokenWrite.propertyKeyGetOrCreateForName( "1" );
        propertyKey2 = tokenWrite.propertyKeyGetOrCreateForName( "2" );
        propertyKey3 = tokenWrite.propertyKeyGetOrCreateForName( "3" );
    }

    @Test
    void shouldReportCircularPropertyRecordChain() throws Exception
    {
        // given
        long nodeId1;
        long nodeId2;
        try ( AutoCloseable ignored = tx() )
        {
            // (N1)────>(P1)────>(P2)
            //            ▲───────┘
            {
                long firstPropId = propertyStore.nextId();
                long secondPropId = propertyStore.nextId();
                nodeId1 = node( nodeStore.nextId(), firstPropId, NULL );
                property( firstPropId, NULL, secondPropId, propertyValue( propertyKey1, intValue( 1 ) ) );
                property( secondPropId, firstPropId, firstPropId, propertyValue( propertyKey2, intValue( 1 ) ) );
            }

            // (N2)────>(P3)─┐
            //            ▲──┘
            {
                nodeId2 = nodeStore.nextId();
                long propId = propertyStore.nextId();
                node( nodeId2, propId, NULL );
                property( propId, NULL, propId, propertyValue( propertyKey1, intValue( 1 ) ) );
            }
        }

        // when/then
        checkNode( nodeId1 );
        expect( NodeConsistencyReport.class, report -> report.propertyChainContainsCircularReference( any() ) );
        reset( monitor );

        checkNode( nodeId2 );
        expect( NodeConsistencyReport.class, report -> report.propertyChainContainsCircularReference( any() ) );
    }

    @Test
    void shouldReportNextPropertyRecordNotInUse() throws Exception
    {
        // given
        long nodeId1;
        long nodeId2;
        try ( AutoCloseable ignored = tx() )
        {
            // (N)---> X
            {
                nodeId1 = node( nodeStore.nextId(), propertyStore.nextId(), NULL );
            }

            // (N)--->(P1)---> X
            {
                long propId1 = propertyStore.nextId();
                long propId2 = propertyStore.nextId();
                nodeId2 = node( nodeStore.nextId(), propId1, NULL );
                property( propId1, NULL, propId2, propertyValue( propertyKey1, longValue( 10 ) ) );
            }
        }

        // when/then
        checkNode( nodeId1 );
        expect( NodeConsistencyReport.class, report -> report.propertyNotInUse( any() ) );
        expect( PropertyConsistencyReport.class, report -> report.nextNotInUse( any() ) );
        reset( monitor );

        checkNode( nodeId2 );
        expect( NodeConsistencyReport.class, report -> report.propertyNotInUse( any() ) );
        expect( PropertyConsistencyReport.class, report -> report.nextNotInUse( any() ) );
    }

    @Test
    void shouldReportPropertyNotFirstInChain() throws Exception
    {
        // given
        long nodeId;
        try ( AutoCloseable ignored = tx() )
        {
            // (N)--->(P1)--->(P2)
            //        /
            //       v
            //     (P0)
            long prop0Id = propertyStore.nextId();
            long prop1Id = propertyStore.nextId();
            long prop2Id = propertyStore.nextId();
            nodeId = node( nodeStore.nextId(), prop1Id, NULL );
            property( prop0Id, NULL, prop1Id, propertyValue( propertyKey1, stringValue( "a" ) ) );
            property( prop1Id, prop0Id, prop2Id, propertyValue( propertyKey2, stringValue( "b" ) ) );
            property( prop2Id, prop1Id, NULL, propertyValue( propertyKey3, stringValue( "c" ) ) );
        }

        // when
        checkNode( nodeId );

        // then
        expect( NodeConsistencyReport.class, report -> report.propertyNotFirstInChain( any() ) );
    }

    @Test
    void shouldReportNextDoesNotReferenceBack() throws Exception
    {
        // given
        long nodeId;
        try ( AutoCloseable ignored = tx() )
        {
            // (N)--->(P1)-next->(P2)
            //                    /
            //                  prev
            //                  v
            //                (P3)
            long prop1Id = propertyStore.nextId();
            long prop2Id = propertyStore.nextId();
            long prop3Id = propertyStore.nextId();
            nodeId = node( nodeStore.nextId(), prop1Id, NULL );
            property( prop1Id, NULL, prop2Id, propertyValue( propertyKey1, stringValue( "a" ) ) );
            property( prop2Id, prop3Id, NULL, propertyValue( propertyKey2, stringValue( "b" ) ) );
            property( prop3Id, NULL, NULL, propertyValue( propertyKey3, stringValue( "c" ) ) );
        }

        // when
        checkNode( nodeId );

        // then
        expect( PropertyConsistencyReport.class, report -> report.nextDoesNotReferenceBack( any() ) );
    }

    @Test
    void shouldReportKeyNotInUse() throws Exception
    {
        // given
        long nodeId;
        try ( AutoCloseable ignored = tx() )
        {
            // (N)--->(P)*unusedKey
            long propId = propertyStore.nextId();
            nodeId = node( nodeStore.nextId(), propId, NULL );
            property( propId, NULL, NULL, propertyValue( 99, intValue( 1 ) ) );
        }

        // when
        checkNode( nodeId );

        // then
        expect( PropertyConsistencyReport.class, report -> report.keyNotInUse( any(), any() ) );
    }

    @Test
    void shouldReportStringRecordNotInUse() throws Exception
    {
        testPropertyValueInconsistency( stringValueOfLength( 60 ), block -> single( block.getValueRecords() ).setInUse( false ),
                PropertyConsistencyReport.class, report -> report.stringNotInUse( any(), any() ) );
    }

    @Test
    void shouldReportNextStringRecordNotInUse() throws Exception
    {
        testPropertyValueInconsistency( stringValueOfLength( 160 ), block -> block.getValueRecords().get( 1 ).setInUse( false ),
                DynamicConsistencyReport.class, report -> report.nextNotInUse( any() ) );
    }

    @Test
    void shouldReportDynamicStringEmpty() throws Exception
    {
        testPropertyValueInconsistency( stringValueOfLength( 60 ), block -> single( block.getValueRecords() ).setData( new byte[0] ),
                PropertyConsistencyReport.class, report -> report.stringEmpty( any(), any() ) );
    }

    @Test
    void shouldReportDynamicStringRecordNotFullReferencesNext() throws Exception
    {
        testPropertyValueInconsistency( stringValueOfLength( 160 ),
                block ->
                {
                    byte[] data = block.getValueRecords().get( 0 ).getData();
                    block.getValueRecords().get( 0 ).setData( Arrays.copyOf( data, data.length / 2 ) );
                },
                DynamicConsistencyReport.class, DynamicConsistencyReport::recordNotFullReferencesNext );
    }

    @Test
    void shouldReportArrayRecordNotInUse() throws Exception
    {
        testPropertyValueInconsistency( intArrayValueOfLength( 30 ), block -> single( block.getValueRecords() ).setInUse( false ),
                PropertyConsistencyReport.class, report -> report.arrayNotInUse( any(), any() ) );
    }

    @Test
    void shouldReportNextArrayRecordNotInUse() throws Exception
    {
        testPropertyValueInconsistency( intArrayValueOfLength( 80 ), block -> block.getValueRecords().get( 1 ).setInUse( false ),
                DynamicConsistencyReport.class, report -> report.nextNotInUse( any() ) );
    }

    @Test
    void shouldReportDynamicArrayEmpty() throws Exception
    {
        testPropertyValueInconsistency( intArrayValueOfLength( 30 ), block -> single( block.getValueRecords() ).setData( new byte[0] ),
                PropertyConsistencyReport.class, report -> report.arrayEmpty( any(), any() ) );
    }

    @Test
    void shouldReportDynamicArrayRecordNotFullReferencesNext() throws Exception
    {
        testPropertyValueInconsistency( intArrayValueOfLength( 80 ),
                block ->
                {
                    byte[] data = block.getValueRecords().get( 0 ).getData();
                    block.getValueRecords().get( 0 ).setData( Arrays.copyOf( data, data.length / 2 ) );
                },
                DynamicConsistencyReport.class, DynamicConsistencyReport::recordNotFullReferencesNext );
    }

    @Test
    void shouldReportInvalidPropertyValue() throws Exception
    {
        testPropertyValueInconsistency( pointValue( CoordinateReferenceSystem.WGS84, 12.34, 56.78 ),
                // This is very internal knowledge, but setting the 56th (floating point precision) bit in the first block of a point value
                // Makes the decoding of that property value throw exception
                block -> block.getValueBlocks()[0] |= 0x0100000000000000L,
                PropertyConsistencyReport.class, report -> report.invalidPropertyValue( anyLong(), anyInt() ) );
    }

    @Test
    void shouldReportInvalidPropertyValueForDynamicArrayValue() throws Exception
    {
        // The format of packed number array in dynamic record doesn't include array length in header,
        // therefore this doesn't work on e.g. number arrays
        testPropertyValueInconsistency( stringArrayValueOfLength( 20, 50 ), block ->
        {
            assertTrue( block.getValueRecords().size() > 2 );
            block.getValueRecords().get( 1 ).setNextBlock( NULL );
        }, PropertyConsistencyReport.class, report -> report.invalidPropertyValue( anyLong(), anyInt() ) );
    }

    private <T extends ConsistencyReport> void testPropertyValueInconsistency( Value consistentValue, Consumer<PropertyBlock> vandal,
            Class<T> expectedReportClass, Consumer<T> report ) throws Exception
    {
        // given
        long nodeId;
        try ( AutoCloseable ignored = tx() )
        {
            // (N)--->(P)---> (vandalized dynamic value chain)
            long propId = propertyStore.nextId();
            nodeId = node( nodeStore.nextId(), propId, NULL );
            PropertyBlock dynamicBlock = propertyValue( propertyKey1, consistentValue );
            property( propId, NULL, NULL, dynamicBlock );
            vandal.accept( dynamicBlock );
            property( propId, NULL, NULL, dynamicBlock );
        }

        // when
        checkNode( nodeId );

        // then
        expect( expectedReportClass, report );
    }

    @Test
    void shouldReportPropertyKeyNotUniqueInChain() throws Exception
    {
        // given
        long nodeId1;
        long nodeId2;
        try ( AutoCloseable ignored = tx() )
        {
            // (N)--->(P)
            //         *key=value1
            //         *key=value2
            {
                long propId = propertyStore.nextId();
                property( propId, NULL, NULL, propertyValue( propertyKey1, intValue( 1 ) ), propertyValue( propertyKey1, intValue( 2 ) ) );
                nodeId1 = node( nodeStore.nextId(), propId, NULL );
            }

            // (N)--->(P1)--------->(P2)
            //         *key=value1   *key=value2
            {
                long propId1 = propertyStore.nextId();
                long propId2 = propertyStore.nextId();
                property( propId1, NULL, propId2, propertyValue( propertyKey1, intValue( 1 ) ) );
                property( propId2, propId1, NULL, propertyValue( propertyKey1, intValue( 2 ) ) );
                nodeId2 = node( nodeStore.nextId(), propId1, NULL );
            }
        }

        // when/then
        checkNode( nodeId1 );
        expect( NodeConsistencyReport.class, PrimitiveConsistencyReport::propertyKeyNotUniqueInChain );
        reset( monitor );

        // then
        checkNode( nodeId2 );
        expect( NodeConsistencyReport.class, PrimitiveConsistencyReport::propertyKeyNotUniqueInChain );
    }

//  shouldReportInvalidPropertyKey: impossible because keys cannot be loaded with a negative id
//  shouldReportInvalidPropertyType: impossible because property blocks w/ invalid type are skipped when loading
//  shouldReportDynamicStringRecordInvalidLength: impossible because DynamicRecordFormat will not load a record with invalid length
//  shouldReportIndexedMultipleTimes: could be possible if using a lucene index provider

    private void checkNode( long nodeId ) throws Exception
    {
        try ( SafePropertyChainReader checker = new SafePropertyChainReader( context() ) )
        {
            checkNode( checker, nodeId );
        }
    }

    private void checkNode( SafePropertyChainReader checker, long nodeId )
    {
        boolean chainOk = checker.read( new IntObjectHashMap<>(), loadNode( nodeId ), reporter::forNode );
        assertFalse( chainOk );
    }
}
