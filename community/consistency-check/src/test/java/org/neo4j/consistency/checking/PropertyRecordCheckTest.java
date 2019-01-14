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
package org.neo4j.consistency.checking;

import org.junit.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;

import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.kernel.impl.store.GeometryType;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.TemporalType;
import org.neo4j.kernel.impl.store.format.standard.StandardFormatSettings;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.values.storable.CoordinateReferenceSystem;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class PropertyRecordCheckTest
        extends RecordCheckTestBase<PropertyRecord, ConsistencyReport.PropertyConsistencyReport, PropertyRecordCheck>
{
    public PropertyRecordCheckTest()
    {
        super( new PropertyRecordCheck(), ConsistencyReport.PropertyConsistencyReport.class, new int[0] );
    }

    @Test
    public void shouldNotReportAnythingForPropertyRecordNotInUse()
    {
        // given
        PropertyRecord property = notInUse( new PropertyRecord( 42 ) );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldNotReportAnythingForPropertyWithoutBlocksThatDoesNotReferenceAnyOtherRecords()
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportPropertyKeyNotInUse()
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyKeyTokenRecord key = add( notInUse( new PropertyKeyTokenRecord( 0 ) ) );
        PropertyBlock block = propertyBlock( key, PropertyType.INT, 0 );
        property.addPropertyBlock( block );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );

        // then
        verify( report ).keyNotInUse( block, key );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportPreviousPropertyNotInUse()
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyRecord prev = add( notInUse( new PropertyRecord( 51 ) ) );
        property.setPrevProp( prev.getId() );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );

        // then
        verify( report ).prevNotInUse( prev );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportNextPropertyNotInUse()
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyRecord next = add( notInUse( new PropertyRecord( 51 ) ) );
        property.setNextProp( next.getId() );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );

        // then
        verify( report ).nextNotInUse( next );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportPreviousPropertyNotReferringBack()
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyRecord prev = add( inUse( new PropertyRecord( 51 ) ) );
        property.setPrevProp( prev.getId() );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );

        // then
        verify( report ).previousDoesNotReferenceBack( prev );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportNextPropertyNotReferringBack()
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyRecord next = add( inUse( new PropertyRecord( 51 ) ) );
        property.setNextProp( next.getId() );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );

        // then
        verify( report ).nextDoesNotReferenceBack( next );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportStringRecordNotInUse()
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyKeyTokenRecord key = add( inUse( new PropertyKeyTokenRecord( 6 ) ) );
        DynamicRecord value = add( notInUse( string( new DynamicRecord( 1001 ) ) ) );
        PropertyBlock block = propertyBlock( key, value );
        property.addPropertyBlock( block );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );
        // then
        verify( report ).stringNotInUse( block, value );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportArrayRecordNotInUse()
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyKeyTokenRecord key = add( inUse( new PropertyKeyTokenRecord( 6 ) ) );
        DynamicRecord value = add( notInUse( array( new DynamicRecord( 1001 ) ) ) );
        PropertyBlock block = propertyBlock( key, value );
        property.addPropertyBlock( block );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );

        // then
        verify( report ).arrayNotInUse( block, value );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportEmptyStringRecord()
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyKeyTokenRecord key = add( inUse( new PropertyKeyTokenRecord( 6 ) ) );
        DynamicRecord value = add( inUse( string( new DynamicRecord( 1001 ) ) ) );
        PropertyBlock block = propertyBlock( key, value );
        property.addPropertyBlock( block );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );

        // then
        verify( report ).stringEmpty( block, value );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportUnknownGTypeGeometryRecord()
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        final int keyId = 6;
        add( inUse( new PropertyKeyTokenRecord( keyId ) ) );
        final long[] longs = GeometryType.encodePoint( keyId, CoordinateReferenceSystem.WGS84, new double[] { 1.0, 2.0 } );
        // corrupt array
        long gtypeBits = 0xFL << StandardFormatSettings.PROPERTY_TOKEN_MAXIMUM_ID_BITS + 4;
        longs[0] |= gtypeBits;

        expectInvalidPropertyValue( property, longs );
    }

    @Test
    public void shouldReport15DimensionalPointRecord()
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        final int keyId = 6;
        add( inUse( new PropertyKeyTokenRecord( keyId ) ) );
        final long[] longs = GeometryType.encodePoint( keyId, CoordinateReferenceSystem.WGS84, new double[] { 1.0, 2.0 } );
        // corrupt array
        long dimensionBits = 0xFL << StandardFormatSettings.PROPERTY_TOKEN_MAXIMUM_ID_BITS + 8;
        longs[0] |= dimensionBits;

        expectInvalidPropertyValue( property, longs );
    }

    @Test
    public void shouldReportUnknownCRSPointRecord()
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        final int keyId = 6;
        add( inUse( new PropertyKeyTokenRecord( keyId ) ) );
        final long[] longs = GeometryType.encodePoint( keyId, CoordinateReferenceSystem.WGS84, new double[] { 1.0, 2.0 } );
        // corrupt array
        long crsTableIdAndCodeBits = 0xFFFFL << StandardFormatSettings.PROPERTY_TOKEN_MAXIMUM_ID_BITS + 12;
        longs[0] |= crsTableIdAndCodeBits;

        expectInvalidPropertyValue( property, longs );
    }

    @Test
    public void shouldReportTooHighDateRecord()
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        final int keyId = 6;
        add( inUse( new PropertyKeyTokenRecord( keyId ) ) );
        final long[] longs = TemporalType.encodeDate( keyId,  LocalDate.MAX.toEpochDay() + 1 );

        expectInvalidPropertyValue( property, longs );
    }

    @Test
    public void shouldReportTooHighLocalTimeRecord()
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        final int keyId = 6;
        add( inUse( new PropertyKeyTokenRecord( keyId ) ) );
        final long[] longs = TemporalType.encodeLocalTime( keyId, LocalTime.MAX.toNanoOfDay() + 1 );

        expectInvalidPropertyValue( property, longs );
    }

    @Test
    public void shouldReportTooHighNanoLocalDateTimeRecord()
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        final int keyId = 6;
        add( inUse( new PropertyKeyTokenRecord( keyId ) ) );
        final long[] longs = TemporalType.encodeLocalDateTime( keyId, 1, 1_000_000_000 );

        expectInvalidPropertyValue( property, longs );
    }

    @Test
    public void shouldReportTooHighEpochSecondLocalDateTimeRecord()
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        final int keyId = 6;
        add( inUse( new PropertyKeyTokenRecord( keyId ) ) );
        final long[] longs = TemporalType.encodeLocalDateTime( keyId, Instant.MAX.getEpochSecond() + 1,1 );

        expectInvalidPropertyValue( property, longs );
    }

    @Test
    public void shouldReportTooHighNanoDateTimeRecord()
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        final int keyId = 6;
        add( inUse( new PropertyKeyTokenRecord( keyId ) ) );
        final long[] longs = TemporalType.encodeDateTime( keyId, 1, 1_000_000_000, 0 );

        expectInvalidPropertyValue( property, longs );
    }

    @Test
    public void shouldReportTooHighEpochSecondDateTimeRecord()
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        final int keyId = 6;
        add( inUse( new PropertyKeyTokenRecord( keyId ) ) );
        final long[] longs = TemporalType.encodeDateTime( keyId, Instant.MAX.getEpochSecond() + 1,1, 0 );

        expectInvalidPropertyValue( property, longs );
    }

    @Test
    public void shouldReportTooHighNanoDateTimeRecordWithNamedTZ()
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        final int keyId = 6;
        add( inUse( new PropertyKeyTokenRecord( keyId ) ) );
        final long[] longs = TemporalType.encodeDateTime( keyId, 1, 1_000_000_000, "Europe/London" );

        expectInvalidPropertyValue( property, longs );
    }

    @Test
    public void shouldReportTooHighEpochSecondDateTimeRecordWithNamedTZ()
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        final int keyId = 6;
        add( inUse( new PropertyKeyTokenRecord( keyId ) ) );
        final long[] longs = TemporalType.encodeDateTime( keyId, Instant.MAX.getEpochSecond() + 1,1, "Europe/London" );

        expectInvalidPropertyValue( property, longs );
    }

    @Test
    public void shouldReportTooHighOffsetSecondDateTimeRecord()
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        final int keyId = 6;
        add( inUse( new PropertyKeyTokenRecord( keyId ) ) );
        final long[] longs = TemporalType.encodeDateTime( keyId, 1,1, ZoneOffset.MAX.getTotalSeconds() + 1 );

        expectInvalidPropertyValue( property, longs );
    }

    @Test
    public void shouldReportTooHighNanoTimeRecord()
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        final int keyId = 6;
        add( inUse( new PropertyKeyTokenRecord( keyId ) ) );
        final long[] longs = TemporalType.encodeTime( keyId,  LocalTime.MAX.toNanoOfDay() + 1, 0 );

        expectInvalidPropertyValue( property, longs );
    }

    @Test
    public void shouldReportTooHighOffsetSecondTimeRecord()
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        final int keyId = 6;
        add( inUse( new PropertyKeyTokenRecord( keyId ) ) );
        final long[] longs = TemporalType.encodeTime( keyId, 1, ZoneOffset.MAX.getTotalSeconds() + 1 );

        expectInvalidPropertyValue( property, longs );
    }

    private void expectInvalidPropertyValue( PropertyRecord property, long[] longs )
    {
        PropertyBlock block =  new PropertyBlock();
        block.setValueBlocks( longs );
        property.addPropertyBlock( block );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );

        // then
        verify( report ).invalidPropertyValue( block );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportEmptyArrayRecord()
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyKeyTokenRecord key = add( inUse( new PropertyKeyTokenRecord( 6 ) ) );
        DynamicRecord value = add( inUse( array( new DynamicRecord( 1001 ) ) ) );
        PropertyBlock block = propertyBlock( key, value );
        property.addPropertyBlock( block );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );

        // then
        verify( report ).arrayEmpty( block, value );
        verifyNoMoreInteractions( report );
    }
}
