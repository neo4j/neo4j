/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;

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
    public void shouldNotReportAnythingForPropertyRecordNotInUse() throws Exception
    {
        // given
        PropertyRecord property = notInUse( new PropertyRecord( 42 ) );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldNotReportAnythingForPropertyWithoutBlocksThatDoesNotReferenceAnyOtherRecords() throws Exception
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportPropertyKeyNotInUse() throws Exception
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
    public void shouldReportPreviousPropertyNotInUse() throws Exception
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
    public void shouldReportNextPropertyNotInUse() throws Exception
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
    public void shouldReportPreviousPropertyNotReferringBack() throws Exception
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
    public void shouldReportNextPropertyNotReferringBack() throws Exception
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
    public void shouldReportStringRecordNotInUse() throws Exception
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
    public void shouldReportArrayRecordNotInUse() throws Exception
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
    public void shouldReportEmptyStringRecord() throws Exception
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
    public void shouldReportEmptyArrayRecord() throws Exception
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
