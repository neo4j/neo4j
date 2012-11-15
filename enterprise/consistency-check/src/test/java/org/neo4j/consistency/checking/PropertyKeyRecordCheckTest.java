/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency.checking;

import static org.mockito.Mockito.verify;

import org.junit.Test;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;

public class PropertyKeyRecordCheckTest extends
                                        RecordCheckTestBase<PropertyIndexRecord, ConsistencyReport.PropertyKeyConsistencyReport, PropertyKeyRecordCheck>
{
    public PropertyKeyRecordCheckTest()
    {
        super( new PropertyKeyRecordCheck(), ConsistencyReport.PropertyKeyConsistencyReport.class );
    }

    @Test
    public void shouldNotReportAnythingForRecordNotInUse() throws Exception
    {
        // given
        PropertyIndexRecord key = notInUse( new PropertyIndexRecord( 42 ) );

        // when
        ConsistencyReport.PropertyKeyConsistencyReport report = check( key );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldNotReportAnythingForRecordThatDoesNotReferenceADynamicBlock() throws Exception
    {
        // given
        PropertyIndexRecord key = inUse( new PropertyIndexRecord( 42 ) );

        // when
        ConsistencyReport.PropertyKeyConsistencyReport report = check( key );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportDynamicBlockNotInUse() throws Exception
    {
        // given
        PropertyIndexRecord key = inUse( new PropertyIndexRecord( 42 ) );
        DynamicRecord name = addKeyName( notInUse( new DynamicRecord( 6 ) ) );
        key.setNameId( (int) name.getId() );

        // when
        ConsistencyReport.PropertyKeyConsistencyReport report = check( key );

        // then
        verify( report ).nameBlockNotInUse( name );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportEmptyName() throws Exception
    {
        // given
        PropertyIndexRecord key = inUse( new PropertyIndexRecord( 42 ) );
        DynamicRecord name = addKeyName( inUse( new DynamicRecord( 6 ) ) );
        key.setNameId( (int) name.getId() );

        // when
        ConsistencyReport.PropertyKeyConsistencyReport report = check( key );

        // then
        verify( report ).emptyName( name );
        verifyOnlyReferenceDispatch( report );
    }

    // change checking

    @Test
    public void shouldNotReportAnythingForConsistentlyChangedRecord() throws Exception
    {
        // given
        PropertyIndexRecord oldRecord = notInUse( new PropertyIndexRecord( 42 ) );
        PropertyIndexRecord newRecord = inUse( new PropertyIndexRecord( 42 ) );
        DynamicRecord name = addKeyName( inUse( new DynamicRecord( 6 ) ) );
        name.setData( new byte[1] );
        newRecord.setNameId( (int) name.getId() );

        // when
        ConsistencyReport.PropertyKeyConsistencyReport report = checkChange( oldRecord, newRecord );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportProblemsWithTheNewStateWhenCheckingChanges() throws Exception
    {
        // given
        PropertyIndexRecord oldRecord = notInUse( new PropertyIndexRecord( 42 ) );
        PropertyIndexRecord newRecord = inUse( new PropertyIndexRecord( 42 ) );
        DynamicRecord name = addKeyName( notInUse( new DynamicRecord( 6 ) ) );
        newRecord.setNameId( (int) name.getId() );

        // when
        ConsistencyReport.PropertyKeyConsistencyReport report = checkChange( oldRecord, newRecord );

        // then
        verify( report ).nameBlockNotInUse( name );
        verifyOnlyReferenceDispatch( report );
    }
}
