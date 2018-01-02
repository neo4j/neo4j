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
package org.neo4j.legacy.consistency.checking;

import org.junit.Test;

import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.legacy.consistency.report.ConsistencyReport;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class MetaDataStoreCheckTest
        extends RecordCheckTestBase<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport,NeoStoreCheck>
{
    public MetaDataStoreCheckTest()
    {
        super( new NeoStoreCheck(), ConsistencyReport.NeoStoreConsistencyReport.class );
    }

    @Test
    public void shouldNotReportAnythingForRecordWithNoPropertyReference() throws Exception
    {
        // given
        NeoStoreRecord record = new NeoStoreRecord();

        // when
        ConsistencyReport.NeoStoreConsistencyReport report = check( record );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldNotReportAnythingForRecordWithConsistentReferenceToProperty() throws Exception
    {
        // given
        NeoStoreRecord record = new NeoStoreRecord();
        record.setNextProp( add( inUse( new PropertyRecord( 7 ) ) ).getId() );

        // when
        ConsistencyReport.NeoStoreConsistencyReport report = check( record );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportPropertyNotInUse() throws Exception
    {
        // given
        NeoStoreRecord record = new NeoStoreRecord();
        PropertyRecord property = add( notInUse( new PropertyRecord( 7 ) ) );
        record.setNextProp( property.getId() );

        // when
        ConsistencyReport.NeoStoreConsistencyReport report = check( record );

        // then
        verify( report ).propertyNotInUse( property );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportPropertyNotFirstInChain() throws Exception
    {
        // given
        NeoStoreRecord record = new NeoStoreRecord();
        PropertyRecord property = add( inUse( new PropertyRecord( 7 ) ) );
        property.setPrevProp( 6 );
        record.setNextProp( property.getId() );

        // when
        ConsistencyReport.NeoStoreConsistencyReport report = check( record );

        // then
        verify( report ).propertyNotFirstInChain( property );
        verifyNoMoreInteractions( report );
    }

    // Change checking

    @Test
    public void shouldNotReportAnythingForConsistentlyChangedRecord() throws Exception
    {
        // given
        NeoStoreRecord oldRecord = new NeoStoreRecord();
        NeoStoreRecord newRecord = new NeoStoreRecord();

        oldRecord.setNextProp( addChange( inUse( new PropertyRecord( 1 ) ),
                                          notInUse( new PropertyRecord( 1 ) ) ).getId() );

        newRecord.setNextProp( addChange( notInUse( new PropertyRecord( 2 ) ),
                                          inUse( new PropertyRecord( 2 ) ) ).getId() );

        // when
        ConsistencyReport.NeoStoreConsistencyReport report = checkChange( oldRecord, newRecord );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportProblemsWithTheNewStateWhenCheckingChanges() throws Exception
    {
        // given
        NeoStoreRecord oldRecord = new NeoStoreRecord();
        NeoStoreRecord newRecord = new NeoStoreRecord();

        oldRecord.setNextProp( addChange( inUse( new PropertyRecord( 1 ) ),
                                          notInUse( new PropertyRecord( 1 ) ) ).getId() );

        PropertyRecord property = addChange( notInUse( new PropertyRecord( 2 ) ),
                                             inUse( new PropertyRecord( 2 ) ) );
        property.setPrevProp( 10 );
        newRecord.setNextProp( property.getId() );

        // when
        ConsistencyReport.NeoStoreConsistencyReport report = checkChange( oldRecord, newRecord );

        // then
        verify( report ).propertyNotFirstInChain( property );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportPropertyChainReplacedButNotUpdated() throws Exception
    {
        // given
        NeoStoreRecord oldRecord = new NeoStoreRecord();
        NeoStoreRecord newRecord = new NeoStoreRecord();
        oldRecord.setNextProp( add( inUse( new PropertyRecord( 1 ) ) ).getId() );
        newRecord.setNextProp( addChange( notInUse( new PropertyRecord( 2 ) ),
                                          inUse( new PropertyRecord( 2 ) ) ).getId() );

        // when
        ConsistencyReport.NeoStoreConsistencyReport report = checkChange( oldRecord, newRecord );

        // then
        verify( report ).propertyNotUpdated();
        verifyNoMoreInteractions( report );
    }
}
