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

import org.neo4j.consistency.checking.full.MandatoryProperties;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.function.Functions;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class MetaDataStoreCheckTest
        extends RecordCheckTestBase<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport,NeoStoreCheck>
{
    public MetaDataStoreCheckTest()
    {
        super( new NeoStoreCheck( new PropertyChain<NeoStoreRecord,ConsistencyReport.NeoStoreConsistencyReport>(
                Functions.<NeoStoreRecord,MandatoryProperties.Check<NeoStoreRecord,ConsistencyReport.NeoStoreConsistencyReport>>nullFunction() ) ),
                ConsistencyReport.NeoStoreConsistencyReport.class, new int[0] );
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
}
