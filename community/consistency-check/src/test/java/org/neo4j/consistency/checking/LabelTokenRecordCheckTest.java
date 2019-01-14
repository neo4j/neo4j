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

import org.neo4j.consistency.report.ConsistencyReport.LabelTokenConsistencyReport;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class LabelTokenRecordCheckTest
        extends RecordCheckTestBase<LabelTokenRecord,LabelTokenConsistencyReport,LabelTokenRecordCheck>
{
    public LabelTokenRecordCheckTest()
    {
        super( new LabelTokenRecordCheck(), LabelTokenConsistencyReport.class, new int[0] );
    }

    @Test
    public void shouldNotReportAnythingForRecordNotInUse()
    {
        // given
        LabelTokenRecord key = notInUse( new LabelTokenRecord( 42 ) );

        // when
        LabelTokenConsistencyReport report = check( key );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldNotReportAnythingForRecordThatDoesNotReferenceADynamicBlock()
    {
        // given
        LabelTokenRecord key = inUse( new LabelTokenRecord( 42 ) );

        // when
        LabelTokenConsistencyReport report = check( key );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportDynamicBlockNotInUse()
    {
        // given
        LabelTokenRecord key = inUse( new LabelTokenRecord( 42 ) );
        DynamicRecord name = addLabelName( notInUse( new DynamicRecord( 6 ) ) );
        key.setNameId( (int) name.getId() );

        // when
        LabelTokenConsistencyReport report = check( key );

        // then
        verify( report ).nameBlockNotInUse( name );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportEmptyName()
    {
        // given
        LabelTokenRecord key = inUse( new LabelTokenRecord( 42 ) );
        DynamicRecord name = addLabelName( inUse( new DynamicRecord( 6 ) ) );
        key.setNameId( (int) name.getId() );

        // when
        LabelTokenConsistencyReport report = check( key );

        // then
        verify( report ).emptyName( name );
        verifyNoMoreInteractions( report );
    }
}
