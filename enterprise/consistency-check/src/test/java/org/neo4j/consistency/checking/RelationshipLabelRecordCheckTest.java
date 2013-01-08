/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;

public class RelationshipLabelRecordCheckTest extends
        RecordCheckTestBase<RelationshipTypeRecord, ConsistencyReport.LabelConsistencyReport, RelationshipLabelRecordCheck>
{
    public RelationshipLabelRecordCheckTest()
    {
        super( new RelationshipLabelRecordCheck(), ConsistencyReport.LabelConsistencyReport.class );
    }

    @Test
    public void shouldNotReportAnythingForRecordNotInUse() throws Exception
    {
        // given
        RelationshipTypeRecord label = notInUse( new RelationshipTypeRecord( 42 ) );

        // when
        ConsistencyReport.LabelConsistencyReport report = check( label );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldNotReportAnythingForRecordThatDoesNotReferenceADynamicBlock() throws Exception
    {
        // given
        RelationshipTypeRecord label = inUse( new RelationshipTypeRecord( 42 ) );

        // when
        ConsistencyReport.LabelConsistencyReport report = check( label );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportDynamicBlockNotInUse() throws Exception
    {
        // given
        RelationshipTypeRecord label = inUse( new RelationshipTypeRecord( 42 ) );
        DynamicRecord name = addLabelName( notInUse( new DynamicRecord( 6 ) ) );
        label.setNameId( (int) name.getId() );

        // when
        ConsistencyReport.LabelConsistencyReport report = check( label );

        // then
        verify( report ).nameBlockNotInUse( name );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportEmptyName() throws Exception
    {
        // given
        RelationshipTypeRecord label = inUse( new RelationshipTypeRecord( 42 ) );
        DynamicRecord name = addLabelName( inUse( new DynamicRecord( 6 ) ) );
        label.setNameId( (int) name.getId() );

        // when
        ConsistencyReport.LabelConsistencyReport report = check( label );

        // then
        verify( report ).emptyName( name );
        verifyOnlyReferenceDispatch( report );
    }

    // change checking

    @Test
    public void shouldNotReportAnythingForConsistentlyChangedRecord() throws Exception
    {
        // given
        RelationshipTypeRecord oldRecord = notInUse( new RelationshipTypeRecord( 42 ) );
        RelationshipTypeRecord newRecord = inUse( new RelationshipTypeRecord( 42 ) );
        DynamicRecord name = addLabelName( inUse( new DynamicRecord( 6 ) ) );
        name.setData( new byte[1] );
        newRecord.setNameId( (int) name.getId()  );

        // when
        ConsistencyReport.LabelConsistencyReport report = checkChange( oldRecord, newRecord );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportProblemsWithTheNewStateWhenCheckingChanges() throws Exception
    {
        // given
        RelationshipTypeRecord oldRecord = notInUse( new RelationshipTypeRecord( 42 ) );
        RelationshipTypeRecord newRecord = inUse( new RelationshipTypeRecord( 42 ) );
        DynamicRecord name = addLabelName( notInUse( new DynamicRecord( 6 ) ) );
        newRecord.setNameId( (int) name.getId()  );

        // when
        ConsistencyReport.LabelConsistencyReport report = checkChange( oldRecord, newRecord );

        // then
        verify( report ).nameBlockNotInUse( name );
        verifyOnlyReferenceDispatch( report );
    }
}
