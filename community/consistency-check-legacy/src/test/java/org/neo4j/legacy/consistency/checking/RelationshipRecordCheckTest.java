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

import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.legacy.consistency.report.ConsistencyReport;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class RelationshipRecordCheckTest extends
                                         RecordCheckTestBase<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport, RelationshipRecordCheck>
{
    public RelationshipRecordCheckTest()
    {
        super( new RelationshipRecordCheck(), ConsistencyReport.RelationshipConsistencyReport.class );
    }

    @Test
    public void shouldNotReportAnythingForRelationshipNotInUse() throws Exception
    {
        // given
        RelationshipRecord relationship = notInUse( new RelationshipRecord( 42, 0, 0, 0 ) );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldNotReportAnythingForRelationshipThatDoesNotReferenceOtherRecords() throws Exception
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldNotReportAnythingForRelationshipWithConsistentReferences() throws Exception
    {
        // given
        /*
         * (1) --> (3) <==> (2)
         */
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1, false, relationship.getId(), NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 53, NONE ) ) );
        add( inUse( new NodeRecord( 3, false, NONE, NONE ) ) );
        add( inUse( new PropertyRecord( 101 ) ) );
        relationship.setNextProp( 101 );
        RelationshipRecord sNext = add( inUse( new RelationshipRecord( 51, 1, 3, 4 ) ) );
        RelationshipRecord tNext = add( inUse( new RelationshipRecord( 52, 2, 3, 4 ) ) );
        RelationshipRecord tPrev = add( inUse( new RelationshipRecord( 53, 3, 2, 4 ) ) );

        relationship.setFirstNextRel( sNext.getId() );
        sNext.setFirstPrevRel( relationship.getId() );
        sNext.setFirstInFirstChain( false );
        relationship.setSecondNextRel( tNext.getId() );
        tNext.setFirstPrevRel( relationship.getId() );
        tNext.setFirstInFirstChain( false );
        relationship.setSecondPrevRel( tPrev.getId() );
        relationship.setFirstInSecondChain( false );
        tPrev.setSecondNextRel( relationship.getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportIllegalRelationshipType() throws Exception
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, NONE ) );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).illegalRelationshipType();
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportRelationshipTypeNotInUse() throws Exception
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        RelationshipTypeTokenRecord relationshipType = add( notInUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).relationshipTypeNotInUse( relationshipType );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportIllegalSourceNode() throws Exception
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, NONE, 1, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).illegalSourceNode();
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportSourceNodeNotInUse() throws Exception
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        NodeRecord node = add( notInUse( new NodeRecord( 1, false, NONE, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).sourceNodeNotInUse( node );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportIllegalTargetNode() throws Exception
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, NONE, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).illegalTargetNode();
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportTargetNodeNotInUse() throws Exception
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        NodeRecord node = add( notInUse( new NodeRecord( 2, false, NONE, NONE ) ) );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).targetNodeNotInUse( node );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportPropertyNotInUse() throws Exception
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        relationship.setNextProp( 11 );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );
        PropertyRecord property = add( notInUse( new PropertyRecord( 11 ) ) );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).propertyNotInUse( property );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportPropertyNotFirstInChain() throws Exception
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        relationship.setNextProp( 11 );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );
        PropertyRecord property = add( inUse( new PropertyRecord( 11 ) ) );
        property.setPrevProp( 6 );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).propertyNotFirstInChain( property );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportSourceNodeNotReferencingBackForFirstRelationshipInSourceChain() throws Exception
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        NodeRecord source = add( inUse( new NodeRecord( 1, false, 7, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).sourceNodeDoesNotReferenceBack( source );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportTargetNodeNotReferencingBackForFirstRelationshipInTargetChain() throws Exception
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        NodeRecord target = add( inUse( new NodeRecord( 2, false, 7, NONE ) ) );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).targetNodeDoesNotReferenceBack( target );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportSourceAndTargetNodeNotReferencingBackForFirstRelationshipInChains() throws Exception
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        NodeRecord source = add( inUse( new NodeRecord( 1, false, NONE, NONE ) ) );
        NodeRecord target = add( inUse( new NodeRecord( 2, false, NONE, NONE ) ) );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).sourceNodeDoesNotReferenceBack( source );
        verify( report ).targetNodeDoesNotReferenceBack( target );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportSourceNodeWithoutChainForRelationshipInTheMiddleOfChain() throws Exception
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        NodeRecord source = add( inUse( new NodeRecord( 1, false, NONE, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );
        RelationshipRecord sPrev = add( inUse( new RelationshipRecord( 51, 1, 0, 0 ) ) );
        relationship.setFirstPrevRel( sPrev.getId() );
        relationship.setFirstInFirstChain( false );
        sPrev.setFirstNextRel( relationship.getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).sourceNodeHasNoRelationships( source );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportTargetNodeWithoutChainForRelationshipInTheMiddleOfChain() throws Exception
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        NodeRecord target = add( inUse( new NodeRecord( 2, false, NONE, NONE ) ) );
        RelationshipRecord tPrev = add( inUse( new RelationshipRecord( 51, 0, 2, 0 ) ) );
        relationship.setSecondPrevRel( tPrev.getId() );
        relationship.setFirstInSecondChain( false );
        tPrev.setSecondNextRel( relationship.getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).targetNodeHasNoRelationships( target );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportSourcePrevReferencingOtherNodes() throws Exception
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1, false, 0, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );
        RelationshipRecord sPrev = add( inUse( new RelationshipRecord( 51, 8, 9, 0 ) ) );
        relationship.setFirstPrevRel( sPrev.getId() );
        relationship.setFirstInFirstChain( false );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).sourcePrevReferencesOtherNodes( sPrev );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportTargetPrevReferencingOtherNodes() throws Exception
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 0, NONE ) ) );
        RelationshipRecord tPrev = add( inUse( new RelationshipRecord( 51, 8, 9, 0 ) ) );
        relationship.setSecondPrevRel( tPrev.getId() );
        relationship.setFirstInSecondChain( false );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).targetPrevReferencesOtherNodes( tPrev );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportSourceNextReferencingOtherNodes() throws Exception
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );
        RelationshipRecord sNext = add( inUse( new RelationshipRecord( 51, 8, 9, 0 ) ) );
        relationship.setFirstNextRel( sNext.getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).sourceNextReferencesOtherNodes( sNext );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportTargetNextReferencingOtherNodes() throws Exception
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );
        RelationshipRecord tNext = add( inUse( new RelationshipRecord( 51, 8, 9, 0 ) ) );
        relationship.setSecondNextRel( tNext.getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).targetNextReferencesOtherNodes( tNext );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportSourcePrevReferencingOtherNodesWhenReferencingTargetNode() throws Exception
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1, false, 0, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );
        RelationshipRecord sPrev = add( inUse( new RelationshipRecord( 51, 2, 0, 0 ) ) );
        relationship.setFirstPrevRel( sPrev.getId() );
        relationship.setFirstInFirstChain( false );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).sourcePrevReferencesOtherNodes( sPrev );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportTargetPrevReferencingOtherNodesWhenReferencingSourceNode() throws Exception
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 0, NONE ) ) );
        RelationshipRecord tPrev = add( inUse( new RelationshipRecord( 51, 1, 0, 0 ) ) );
        relationship.setSecondPrevRel( tPrev.getId() );
        relationship.setFirstInSecondChain( false );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).targetPrevReferencesOtherNodes( tPrev );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportSourceNextReferencingOtherNodesWhenReferencingTargetNode() throws Exception
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );
        RelationshipRecord sNext = add( inUse( new RelationshipRecord( 51, 2, 0, 0 ) ) );
        relationship.setFirstNextRel( sNext.getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).sourceNextReferencesOtherNodes( sNext );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportTargetNextReferencingOtherNodesWhenReferencingSourceNode() throws Exception
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );
        RelationshipRecord tNext = add( inUse( new RelationshipRecord( 51, 1, 0, 0 ) ) );
        relationship.setSecondNextRel( tNext.getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).targetNextReferencesOtherNodes( tNext );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportSourcePrevNotReferencingBack() throws Exception
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1, false, 0, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );
        RelationshipRecord sPrev = add( inUse( new RelationshipRecord( 51, 1, 3, 0 ) ) );
        relationship.setFirstPrevRel( sPrev.getId() );
        relationship.setFirstInFirstChain( false );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).sourcePrevDoesNotReferenceBack( sPrev );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportTargetPrevNotReferencingBack() throws Exception
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 0, NONE ) ) );
        RelationshipRecord tPrev = add( inUse( new RelationshipRecord( 51, 2, 3, 0 ) ) );
        relationship.setSecondPrevRel( tPrev.getId() );
        relationship.setFirstInSecondChain( false );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).targetPrevDoesNotReferenceBack( tPrev );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportSourceNextNotReferencingBack() throws Exception
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );
        RelationshipRecord sNext = add( inUse( new RelationshipRecord( 51, 3, 1, 0 ) ) );
        relationship.setFirstNextRel( sNext.getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).sourceNextDoesNotReferenceBack( sNext );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportTargetNextNotReferencingBack() throws Exception
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );
        RelationshipRecord tNext = add( inUse( new RelationshipRecord( 51, 3, 2, 0 ) ) );
        relationship.setSecondNextRel( tNext.getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).targetNextDoesNotReferenceBack( tNext );
        verifyNoMoreInteractions( report );
    }

    // change checking

    @Test
    public void shouldNotReportAnythingForConsistentlyChangedRelationship() throws Exception
    {
        // given
        RelationshipRecord oldRelationship = inUse( new RelationshipRecord( 42, 11, 12, 0 ) );
        oldRelationship.setNextProp( 1 );
        oldRelationship.setFirstNextRel( 101 );
        oldRelationship.setFirstPrevRel( 102 );
        oldRelationship.setFirstInFirstChain( false );
        oldRelationship.setSecondNextRel( 103 );
        oldRelationship.setSecondPrevRel( 104 );
        oldRelationship.setFirstInSecondChain( false );
        RelationshipRecord newRelationship = inUse( new RelationshipRecord( 42, 11, 12, 0 ) );
        oldRelationship.setNextProp( 2 );
        newRelationship.setFirstNextRel( 201 );
        newRelationship.setFirstPrevRel( 202 );
        newRelationship.setFirstInFirstChain( false );
        newRelationship.setSecondNextRel( 203 );
        newRelationship.setSecondPrevRel( 204 );
        newRelationship.setFirstInSecondChain( false );

        add( inUse( new RelationshipTypeTokenRecord( 0 ) ) );
        add( inUse( new NodeRecord( 11, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 12, false, 42, NONE ) ) );

        addChange( inUse( new PropertyRecord( 1 ) ),
                   inUse( new PropertyRecord( 1 ) ) );
        addChange( inUse( new RelationshipRecord( 101, 0, 0, 0 ) ),
                   inUse( new RelationshipRecord( 101, 0, 0, 0 ) ) );
        addChange( inUse( new RelationshipRecord( 102, 0, 0, 0 ) ),
                   inUse( new RelationshipRecord( 102, 0, 0, 0 ) ) );
        addChange( inUse( new RelationshipRecord( 103, 0, 0, 0 ) ),
                   inUse( new RelationshipRecord( 103, 0, 0, 0 ) ) );
        addChange( inUse( new RelationshipRecord( 104, 0, 0, 0 ) ),
                   inUse( new RelationshipRecord( 104, 0, 0, 0 ) ) );

        addChange( notInUse( new PropertyRecord( 2 ) ),
                   inUse( new PropertyRecord( 2 ) ) );
        addChange( notInUse( new RelationshipRecord( 201, 0, 0, 0 ) ),
                   inUse( new RelationshipRecord( 201, 11, 0, 0 ) ) ).setFirstPrevRel( 42 );
        addChange( notInUse( new RelationshipRecord( 202, 0, 0, 0 ) ),
                   inUse( new RelationshipRecord( 202, 0, 11, 0 ) ) ).setSecondNextRel( 42 );
        addChange( notInUse( new RelationshipRecord( 203, 0, 0, 0 ) ),
                   inUse( new RelationshipRecord( 203, 0, 12, 0 ) ) ).setSecondPrevRel( 42 );
        addChange( notInUse( new RelationshipRecord( 204, 0, 0, 0 ) ),
                   inUse( new RelationshipRecord( 204, 12, 0, 0 ) ) ).setFirstNextRel( 42 );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = checkChange( oldRelationship, newRelationship );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportProblemsWithTheNewStateWhenCheckingChanges() throws Exception
    {
        // given
        RelationshipRecord oldRelationship = notInUse( new RelationshipRecord( 42, 0, 0, 0 ) );
        RelationshipRecord newRelationship = inUse( new RelationshipRecord( 42, 1, 2, 0 ) );
        NodeRecord source = add( notInUse( new NodeRecord( 1, false, 0, 0 ) ) );
        NodeRecord target = add( notInUse( new NodeRecord( 2, false, 0, 0 ) ) );
        RelationshipTypeTokenRecord label = add( notInUse( new RelationshipTypeTokenRecord( 0 ) ) );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = checkChange( oldRelationship, newRelationship );

        // then
        verify( report ).sourceNodeNotInUse( source );
        verify( report ).targetNodeNotInUse( target );
        verify( report ).relationshipTypeNotInUse( label );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldNotReportAnythingWhenAddingAnInitialProperty() throws Exception
    {
        // given
        add( inUse( new RelationshipTypeTokenRecord( 0 ) ) );

        RelationshipRecord oldRelationship = inUse( new RelationshipRecord( 42, 1, 2, 0 ) );
        RelationshipRecord newRelationship = inUse( new RelationshipRecord( 42, 1, 2, 0 ) );

        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );

        newRelationship.setNextProp( addChange( notInUse( new PropertyRecord( 10 ) ),
                                                inUse( new PropertyRecord( 10 ) ) ).getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = checkChange( oldRelationship, newRelationship );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldNotReportAnythingWhenChangingProperty() throws Exception
    {
        // given
        add( inUse( new RelationshipTypeTokenRecord( 0 ) ) );

        RelationshipRecord oldRelationship = inUse( new RelationshipRecord( 42, 1, 2, 0 ) );
        RelationshipRecord newRelationship = inUse( new RelationshipRecord( 42, 1, 2, 0 ) );

        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );

        PropertyRecord oldProp = addChange( inUse( new PropertyRecord( 10 ) ),
                                            inUse( new PropertyRecord( 10 ) ) );
        PropertyRecord newProp = addChange( notInUse( new PropertyRecord( 11 ) ),
                                            inUse( new PropertyRecord( 11 ) ) );
        oldProp.setPrevProp( newProp.getId() );
        newProp.setNextProp( oldProp.getId() );

        oldRelationship.setNextProp( oldProp.getId() );
        newRelationship.setNextProp( newProp.getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = checkChange( oldRelationship, newRelationship );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldNotReportAnythingWhenAddingPrevSourceRelationship() throws Exception
    {
        // given
        add( inUse( new RelationshipTypeTokenRecord( 0 ) ) );

        RelationshipRecord oldRelationship = inUse( new RelationshipRecord( 42, 1, 2, 0 ) );
        RelationshipRecord newRelationship = inUse( new RelationshipRecord( 42, 1, 2, 0 ) );

        addChange( inUse( new NodeRecord( 1, false, 42, NONE ) ),
                   inUse( new NodeRecord( 1, false, 10, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );

        RelationshipRecord prev = addChange( notInUse( new RelationshipRecord( 10, 0, 0, 0 ) ),
                                             inUse( new RelationshipRecord( 10, 1, 3, 0 ) ) );

        newRelationship.setFirstPrevRel( prev.getId() );
        newRelationship.setFirstInFirstChain( false );
        prev.setFirstNextRel( newRelationship.getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = checkChange( oldRelationship, newRelationship );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldNotReportAnythingWhenAddingPrevTargetRelationship() throws Exception
    {
        // given
        add( inUse( new RelationshipTypeTokenRecord( 0 ) ) );

        RelationshipRecord oldRelationship = inUse( new RelationshipRecord( 42, 1, 2, 0 ) );
        RelationshipRecord newRelationship = inUse( new RelationshipRecord( 42, 1, 2, 0 ) );

        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        addChange( inUse( new NodeRecord( 2, false, 42, NONE ) ),
                   inUse( new NodeRecord( 2, false, 10, NONE ) ) );

        RelationshipRecord prev = addChange( notInUse( new RelationshipRecord( 10, 0, 0, 0 ) ),
                                             inUse( new RelationshipRecord( 10, 3, 2, 0 ) ) );

        newRelationship.setSecondPrevRel( prev.getId() );
        newRelationship.setFirstInSecondChain( false );
        prev.setSecondNextRel( newRelationship.getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = checkChange( oldRelationship, newRelationship );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldNotReportAnythingWhenAddingNextSourceRelationship() throws Exception
    {
        // given
        add( inUse( new RelationshipTypeTokenRecord( 0 ) ) );

        RelationshipRecord oldRelationship = inUse( new RelationshipRecord( 42, 1, 2, 0 ) );
        RelationshipRecord newRelationship = inUse( new RelationshipRecord( 42, 1, 2, 0 ) );

        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );

        RelationshipRecord next = addChange( notInUse( new RelationshipRecord( 10, 0, 0, 0 ) ),
                                             inUse( new RelationshipRecord( 10, 1, 3, 0 ) ) );

        newRelationship.setFirstNextRel( next.getId() );
        next.setFirstPrevRel( newRelationship.getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = checkChange( oldRelationship, newRelationship );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldNotReportAnythingWhenAddingNextTargetRelationship() throws Exception
    {
        // given
        add( inUse( new RelationshipTypeTokenRecord( 0 ) ) );

        RelationshipRecord oldRelationship = inUse( new RelationshipRecord( 42, 1, 2, 0 ) );
        RelationshipRecord newRelationship = inUse( new RelationshipRecord( 42, 1, 2, 0 ) );

        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );

        RelationshipRecord next = addChange( notInUse( new RelationshipRecord( 10, 0, 0, 0 ) ),
                                             inUse( new RelationshipRecord( 10, 3, 2, 0 ) ) );

        newRelationship.setSecondNextRel( next.getId() );
        next.setSecondPrevRel( newRelationship.getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = checkChange( oldRelationship, newRelationship );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldNotReportAnythingWhenChangingPrevSourceRelationship() throws Exception
    {
        // given
        add( inUse( new RelationshipTypeTokenRecord( 0 ) ) );

        RelationshipRecord oldRelationship = inUse( new RelationshipRecord( 42, 1, 2, 0 ) );
        RelationshipRecord newRelationship = inUse( new RelationshipRecord( 42, 1, 2, 0 ) );

        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );

        RelationshipRecord oldPrev = inUse( new RelationshipRecord( 10, 1, 3, 0 ) );
        addChange( oldPrev, inUse( new RelationshipRecord( 10, 1, 3, 0 ) ) );
        RelationshipRecord newPrev = addChange( notInUse( new RelationshipRecord( 11, 0, 0, 0 ) ),
                                                inUse( new RelationshipRecord( 11, 1, 3, 0 ) ) );
        oldRelationship.setFirstPrevRel( oldPrev.getId() );
        oldRelationship.setFirstInFirstChain( false );
        oldPrev.setFirstNextRel( oldRelationship.getId() );

        newRelationship.setFirstPrevRel( newPrev.getId() );
        newRelationship.setFirstInFirstChain( false );
        newPrev.setFirstNextRel( newRelationship.getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = checkChange( oldRelationship, newRelationship );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldNotReportAnythingWhenChangingNextSourceRelationship() throws Exception
    {
        // given
        add( inUse( new RelationshipTypeTokenRecord( 0 ) ) );

        RelationshipRecord oldRelationship = inUse( new RelationshipRecord( 42, 1, 2, 0 ) );
        RelationshipRecord newRelationship = inUse( new RelationshipRecord( 42, 1, 2, 0 ) );

        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );

        RelationshipRecord oldNext = inUse( new RelationshipRecord( 10, 1, 3, 0 ) );
        addChange( oldNext, inUse( new RelationshipRecord( 10, 1, 3, 0 ) ) );
        RelationshipRecord newNext = addChange( notInUse( new RelationshipRecord( 11, 0, 0, 0 ) ),
                                                inUse( new RelationshipRecord( 11, 1, 3, 0 ) ) );
        oldRelationship.setFirstNextRel( oldNext.getId() );
        oldNext.setFirstPrevRel( oldRelationship.getId() );

        newRelationship.setFirstNextRel( newNext.getId() );
        newNext.setFirstPrevRel( newRelationship.getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = checkChange( oldRelationship, newRelationship );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldNotReportAnythingWhenChangingPrevTargetRelationship() throws Exception
    {
        // given
        add( inUse( new RelationshipTypeTokenRecord( 0 ) ) );

        RelationshipRecord oldRelationship = inUse( new RelationshipRecord( 42, 1, 2, 0 ) );
        RelationshipRecord newRelationship = inUse( new RelationshipRecord( 42, 1, 2, 0 ) );

        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );

        RelationshipRecord oldPrev = inUse( new RelationshipRecord( 10, 3, 2, 0 ) );
        addChange( oldPrev, inUse( new RelationshipRecord( 10, 3, 2, 0 ) ) );
        RelationshipRecord newPrev = addChange( notInUse( new RelationshipRecord( 11, 0, 0, 0 ) ),
                                                inUse( new RelationshipRecord( 11, 3, 2, 0 ) ) );
        oldRelationship.setSecondPrevRel( oldPrev.getId() );
        oldRelationship.setFirstInSecondChain( false );
        oldPrev.setSecondNextRel( oldRelationship.getId() );

        newRelationship.setSecondPrevRel( newPrev.getId() );
        newRelationship.setFirstInSecondChain( false );
        newPrev.setSecondNextRel( newRelationship.getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = checkChange( oldRelationship, newRelationship );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldNotReportAnythingWhenChangingNextTargetRelationship() throws Exception
    {
        // given
        add( inUse( new RelationshipTypeTokenRecord( 0 ) ) );

        RelationshipRecord oldRelationship = inUse( new RelationshipRecord( 42, 1, 2, 0 ) );
        RelationshipRecord newRelationship = inUse( new RelationshipRecord( 42, 1, 2, 0 ) );

        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );

        RelationshipRecord oldNext = inUse( new RelationshipRecord( 10, 3, 2, 0 ) );
        addChange( oldNext, inUse( new RelationshipRecord( 10, 3, 2, 0 ) ) );
        RelationshipRecord newNext = addChange( notInUse( new RelationshipRecord( 11, 0, 0, 0 ) ),
                                                inUse( new RelationshipRecord( 11, 3, 2, 0 ) ) );
        oldRelationship.setSecondNextRel( oldNext.getId() );
        oldNext.setSecondPrevRel( oldRelationship.getId() );

        newRelationship.setSecondNextRel( newNext.getId() );
        newNext.setSecondPrevRel( newRelationship.getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = checkChange( oldRelationship, newRelationship );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportPropertyChainReplacedButNotUpdated() throws Exception
    {
        // given
        RelationshipRecord oldRelationship = inUse( new RelationshipRecord( 42, 11, 12, 0 ) );
        oldRelationship.setNextProp( 1 );
        RelationshipRecord newRelationship = inUse( new RelationshipRecord( 42, 11, 12, 0 ) );
        newRelationship.setNextProp( 2 );
        add( inUse( new RelationshipTypeTokenRecord( 0 ) ) );
        add( inUse( new NodeRecord( 11, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 12, false, 42, NONE ) ) );
        addChange( notInUse( new PropertyRecord( 2 ) ),
                   inUse( new PropertyRecord( 2 ) ) );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = checkChange( oldRelationship, newRelationship );

        // then
        verify( report ).propertyNotUpdated();
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportSourcePreviousReplacedButNotUpdated() throws Exception
    {
        // given
        RelationshipRecord oldRelationship = inUse( new RelationshipRecord( 42, 11, 12, 0 ) );
        oldRelationship.setFirstPrevRel( 101 );
        oldRelationship.setFirstInFirstChain( false );
        RelationshipRecord newRelationship = inUse( new RelationshipRecord( 42, 11, 12, 0 ) );
        newRelationship.setFirstPrevRel( 201 );
        newRelationship.setFirstInFirstChain( false );

        add( inUse( new RelationshipTypeTokenRecord( 0 ) ) );
        add( inUse( new NodeRecord( 11, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 12, false, 42, NONE ) ) );

        addChange( notInUse( new RelationshipRecord( 201, 0, 0, 0 ) ),
                   inUse( new RelationshipRecord( 201, 0, 11, 0 ) ) ).setSecondNextRel( 42 );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = checkChange( oldRelationship, newRelationship );

        // then
        verify( report ).sourcePrevNotUpdated();
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportSourceNextReplacedButNotUpdated() throws Exception
    {
        // given
        RelationshipRecord oldRelationship = inUse( new RelationshipRecord( 42, 11, 12, 0 ) );
        oldRelationship.setFirstNextRel( 101 );
        RelationshipRecord newRelationship = inUse( new RelationshipRecord( 42, 11, 12, 0 ) );
        newRelationship.setFirstNextRel( 201 );

        add( inUse( new RelationshipTypeTokenRecord( 0 ) ) );
        add( inUse( new NodeRecord( 11, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 12, false, 42, NONE ) ) );

        addChange( notInUse( new RelationshipRecord( 201, 0, 0, 0 ) ),
                   inUse( new RelationshipRecord( 201, 0, 11, 0 ) ) ).setSecondPrevRel( 42 );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = checkChange( oldRelationship, newRelationship );

        // then
        verify( report ).sourceNextNotUpdated();
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportTargetPreviousReplacedButNotUpdated() throws Exception
    {
        // given
        RelationshipRecord oldRelationship = inUse( new RelationshipRecord( 42, 11, 12, 0 ) );
        oldRelationship.setSecondPrevRel( 101 );
        oldRelationship.setFirstInSecondChain( false );
        RelationshipRecord newRelationship = inUse( new RelationshipRecord( 42, 11, 12, 0 ) );
        newRelationship.setSecondPrevRel( 201 );
        newRelationship.setFirstInSecondChain( false );

        add( inUse( new RelationshipTypeTokenRecord( 0 ) ) );
        add( inUse( new NodeRecord( 11, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 12, false, 42, NONE ) ) );

        addChange( notInUse( new RelationshipRecord( 201, 0, 0, 0 ) ),
                   inUse( new RelationshipRecord( 201, 12, 0, 0 ) ) ).setFirstNextRel( 42 );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = checkChange( oldRelationship, newRelationship );

        // then
        verify( report ).targetPrevNotUpdated();
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportTargetNextReplacedButNotUpdated() throws Exception
    {
        // given
        RelationshipRecord oldRelationship = inUse( new RelationshipRecord( 42, 11, 12, 0 ) );
        oldRelationship.setSecondNextRel( 101 );
        RelationshipRecord newRelationship = inUse( new RelationshipRecord( 42, 11, 12, 0 ) );
        newRelationship.setSecondNextRel( 201 );

        add( inUse( new RelationshipTypeTokenRecord( 0 ) ) );
        add( inUse( new NodeRecord( 11, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 12, false, 42, NONE ) ) );

        addChange( notInUse( new RelationshipRecord( 201, 0, 0, 0 ) ),
                   inUse( new RelationshipRecord( 201, 12, 0, 0 ) ) ).setFirstPrevRel( 42 );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = checkChange( oldRelationship, newRelationship );

        // then
        verify( report ).targetNextNotUpdated();
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportDeletedButReferencesNotUpdated() throws Exception
    {
        // given
        RelationshipRecord oldRelationship = inUse( new RelationshipRecord( 42, 11, 12, 0 ) );
        oldRelationship.setFirstPrevRel( add( inUse( new RelationshipRecord( 101, 11, 12, 0 ) ) ).getId() );
        oldRelationship.setFirstInFirstChain( false );
        oldRelationship.setFirstNextRel( add( inUse( new RelationshipRecord( 102, 11, 12, 0 ) ) ).getId() );
        oldRelationship.setSecondPrevRel( add( inUse( new RelationshipRecord( 103, 11, 12, 0 ) ) ).getId() );
        oldRelationship.setFirstInSecondChain( false );
        oldRelationship.setSecondNextRel( add( inUse( new RelationshipRecord( 104, 11, 12, 0 ) ) ).getId() );
        oldRelationship.setNextProp( add( inUse( new PropertyRecord( 201 ) ) ).getId() );
        RelationshipRecord newRelationship = notInUse( new RelationshipRecord( 42, 0, 0, 0 ) );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = checkChange( oldRelationship, newRelationship );

        // then
        verify( report ).sourcePrevNotUpdated();
        verify( report ).sourceNextNotUpdated();
        verify( report ).targetPrevNotUpdated();
        verify( report ).targetNextNotUpdated();
        verify( report ).propertyNotUpdated();
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportSourcePrevAddedButNodeNotUpdated() throws Exception
    {
        // given
        RelationshipRecord oldRelationship = inUse( new RelationshipRecord( 42, 1, 2, 0 ) );
        RelationshipRecord newRelationship = inUse( new RelationshipRecord( 42, 1, 2, 0 ) );
        add( inUse( new RelationshipTypeTokenRecord( 0 ) ) );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );
        RelationshipRecord prev = addChange( notInUse( new RelationshipRecord( 10, 0, 0, 0 ) ),
                                             inUse( new RelationshipRecord( 10, 1, 3, 0 ) ) );
        newRelationship.setFirstPrevRel( prev.getId() );
        prev.setFirstNextRel( newRelationship.getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = checkChange( oldRelationship, newRelationship );

        // then
        verify( report ).sourceNodeNotUpdated();
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportTargetPrevAddedButNodeNotUpdated() throws Exception
    {
        // given
        RelationshipRecord oldRelationship = inUse( new RelationshipRecord( 42, 1, 2, 0 ) );
        RelationshipRecord newRelationship = inUse( new RelationshipRecord( 42, 1, 2, 0 ) );
        add( inUse( new RelationshipTypeTokenRecord( 0 ) ) );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );
        RelationshipRecord prev = addChange( notInUse( new RelationshipRecord( 10, 0, 0, 0 ) ),
                                             inUse( new RelationshipRecord( 10, 3, 2, 0 ) ) );
        newRelationship.setSecondPrevRel( prev.getId() );
        prev.setSecondNextRel( newRelationship.getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = checkChange( oldRelationship, newRelationship );

        // then
        verify( report ).targetNodeNotUpdated();
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportDeletedFirstButReferencedNodesNotUpdated() throws Exception
    {
        // given
        RelationshipRecord oldRelationship = inUse( new RelationshipRecord( 42, 11, 12, 0 ) );
        RelationshipRecord newRelationship = notInUse( new RelationshipRecord( 42, 0, 0, 0 ) );
        add( inUse( new NodeRecord( 11, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 12, false, 42, NONE ) ) );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = checkChange( oldRelationship, newRelationship );

        // then
        verify( report ).sourceNodeNotUpdated();
        verify( report ).targetNodeNotUpdated();
        verifyNoMoreInteractions( report );
    }
}
