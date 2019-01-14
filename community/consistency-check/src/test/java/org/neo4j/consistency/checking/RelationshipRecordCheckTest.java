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

import org.neo4j.consistency.checking.RelationshipRecordCheck.RelationshipField;
import org.neo4j.consistency.checking.RelationshipRecordCheck.RelationshipTypeField;
import org.neo4j.consistency.checking.full.CheckStage;
import org.neo4j.consistency.checking.full.MultiPassStore;
import org.neo4j.consistency.report.ConsistencyReport.RelationshipConsistencyReport;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.consistency.checking.full.MultiPassStore.NODES;
import static org.neo4j.consistency.checking.full.MultiPassStore.RELATIONSHIPS;

public class RelationshipRecordCheckTest extends
        RecordCheckTestBase<RelationshipRecord, RelationshipConsistencyReport, RelationshipRecordCheck>
{
    private boolean checkSingleDirection;

    public RelationshipRecordCheckTest()
    {
        super( new RelationshipRecordCheck(
                RelationshipTypeField.RELATIONSHIP_TYPE, NodeField.SOURCE, RelationshipField.SOURCE_PREV,
                RelationshipField.SOURCE_NEXT, NodeField.TARGET, RelationshipField.TARGET_PREV,
                RelationshipField.TARGET_NEXT,
                new PropertyChain<>( from -> null ) ),
                RelationshipConsistencyReport.class,
                CheckStage.Stage6_RS_Forward.getCacheSlotSizes(), MultiPassStore.RELATIONSHIPS );
    }

    private void checkSingleDirection()
    {
        this.checkSingleDirection = true;
    }

    @Override
    final RelationshipConsistencyReport check( RelationshipRecord record )
    {
        // Make sure the cache is properly populated
        records.populateCache();
        RelationshipConsistencyReport report = mock( RelationshipConsistencyReport.class );
        records.cacheAccess().setCacheSlotSizes( CheckStage.Stage6_RS_Forward.getCacheSlotSizes() );
        super.check( report, record );
        if ( !checkSingleDirection )
        {
            records.cacheAccess().setForward( !records.cacheAccess().isForward() );
            super.check( report, record );
        }
        return report;
    }

    @Test
    public void shouldNotReportAnythingForRelationshipNotInUse()
    {
        // given
        RelationshipRecord relationship = notInUse( new RelationshipRecord( 42, 0, 0, 0 ) );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldNotReportAnythingForRelationshipThatDoesNotReferenceOtherRecords()
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldNotReportAnythingForRelationshipWithConsistentReferences()
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
        RelationshipConsistencyReport report = check( relationship );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportIllegalRelationshipType()
    {
        // given
        checkSingleDirection();
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, NONE ) );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).illegalRelationshipType();
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportRelationshipTypeNotInUse()
    {
        // given
        checkSingleDirection();
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        RelationshipTypeTokenRecord relationshipType = add( notInUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).relationshipTypeNotInUse( relationshipType );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportIllegalSourceNode()
    {
        // given
        checkSingleDirection();
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, NONE, 1, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).illegalSourceNode();
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportSourceNodeNotInUse()
    {
        // given
        checkSingleDirection();
        initialize( RELATIONSHIPS, NODES );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        NodeRecord node = add( notInUse( new NodeRecord( 1, false, NONE, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).sourceNodeNotInUse( node );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportIllegalTargetNode()
    {
        // given
        checkSingleDirection();
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, NONE, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).illegalTargetNode();
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportTargetNodeNotInUse()
    {
        // given
        checkSingleDirection();
        initialize( RELATIONSHIPS, NODES );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        NodeRecord node = add( notInUse( new NodeRecord( 2, false, NONE, NONE ) ) );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).targetNodeNotInUse( node );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportPropertyNotInUse()
    {
        // given
        checkSingleDirection();
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        relationship.setNextProp( 11 );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );
        PropertyRecord property = add( notInUse( new PropertyRecord( 11 ) ) );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).propertyNotInUse( property );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportPropertyNotFirstInChain()
    {
        // given
        checkSingleDirection();
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        relationship.setNextProp( 11 );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );
        PropertyRecord property = add( inUse( new PropertyRecord( 11 ) ) );
        property.setPrevProp( 6 );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).propertyNotFirstInChain( property );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportSourceNodeNotReferencingBackForFirstRelationshipInSourceChain()
    {
        // given
        checkSingleDirection();
        initialize( RELATIONSHIPS, NODES );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        NodeRecord source = add( inUse( new NodeRecord( 1, false, 7, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).sourceNodeDoesNotReferenceBack( source );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportTargetNodeNotReferencingBackForFirstRelationshipInTargetChain()
    {
        // given
        checkSingleDirection();
        initialize( RELATIONSHIPS, NODES );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        NodeRecord target = add( inUse( new NodeRecord( 2, false, 7, NONE ) ) );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).targetNodeDoesNotReferenceBack( target );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportSourceAndTargetNodeNotReferencingBackForFirstRelationshipInChains()
    {
        // given
        checkSingleDirection();
        initialize( RELATIONSHIPS, NODES );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        NodeRecord source = add( inUse( new NodeRecord( 1, false, NONE, NONE ) ) );
        NodeRecord target = add( inUse( new NodeRecord( 2, false, NONE, NONE ) ) );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).sourceNodeDoesNotReferenceBack( source );
        verify( report ).targetNodeDoesNotReferenceBack( target );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportSourceNodeWithoutChainForRelationshipInTheMiddleOfChain()
    {
        // given
        checkSingleDirection();
        initialize( RELATIONSHIPS, NODES );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        NodeRecord source = add( inUse( new NodeRecord( 1, false, NONE, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );
        RelationshipRecord sPrev = add( inUse( new RelationshipRecord( 51, 1, 0, 0 ) ) );
        relationship.setFirstPrevRel( sPrev.getId() );
        relationship.setFirstInFirstChain( false );
        sPrev.setFirstNextRel( relationship.getId() );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).sourceNodeHasNoRelationships( source );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportTargetNodeWithoutChainForRelationshipInTheMiddleOfChain()
    {
        // given
        checkSingleDirection();
        initialize( RELATIONSHIPS, NODES );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        NodeRecord target = add( inUse( new NodeRecord( 2, false, NONE, NONE ) ) );
        RelationshipRecord tPrev = add( inUse( new RelationshipRecord( 51, 0, 2, 0 ) ) );
        relationship.setSecondPrevRel( tPrev.getId() );
        relationship.setFirstInSecondChain( false );
        tPrev.setSecondNextRel( relationship.getId() );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).targetNodeHasNoRelationships( target );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportSourcePrevReferencingOtherNodes()
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
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).sourcePrevReferencesOtherNodes( sPrev );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportTargetPrevReferencingOtherNodes()
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
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).targetPrevReferencesOtherNodes( tPrev );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportSourceNextReferencingOtherNodes()
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );
        RelationshipRecord sNext = add( inUse( new RelationshipRecord( 51, 8, 9, 0 ) ) );
        relationship.setFirstNextRel( sNext.getId() );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).sourceNextReferencesOtherNodes( sNext );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportTargetNextReferencingOtherNodes()
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );
        RelationshipRecord tNext = add( inUse( new RelationshipRecord( 51, 8, 9, 0 ) ) );
        relationship.setSecondNextRel( tNext.getId() );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).targetNextReferencesOtherNodes( tNext );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportSourcePrevReferencingOtherNodesWhenReferencingTargetNode()
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
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).sourcePrevReferencesOtherNodes( sPrev );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportTargetPrevReferencingOtherNodesWhenReferencingSourceNode()
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
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).targetPrevReferencesOtherNodes( tPrev );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportSourceNextReferencingOtherNodesWhenReferencingTargetNode()
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );
        RelationshipRecord sNext = add( inUse( new RelationshipRecord( 51, 2, 0, 0 ) ) );
        relationship.setFirstNextRel( sNext.getId() );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).sourceNextReferencesOtherNodes( sNext );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportTargetNextReferencingOtherNodesWhenReferencingSourceNode()
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );
        RelationshipRecord tNext = add( inUse( new RelationshipRecord( 51, 1, 0, 0 ) ) );
        relationship.setSecondNextRel( tNext.getId() );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).targetNextReferencesOtherNodes( tNext );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportSourcePrevNotReferencingBack()
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
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).sourcePrevDoesNotReferenceBack( sPrev );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportTargetPrevNotReferencingBack()
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
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).targetPrevDoesNotReferenceBack( tPrev );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportSourceNextNotReferencingBack()
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );
        RelationshipRecord sNext = add( inUse( new RelationshipRecord( 51, 3, 1, 0 ) ) );
        relationship.setFirstNextRel( sNext.getId() );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).sourceNextDoesNotReferenceBack( sNext );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportTargetNextNotReferencingBack()
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1, false, 42, NONE ) ) );
        add( inUse( new NodeRecord( 2, false, 42, NONE ) ) );
        RelationshipRecord tNext = add( inUse( new RelationshipRecord( 51, 3, 2, 0 ) ) );
        relationship.setSecondNextRel( tNext.getId() );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).targetNextDoesNotReferenceBack( tNext );
        verifyNoMoreInteractions( report );
    }
}
