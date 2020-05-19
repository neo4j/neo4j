/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.junit.jupiter.api.Test;

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

class RelationshipRecordCheckTest extends
        RecordCheckTestBase<RelationshipRecord, RelationshipConsistencyReport, RelationshipRecordCheck>
{
    private boolean checkSingleDirection;

    RelationshipRecordCheckTest()
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
    void shouldNotReportAnythingForRelationshipNotInUse()
    {
        // given
        RelationshipRecord relationship = notInUse( new RelationshipRecord( 42 ) );
        relationship.setLinks( 0, 0, 0 );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    void shouldNotReportAnythingForRelationshipThatDoesNotReferenceOtherRecords()
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42 ) );
        relationship.setLinks( 1, 2, 4 );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1 ).initialize( false, NONE, false, 42, 0 ) ) );
        add( inUse( new NodeRecord( 2 ).initialize( false, NONE, false, 42, 0 ) ) );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    void shouldNotReportAnythingForRelationshipWithConsistentReferences()
    {
        // given
        /*
         * (1) --> (3) <==> (2)
         */
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42 ) );
        relationship.setLinks( 1, 2, 4 );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1 ).initialize( false, NONE, false, relationship.getId(), 0 ) ) );
        add( inUse( new NodeRecord( 2 ).initialize( false, NONE, false, 53, 0 ) ) );
        add( inUse( new NodeRecord( 3 ).initialize( false, NONE, false, NONE, 0 ) ) );
        add( inUse( new PropertyRecord( 101 ) ) );
        relationship.setNextProp( 101 );

        RelationshipRecord sNext = add( inUse( new RelationshipRecord( 51 ) ) );
        sNext.setLinks( 1, 3, 4 );
        RelationshipRecord tNext = add( inUse( new RelationshipRecord( 52 ) ) );
        tNext.setLinks( 2, 3, 4 );
        RelationshipRecord tPrev = add( inUse( new RelationshipRecord( 53 ) ) );
        tPrev.setLinks( 3, 2, 4 );

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
    void shouldReportIllegalRelationshipType()
    {
        // given
        checkSingleDirection();
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42 ) );
        relationship.setLinks( 1, 2, NONE );
        add( inUse( new NodeRecord( 1 ).initialize( false, NONE, false, 42, 0 ) ) );
        add( inUse( new NodeRecord( 2 ).initialize( false, NONE, false, 42, 0 ) ) );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).illegalRelationshipType();
        verifyNoMoreInteractions( report );
    }

    @Test
    void shouldReportRelationshipTypeNotInUse()
    {
        // given
        checkSingleDirection();
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42 ) );
        relationship.setLinks( 1, 2, 4 );
        RelationshipTypeTokenRecord relationshipType = add( notInUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1 ).initialize( false, NONE, false, 42, 0 ) ) );
        add( inUse( new NodeRecord( 2 ).initialize( false, NONE, false, 42, 0 ) ) );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).relationshipTypeNotInUse( relationshipType );
        verifyNoMoreInteractions( report );
    }

    @Test
    void shouldReportIllegalSourceNode()
    {
        // given
        checkSingleDirection();
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42 ) );
        relationship.setLinks( NONE, 1, 4 );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1 ).initialize( false, NONE, false, 42, 0 ) ) );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).illegalSourceNode();
        verifyNoMoreInteractions( report );
    }

    @Test
    void shouldReportSourceNodeNotInUse()
    {
        // given
        checkSingleDirection();
        initialize( RELATIONSHIPS, NODES );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42 ) );
        relationship.setLinks( 1, 2, 4 );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        NodeRecord node = add( notInUse( new NodeRecord( 1 ).initialize( false, NONE, false, NONE, 0 ) ) );
        add( inUse( new NodeRecord( 2 ).initialize( false, NONE, false, 42, 0 ) ) );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).sourceNodeNotInUse( node );
        verifyNoMoreInteractions( report );
    }

    @Test
    void shouldReportIllegalTargetNode()
    {
        // given
        checkSingleDirection();
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42 ) );
        relationship.setLinks( 1, NONE, 4 );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1 ).initialize( false, NONE, false, 42, 0 ) ) );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).illegalTargetNode();
        verifyNoMoreInteractions( report );
    }

    @Test
    void shouldReportTargetNodeNotInUse()
    {
        // given
        checkSingleDirection();
        initialize( RELATIONSHIPS, NODES );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42 ) );
        relationship.setLinks( 1, 2, 4 );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1 ).initialize( false, NONE, false, 42, 0 ) ) );
        NodeRecord node = add( notInUse( new NodeRecord( 2 ).initialize( false, NONE, false, NONE, 0 ) ) );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).targetNodeNotInUse( node );
        verifyNoMoreInteractions( report );
    }

    @Test
    void shouldReportPropertyNotInUse()
    {
        // given
        checkSingleDirection();
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42 ) );
        relationship.setLinks( 1, 2, 4 );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        relationship.setNextProp( 11 );
        add( inUse( new NodeRecord( 1 ).initialize( false, NONE, false, 42, 0 ) ) );
        add( inUse( new NodeRecord( 2 ).initialize( false, NONE, false, 42, 0 ) ) );
        PropertyRecord property = add( notInUse( new PropertyRecord( 11 ) ) );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).propertyNotInUse( property );
        verifyNoMoreInteractions( report );
    }

    @Test
    void shouldReportPropertyNotFirstInChain()
    {
        // given
        checkSingleDirection();
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42 ) );
        relationship.setLinks( 1, 2, 4 );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        relationship.setNextProp( 11 );
        add( inUse( new NodeRecord( 1 ).initialize( false, NONE, false, 42, 0 ) ) );
        add( inUse( new NodeRecord( 2 ).initialize( false, NONE, false, 42, 0 ) ) );
        PropertyRecord property = add( inUse( new PropertyRecord( 11 ) ) );
        property.setPrevProp( 6 );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).propertyNotFirstInChain( property );
        verifyNoMoreInteractions( report );
    }

    @Test
    void shouldReportSourceNodeNotReferencingBackForFirstRelationshipInSourceChain()
    {
        // given
        checkSingleDirection();
        initialize( RELATIONSHIPS, NODES );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42 ) );
        relationship.setLinks( 1, 2, 4 );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        NodeRecord source = add( inUse( new NodeRecord( 1 ).initialize( false, NONE, false, 7, 0 ) ) );
        add( inUse( new NodeRecord( 2 ).initialize( false, NONE, false, 42, 0 ) ) );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).sourceNodeDoesNotReferenceBack( source );
        verifyNoMoreInteractions( report );
    }

    @Test
    void shouldReportTargetNodeNotReferencingBackForFirstRelationshipInTargetChain()
    {
        // given
        checkSingleDirection();
        initialize( RELATIONSHIPS, NODES );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42 ) );
        relationship.setLinks( 1, 2, 4 );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1 ).initialize( false, NONE, false, 42, 0 ) ) );
        NodeRecord target = add( inUse( new NodeRecord( 2 ).initialize( false, NONE, false, 7, 0 ) ) );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).targetNodeDoesNotReferenceBack( target );
        verifyNoMoreInteractions( report );
    }

    @Test
    void shouldReportSourceAndTargetNodeNotReferencingBackForFirstRelationshipInChains()
    {
        // given
        checkSingleDirection();
        initialize( RELATIONSHIPS, NODES );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42 ) );
        relationship.setLinks( 1, 2, 4 );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        NodeRecord source = add( inUse( new NodeRecord( 1 ).initialize( false, NONE, false, NONE, 0 ) ) );
        NodeRecord target = add( inUse( new NodeRecord( 2 ).initialize( false, NONE, false, NONE, 0 ) ) );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).sourceNodeDoesNotReferenceBack( source );
        verify( report ).targetNodeDoesNotReferenceBack( target );
        verifyNoMoreInteractions( report );
    }

    @Test
    void shouldReportSourceNodeWithoutChainForRelationshipInTheMiddleOfChain()
    {
        // given
        checkSingleDirection();
        initialize( RELATIONSHIPS, NODES );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42 ) );
        relationship.setLinks( 1, 2, 4 );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        NodeRecord source = add( inUse( new NodeRecord( 1 ).initialize( false, NONE, false, NONE, 0 ) ) );
        add( inUse( new NodeRecord( 2 ).initialize( false, NONE, false, 42, 0 ) ) );
        RelationshipRecord sPrev = add( inUse( new RelationshipRecord( 51 ) ) );
        sPrev.setLinks( 1, 0, 0 );
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
    void shouldReportTargetNodeWithoutChainForRelationshipInTheMiddleOfChain()
    {
        // given
        checkSingleDirection();
        initialize( RELATIONSHIPS, NODES );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42 ) );
        relationship.setLinks( 1, 2, 4 );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1 ).initialize( false, NONE, false, 42, 0 ) ) );
        NodeRecord target = add( inUse( new NodeRecord( 2 ).initialize( false, NONE, false, NONE, 0 ) ) );
        RelationshipRecord tPrev = add( inUse( new RelationshipRecord( 51 ) ) );
        tPrev.setLinks( 0, 2, 0 );
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
    void shouldReportSourcePrevReferencingOtherNodes()
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42 ) );
        relationship.setLinks( 1, 2, 4 );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1 ).initialize( false, NONE, false, 0, 0 ) ) );
        add( inUse( new NodeRecord( 2 ).initialize( false, NONE, false, 42, 0 ) ) );
        RelationshipRecord sPrev = add( inUse( new RelationshipRecord( 51 ) ) );
        sPrev.setLinks( 8, 9, 0 );
        relationship.setFirstPrevRel( sPrev.getId() );
        relationship.setFirstInFirstChain( false );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).sourcePrevReferencesOtherNodes( sPrev );
        verifyNoMoreInteractions( report );
    }

    @Test
    void shouldReportTargetPrevReferencingOtherNodes()
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42 ) );
        relationship.setLinks( 1, 2, 4 );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1 ).initialize( false, NONE, false, 42, 0 ) ) );
        add( inUse( new NodeRecord( 2 ).initialize( false, NONE, false, 0, 0 ) ) );
        RelationshipRecord tPrev = add( inUse( new RelationshipRecord( 51 ) ) );
        tPrev.setLinks( 8, 9, 0 );
        relationship.setSecondPrevRel( tPrev.getId() );
        relationship.setFirstInSecondChain( false );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).targetPrevReferencesOtherNodes( tPrev );
        verifyNoMoreInteractions( report );
    }

    @Test
    void shouldReportSourceNextReferencingOtherNodes()
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42 ) );
        relationship.setLinks( 1, 2, 4 );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1 ).initialize( false, NONE, false, 42, 0 ) ) );
        add( inUse( new NodeRecord( 2 ).initialize( false, NONE, false, 42, 0 ) ) );
        RelationshipRecord sNext = add( inUse( new RelationshipRecord( 51 ) ) );
        sNext.setLinks( 8, 9, 0 );
        relationship.setFirstNextRel( sNext.getId() );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).sourceNextReferencesOtherNodes( sNext );
        verifyNoMoreInteractions( report );
    }

    @Test
    void shouldReportTargetNextReferencingOtherNodes()
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42 ) );
        relationship.setLinks( 1, 2, 4 );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1 ).initialize( false, NONE, false, 42, 0 ) ) );
        add( inUse( new NodeRecord( 2 ).initialize( false, NONE, false, 42, 0 ) ) );
        RelationshipRecord tNext = add( inUse( new RelationshipRecord( 51 ) ) );
        tNext.setLinks( 8, 9, 0 );
        relationship.setSecondNextRel( tNext.getId() );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).targetNextReferencesOtherNodes( tNext );
        verifyNoMoreInteractions( report );
    }

    @Test
    void shouldReportSourcePrevReferencingOtherNodesWhenReferencingTargetNode()
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42 ) );
        relationship.setLinks( 1, 2, 4 );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1 ).initialize( false, NONE, false, 0, 0 ) ) );
        add( inUse( new NodeRecord( 2 ).initialize( false, NONE, false, 42, 0 ) ) );
        RelationshipRecord sPrev = add( inUse( new RelationshipRecord( 51 ) ) );
        sPrev.setLinks( 2, 0, 0 );
        relationship.setFirstPrevRel( sPrev.getId() );
        relationship.setFirstInFirstChain( false );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).sourcePrevReferencesOtherNodes( sPrev );
        verifyNoMoreInteractions( report );
    }

    @Test
    void shouldReportTargetPrevReferencingOtherNodesWhenReferencingSourceNode()
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42 ) );
        relationship.setLinks( 1, 2, 4 );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1 ).initialize( false, NONE, false, 42, 0 ) ) );
        add( inUse( new NodeRecord( 2 ).initialize( false, NONE, false, 0, 0 ) ) );
        RelationshipRecord tPrev = add( inUse( new RelationshipRecord( 51 ) ) );
        tPrev.setLinks( 1, 0, 0 );
        relationship.setSecondPrevRel( tPrev.getId() );
        relationship.setFirstInSecondChain( false );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).targetPrevReferencesOtherNodes( tPrev );
        verifyNoMoreInteractions( report );
    }

    @Test
    void shouldReportSourceNextReferencingOtherNodesWhenReferencingTargetNode()
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42 ) );
        relationship.setLinks( 1, 2, 4 );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1 ).initialize( false, NONE, false, 42, 0 ) ) );
        add( inUse( new NodeRecord( 2 ).initialize( false, NONE, false, 42, 0 ) ) );
        RelationshipRecord sNext = add( inUse( new RelationshipRecord( 51 ) ) );
        sNext.setLinks( 2, 0, 0 );
        relationship.setFirstNextRel( sNext.getId() );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).sourceNextReferencesOtherNodes( sNext );
        verifyNoMoreInteractions( report );
    }

    @Test
    void shouldReportTargetNextReferencingOtherNodesWhenReferencingSourceNode()
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42 ) );
        relationship.setLinks( 1, 2, 4 );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1 ).initialize( false, NONE, false, 42, 0 ) ) );
        add( inUse( new NodeRecord( 2 ).initialize( false, NONE, false, 42, 0 ) ) );
        RelationshipRecord tNext = add( inUse( new RelationshipRecord( 51 ) ) );
        tNext.setLinks( 1, 0, 0 );
        relationship.setSecondNextRel( tNext.getId() );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).targetNextReferencesOtherNodes( tNext );
        verifyNoMoreInteractions( report );
    }

    @Test
    void shouldReportSourcePrevNotReferencingBack()
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42 ) );
        relationship.setLinks( 1, 2, 4 );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1 ).initialize( false, NONE, false, 0, 0 ) ) );
        add( inUse( new NodeRecord( 2 ).initialize( false, NONE, false, 42, 0 ) ) );
        RelationshipRecord sPrev = add( inUse( new RelationshipRecord( 51 ) ) );
        sPrev.setLinks( 1, 3, 0 );
        relationship.setFirstPrevRel( sPrev.getId() );
        relationship.setFirstInFirstChain( false );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).sourcePrevDoesNotReferenceBack( sPrev );
        verifyNoMoreInteractions( report );
    }

    @Test
    void shouldReportTargetPrevNotReferencingBack()
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42 ) );
        relationship.setLinks( 1, 2, 4 );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1 ).initialize( false, NONE, false, 42, 0 ) ) );
        add( inUse( new NodeRecord( 2 ).initialize( false, NONE, false, 0, 0 ) ) );
        RelationshipRecord tPrev = add( inUse( new RelationshipRecord( 51 ) ) );
        tPrev.setLinks( 2, 3, 0 );
        relationship.setSecondPrevRel( tPrev.getId() );
        relationship.setFirstInSecondChain( false );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).targetPrevDoesNotReferenceBack( tPrev );
        verifyNoMoreInteractions( report );
    }

    @Test
    void shouldReportSourceNextNotReferencingBack()
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42 ) );
        relationship.setLinks( 1, 2, 4 );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1 ).initialize( false, NONE, false, 42, 0 ) ) );
        add( inUse( new NodeRecord( 2 ).initialize( false, NONE, false, 42, 0 ) ) );
        RelationshipRecord sNext = add( inUse( new RelationshipRecord( 51 ) ) );
        sNext.setLinks( 3, 1, 0 );
        relationship.setFirstNextRel( sNext.getId() );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).sourceNextDoesNotReferenceBack( sNext );
        verifyNoMoreInteractions( report );
    }

    @Test
    void shouldReportTargetNextNotReferencingBack()
    {
        // given
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42 ) );
        relationship.setLinks( 1, 2, 4 );
        add( inUse( new RelationshipTypeTokenRecord( 4 ) ) );
        add( inUse( new NodeRecord( 1 ).initialize( false, NONE, false, 42, 0 ) ) );
        add( inUse( new NodeRecord( 2 ).initialize( false, NONE, false, 42, 0 ) ) );
        RelationshipRecord tNext = add( inUse( new RelationshipRecord( 51 ) ) );
        tNext.setLinks( 3, 2, 0 );
        relationship.setSecondNextRel( tNext.getId() );

        // when
        RelationshipConsistencyReport report = check( relationship );

        // then
        verify( report ).targetNextDoesNotReferenceBack( tNext );
        verifyNoMoreInteractions( report );
    }
}
