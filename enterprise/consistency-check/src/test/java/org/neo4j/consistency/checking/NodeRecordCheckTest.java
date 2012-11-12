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

import org.junit.Test;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

import static org.mockito.Mockito.verify;

public class NodeRecordCheckTest
        extends RecordCheckTestBase<NodeRecord, ConsistencyReport.NodeConsistencyReport, NodeRecordCheck>
{
    public NodeRecordCheckTest()
    {
        super( new NodeRecordCheck(), ConsistencyReport.NodeConsistencyReport.class );
    }

    @Test
    public void shouldNotReportAnythingForNodeNotInUse() throws Exception
    {
        // given
        NodeRecord node = notInUse( new NodeRecord( 42, 0, 0 ) );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldNotReportAnythingForNodeThatDoesNotReferenceOtherRecords() throws Exception
    {
        // given
        NodeRecord node = inUse( new NodeRecord( 42, NONE, NONE ) );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldNotReportAnythingForNodeWithConsistentReferences() throws Exception
    {
        // given
        NodeRecord node = inUse( new NodeRecord( 42, 7, 11 ) );
        add( inUse( new RelationshipRecord( 7, 42, 0, 0 ) ) );
        add( inUse( new PropertyRecord( 11 ) ) );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportRelationshipNotInUse() throws Exception
    {
        // given
        NodeRecord node = inUse( new NodeRecord( 42, 7, 11 ) );
        RelationshipRecord relationship = add( notInUse( new RelationshipRecord( 7, 0, 0, 0 ) ) );
        add( inUse( new PropertyRecord( 11 ) ) );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verify( report ).relationshipNotInUse( relationship );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportPropertyNotInUse() throws Exception
    {
        // given
        NodeRecord node = inUse( new NodeRecord( 42, NONE, 11 ) );
        PropertyRecord property = add( notInUse( new PropertyRecord( 11 ) ) );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verify( report ).propertyNotInUse( property );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportPropertyNotFirstInChain() throws Exception
    {
        // given
        NodeRecord node = inUse( new NodeRecord( 42, NONE, 11 ) );
        PropertyRecord property = add( inUse( new PropertyRecord( 11 ) ) );
        property.setPrevProp( 6 );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verify( report ).propertyNotFirstInChain( property );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportRelationshipForOtherNodes() throws Exception
    {
        // given
        NodeRecord node = inUse( new NodeRecord( 42, 7, NONE ) );
        RelationshipRecord relationship = add( inUse( new RelationshipRecord( 7, 1, 2, 0 ) ) );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verify( report ).relationshipForOtherNode( relationship );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportRelationshipNotFirstInSourceChain() throws Exception
    {
        // given
        NodeRecord node = inUse( new NodeRecord( 42, 7, NONE ) );
        RelationshipRecord relationship = add( inUse( new RelationshipRecord( 7, 42, 0, 0 ) ) );
        relationship.setFirstPrevRel( 6 );
        relationship.setSecondPrevRel( 8 );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verify( report ).relationshipNotFirstInSourceChain( relationship );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportRelationshipNotFirstInTargetChain() throws Exception
    {
        // given
        NodeRecord node = inUse( new NodeRecord( 42, 7, NONE ) );
        RelationshipRecord relationship = add( inUse( new RelationshipRecord( 7, 0, 42, 0 ) ) );
        relationship.setFirstPrevRel( 6 );
        relationship.setSecondPrevRel( 8 );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verify( report ).relationshipNotFirstInTargetChain( relationship );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportLoopRelationshipNotFirstInTargetAndSourceChains() throws Exception
    {
        // given
        NodeRecord node = inUse( new NodeRecord( 42, 7, NONE ) );
        RelationshipRecord relationship = add( inUse( new RelationshipRecord( 7, 42, 42, 0 ) ) );
        relationship.setFirstPrevRel( 8 );
        relationship.setSecondPrevRel( 8 );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verify( report ).relationshipNotFirstInSourceChain( relationship );
        verify( report ).relationshipNotFirstInTargetChain( relationship );
        verifyOnlyReferenceDispatch( report );
    }

    // change checking

    @Test
    public void shouldNotReportAnythingForConsistentlyChangedNode() throws Exception
    {
        // given
        NodeRecord oldNode = inUse( new NodeRecord( 42, 11, 1 ) );
        NodeRecord newNode = inUse( new NodeRecord( 42, 12, 2 ) );

        addChange( inUse( new RelationshipRecord( 11, 42, 0, 0 ) ),
                   notInUse( new RelationshipRecord( 11, 0, 0, 0 ) ) );
        addChange( notInUse( new RelationshipRecord( 12, 0, 0, 0 ) ),
                   inUse( new RelationshipRecord( 12, 42, 0, 0 ) ) );

        addChange( inUse( new PropertyRecord( 1 ) ),
                   notInUse( new PropertyRecord( 1 ) ) );
        addChange( notInUse( new PropertyRecord( 2 ) ),
                   inUse( new PropertyRecord( 2 ) ) );

        // when
        ConsistencyReport.NodeConsistencyReport report = checkChange( oldNode, newNode );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportProblemsWithTheNewStateWhenCheckingChanges() throws Exception
    {
        // given
        NodeRecord oldNode = notInUse( new NodeRecord( 42, 0, 0 ) );
        NodeRecord newNode = inUse( new NodeRecord( 42, 1, 2 ) );
        RelationshipRecord relationship = add( notInUse( new RelationshipRecord( 1, 0, 0, 0 ) ) );
        PropertyRecord property = add( notInUse( new PropertyRecord( 2 ) ) );

        // when
        ConsistencyReport.NodeConsistencyReport report = checkChange( oldNode, newNode );

        // then
        verify( report ).relationshipNotInUse( relationship );
        verify( report ).propertyNotInUse( property );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldNotReportAnythingWhenAddingAnInitialProperty() throws Exception
    {
        // given
        NodeRecord oldNode = inUse( new NodeRecord( 42, NONE, NONE ) );
        NodeRecord newNode = inUse( new NodeRecord( 42, NONE, 10 ) );

       addChange( notInUse( new PropertyRecord( 10 ) ), inUse( new PropertyRecord( 10 ) ) );

        // when
        ConsistencyReport.NodeConsistencyReport report = checkChange( oldNode, newNode );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldNotReportAnythingWhenChangingProperty() throws Exception
    {
        // given
        NodeRecord oldNode = inUse( new NodeRecord( 42, NONE, 10 ) );
        NodeRecord newNode = inUse( new NodeRecord( 42, NONE, 11 ) );

        PropertyRecord oldProp = addChange( inUse( new PropertyRecord( 10 ) ),
                                            inUse( new PropertyRecord( 10 ) ) );
        PropertyRecord newProp = addChange( notInUse( new PropertyRecord( 11 ) ),
                                            inUse( new PropertyRecord( 11 ) ) );
        oldProp.setPrevProp( newProp.getId() );
        newProp.setNextProp( oldProp.getId() );

        // when
        ConsistencyReport.NodeConsistencyReport report = checkChange( oldNode, newNode );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldNotReportAnythingWhenAddingAnInitialRelationship() throws Exception
    {
        // given
        NodeRecord oldNode = inUse( new NodeRecord( 42, NONE, NONE ) );
        NodeRecord newNode = inUse( new NodeRecord( 42, 10, NONE ) );

        addChange( notInUse( new RelationshipRecord( 10, 0, 0, 0 ) ),
                   inUse( new RelationshipRecord( 10, 42, 1, 0 ) ) );

        // when
        ConsistencyReport.NodeConsistencyReport report = checkChange( oldNode, newNode );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldNotReportAnythingWhenChangingRelationship() throws Exception
    {
        // given
        NodeRecord oldNode = inUse( new NodeRecord( 42, 9, NONE ) );
        NodeRecord newNode = inUse( new NodeRecord( 42, 10, NONE ) );

        RelationshipRecord rel1 = addChange( inUse( new RelationshipRecord( 9, 42, 0, 0 ) ),
                                             inUse( new RelationshipRecord( 9, 42, 0, 0 ) ) );
        RelationshipRecord rel2 = addChange( notInUse( new RelationshipRecord( 10, 0, 0, 0 ) ),
                                             inUse( new RelationshipRecord( 10, 42, 1, 0 ) ) );
        rel1.setFirstPrevRel( rel2.getId() );
        rel2.setFirstNextRel( rel1.getId() );

        // when
        ConsistencyReport.NodeConsistencyReport report = checkChange( oldNode, newNode );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportPropertyChainReplacedButNotUpdated() throws Exception
    {
        // given
        NodeRecord oldNode = inUse( new NodeRecord( 42, NONE, 1 ) );
        NodeRecord newNode = inUse( new NodeRecord( 42, NONE, 2 ) );
        addChange( notInUse( new PropertyRecord( 2 ) ),
                   inUse( new PropertyRecord( 2 ) ) );

        // when
        ConsistencyReport.NodeConsistencyReport report = checkChange( oldNode, newNode );

        // then
        verify( report ).propertyNotUpdated();
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportRelationshipChainReplacedButNotUpdated() throws Exception
    {
        // given
        NodeRecord oldNode = inUse( new NodeRecord( 42, 1, NONE ) );
        NodeRecord newNode = inUse( new NodeRecord( 42, 2, NONE ) );
        addChange( notInUse( new RelationshipRecord( 2, 0, 0, 0 ) ),
                   inUse( new RelationshipRecord( 2, 42, 0, 0 ) ) );

        // when
        ConsistencyReport.NodeConsistencyReport report = checkChange( oldNode, newNode );

        // then
        verify( report ).relationshipNotUpdated();
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportDeletedButReferencesNotUpdated() throws Exception
    {
        // given
        NodeRecord oldNode = inUse( new NodeRecord( 42, 1, 10 ) );
        NodeRecord newNode = notInUse( new NodeRecord( 42, 1, 10 ) );

        // when
        ConsistencyReport.NodeConsistencyReport report = checkChange( oldNode, newNode );

        // then
        verify( report ).relationshipNotUpdated();
        verify( report ).propertyNotUpdated();
        verifyOnlyReferenceDispatch( report );
    }
}
