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

import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.legacy.consistency.report.ConsistencyReport;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class PropertyRecordCheckTest
        extends RecordCheckTestBase<PropertyRecord, ConsistencyReport.PropertyConsistencyReport, PropertyRecordCheck>
{
    public PropertyRecordCheckTest()
    {
        super( new PropertyRecordCheck(), ConsistencyReport.PropertyConsistencyReport.class );
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

    // change checking

    @Test
    public void shouldNotReportAnythingForConsistentlyChangedProperty() throws Exception
    {
        // given
        PropertyRecord oldProperty = inUse( new PropertyRecord( 42 ) );
        oldProperty.setPrevProp( 1 );
        oldProperty.setNextProp( 2 );
        PropertyRecord newProperty = inUse( new PropertyRecord( 42 ) );
        newProperty.setPrevProp( 11 );
        newProperty.setNextProp( 12 );
        newProperty.setNodeId( addChange( inUse( new NodeRecord( 100, false, NONE, 1 ) ),
                                          inUse( new NodeRecord( 100, false, NONE, 11 ) ) ).getId() );

        PropertyRecord oldPrev = inUse( new PropertyRecord( 1 ) );
        addChange( oldPrev, notInUse( new PropertyRecord( 1 ) ) );
        oldPrev.setNextProp( 42 );
        addChange( inUse( new PropertyRecord( 2 ) ),
                   notInUse( new PropertyRecord( 2 ) ) );

        addChange( notInUse( new PropertyRecord( 11 ) ),
                   inUse( new PropertyRecord( 11 ) ) ).setNextProp( 42 );
        addChange( notInUse( new PropertyRecord( 12 ) ),
                   inUse( new PropertyRecord( 12 ) ) ).setPrevProp( 42 );

        // when
        ConsistencyReport.PropertyConsistencyReport report = checkChange( oldProperty, newProperty );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportProblemsWithTheNewStateWhenCheckingChanges() throws Exception
    {
        // given
        PropertyRecord oldProperty = notInUse( new PropertyRecord( 42 ) );
        PropertyRecord newProperty = inUse( new PropertyRecord( 42 ) );
        newProperty.setNodeId( add( notInUse( new NodeRecord( 10, false, 0, 0 ) ) ).getId() );
        newProperty.setPrevProp( 1 );
        newProperty.setNextProp( 2 );
        PropertyRecord prev = add( notInUse( new PropertyRecord( 1 ) ) );
        PropertyRecord next = add( notInUse( new PropertyRecord( 2 ) ) );

        // when
        ConsistencyReport.PropertyConsistencyReport report = checkChange( oldProperty, newProperty );

        // then
        verify( report ).prevNotInUse( prev );
        verify( report ).nextNotInUse( next );
        verify( report ).ownerDoesNotReferenceBack();
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldNotReportAnythingWhenAddingAnInitialNextProperty() throws Exception
    {
        // given
        PropertyRecord oldProperty = inUse( new PropertyRecord( 42 ) );
        PropertyRecord newProperty = inUse( new PropertyRecord( 42 ) );
        PropertyRecord nextProperty = addChange( notInUse( new PropertyRecord( 1 ) ),
                                                 inUse( new PropertyRecord( 1 ) ) );
        nextProperty.setPrevProp( 42 );
        newProperty.setNextProp( nextProperty.getId() );

        newProperty.setNodeId( add( inUse( new NodeRecord( 100, false, NONE, newProperty.getId() ) ) ).getId() );

        // when
        ConsistencyReport.PropertyConsistencyReport report = checkChange( oldProperty, newProperty );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldNotReportAnythingWhenAddingAnInitialPrevProperty() throws Exception
    {
        // given
        PropertyRecord oldProperty = inUse( new PropertyRecord( 42 ) );
        PropertyRecord newProperty = inUse( new PropertyRecord( 42 ) );
        PropertyRecord prevProperty = addChange( notInUse( new PropertyRecord( 1 ) ),
                                                 inUse( new PropertyRecord( 1 ) ) );
        prevProperty.setNextProp( 42 );
        newProperty.setPrevProp( prevProperty.getId() );

        newProperty.setNodeId( addChange( inUse( new NodeRecord( 100, false, NONE, oldProperty.getId() ) ),
                                          inUse( new NodeRecord( 100, false, NONE, prevProperty.getId() ) ) ).getId() );

        // when
        ConsistencyReport.PropertyConsistencyReport report = checkChange( oldProperty, newProperty );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldNotReportAnythingWhenChangingNextProperty() throws Exception
    {
        // given
        PropertyRecord oldProperty = inUse( new PropertyRecord( 42 ) );
        PropertyRecord newProperty = inUse( new PropertyRecord( 42 ) );
        PropertyRecord oldNext = inUse( new PropertyRecord( 1 ) );
        addChange( oldNext, inUse( new PropertyRecord( 1 ) ) );
        PropertyRecord newNext = addChange( notInUse( new PropertyRecord( 2 ) ),
                                            inUse( new PropertyRecord( 2 ) ));
        oldProperty.setNextProp( oldNext.getId() );
        oldNext.setPrevProp( 42 );
        newProperty.setNextProp( newNext.getId() );
        newNext.setPrevProp( newProperty.getId() );

        newProperty.setNodeId( add( inUse( new NodeRecord( 100, false, NONE, newProperty.getId() ) ) ).getId() );

        // when
        ConsistencyReport.PropertyConsistencyReport report = checkChange( oldProperty, newProperty );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldNotReportAnythingWhenChangingPrevProperty() throws Exception
    {
        // given
        PropertyRecord oldProperty = inUse( new PropertyRecord( 42 ) );
        PropertyRecord newProperty = inUse( new PropertyRecord( 42 ) );
        PropertyRecord oldPrev = inUse( new PropertyRecord( 1 ) );
        addChange( oldPrev, inUse( new PropertyRecord( 1 ) ) );
        PropertyRecord newPrev = addChange( notInUse( new PropertyRecord( 2 ) ),
                                            inUse( new PropertyRecord( 2 ) ));
        oldProperty.setPrevProp( oldPrev.getId() );
        oldPrev.setNextProp( 42 );
        newProperty.setPrevProp( newPrev.getId() );
        newPrev.setNextProp( newProperty.getId() );

        newProperty.setNodeId( addChange( inUse( new NodeRecord( 100, false, NONE, oldPrev.getId() ) ),
                                          inUse( new NodeRecord( 100, false, NONE, newPrev.getId() ) ) ).getId() );

        // when
        ConsistencyReport.PropertyConsistencyReport report = checkChange( oldProperty, newProperty );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportPreviousReplacedButNotUpdated() throws Exception
    {
        // given
        PropertyRecord oldProperty = inUse( new PropertyRecord( 42 ) );
        oldProperty.setPrevProp( 1 );
        PropertyRecord newProperty = inUse( new PropertyRecord( 42 ) );
        newProperty.setPrevProp( 2 );

        add( inUse( new PropertyRecord( 1 ) ) ).setNextProp( 42 );
        addChange( notInUse( new PropertyRecord( 2 ) ),
                   inUse( new PropertyRecord( 2 ) ) ).setNextProp( 42 );

        newProperty.setNodeId( add( inUse( new NodeRecord( 100, false, NONE, 1 ) ) ).getId() );

        // when
        ConsistencyReport.PropertyConsistencyReport report = checkChange( oldProperty, newProperty );

        // then
        verify( report ).prevNotUpdated();
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportNextReplacedButNotUpdated() throws Exception
    {
        PropertyRecord oldProperty = inUse( new PropertyRecord( 42 ) );
        oldProperty.setNextProp( 1 );
        PropertyRecord newProperty = inUse( new PropertyRecord( 42 ) );
        newProperty.setNextProp( 2 );

        addChange( notInUse( new PropertyRecord( 2 ) ),
                   inUse( new PropertyRecord( 2 ) ) ).setPrevProp( 42 );

        newProperty.setNodeId( add( inUse( new NodeRecord( 100, false, NONE, newProperty.getId() ) ) ).getId() );

        // when
        ConsistencyReport.PropertyConsistencyReport report = checkChange( oldProperty, newProperty );

        // then
        verify( report ).nextNotUpdated();
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportStringValueUnreferencedButStillInUse() throws Exception
    {
        // given
        PropertyRecord oldProperty = inUse( new PropertyRecord( 42 ) );
        PropertyBlock block = propertyBlock( add( inUse( new PropertyKeyTokenRecord( 1 ) ) ),
                                             add( string( inUse( new DynamicRecord( 100 ) ) ) ) );
        oldProperty.addPropertyBlock( block );
        PropertyRecord newProperty = inUse( new PropertyRecord( 42 ) );

        newProperty.setNodeId( add( inUse( new NodeRecord( 100, false, NONE, newProperty.getId() ) ) ).getId() );

        // when
        ConsistencyReport.PropertyConsistencyReport report = checkChange( oldProperty, newProperty );

        // then
        verify( report ).stringUnreferencedButNotDeleted( block );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportArrayValueUnreferencedButStillInUse() throws Exception
    {
        // given
        PropertyRecord oldProperty = inUse( new PropertyRecord( 42 ) );
        PropertyBlock block = propertyBlock( add( inUse( new PropertyKeyTokenRecord( 1 ) ) ),
                                             add( array( inUse( new DynamicRecord( 100 ) ) ) ) );
        oldProperty.addPropertyBlock( block );
        PropertyRecord newProperty = inUse( new PropertyRecord( 42 ) );

        newProperty.setNodeId( add( inUse( new NodeRecord( 100, false, NONE, newProperty.getId() ) ) ).getId() );

        // when
        ConsistencyReport.PropertyConsistencyReport report = checkChange( oldProperty, newProperty );

        // then
        verify( report ).arrayUnreferencedButNotDeleted( block );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportPropertyChangedForWrongNode() throws Exception
    {
        // given
        PropertyRecord oldProperty = inUse( new PropertyRecord( 42 ) );
        PropertyRecord newProperty = notInUse( new PropertyRecord( 42 ) );
        newProperty.setNodeId( 10 );
        add( inUse( new NodeRecord( 10, false, NONE, NONE ) ) );

        // when
        ConsistencyReport.PropertyConsistencyReport report = checkChange( oldProperty, newProperty );

        // then
        verify( report ).changedForWrongOwner();
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportPropertyChangedForWrongNodeWithChain() throws Exception
    {
        // given
        PropertyRecord oldProperty = inUse( new PropertyRecord( 42 ) );
        PropertyRecord newProperty = notInUse( new PropertyRecord( 42 ) );
        PropertyRecord a = add( inUse( new PropertyRecord( 1 ) ) );
        PropertyRecord b = add( inUse( new PropertyRecord( 2 ) ) );
        a.setNextProp( b.getId() );
        b.setPrevProp( a.getId() );
        newProperty.setNodeId( add( inUse( new NodeRecord( 10, false, NONE, a.getId() ) ) ).getId() );

        // when
        ConsistencyReport.PropertyConsistencyReport report = checkChange( oldProperty, newProperty );

        // then
        verify( report ).changedForWrongOwner();
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportPropertyChangedForWrongRelationship() throws Exception
    {
        // given
        PropertyRecord oldProperty = inUse( new PropertyRecord( 42 ) );
        PropertyRecord newProperty = notInUse( new PropertyRecord( 42 ) );
        newProperty.setRelId( 10 );
        add( inUse( new RelationshipRecord( 10, 100, 200, 0 ) ) );

        // when
        ConsistencyReport.PropertyConsistencyReport report = checkChange( oldProperty, newProperty );

        // then
        verify( report ).changedForWrongOwner();
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportPropertyChangedForWrongRelationshipWithChain() throws Exception
    {
        // given
        PropertyRecord oldProperty = inUse( new PropertyRecord( 42 ) );
        PropertyRecord newProperty = notInUse( new PropertyRecord( 42 ) );
        RelationshipRecord rel = add( inUse( new RelationshipRecord( 1, 10, 20, 0 ) ) );
        PropertyRecord a = add( inUse( new PropertyRecord( 1 ) ) );
        PropertyRecord b = add( inUse( new PropertyRecord( 2 ) ) );
        a.setNextProp( b.getId() );
        b.setPrevProp( a.getId() );
        rel.setNextProp( a.getId() );
        newProperty.setRelId( rel.getId() );

        // when
        ConsistencyReport.PropertyConsistencyReport report = checkChange( oldProperty, newProperty );

        // then
        verify( report ).changedForWrongOwner();
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportPropertyChangedForWrongNeoStore() throws Exception
    {
        // given
        PropertyRecord oldProperty = inUse( new PropertyRecord( 42 ) );
        PropertyRecord newProperty = notInUse( new PropertyRecord( 42 ) );
        add( inUse( new NeoStoreRecord() ) );

        // when
        ConsistencyReport.PropertyConsistencyReport report = checkChange( oldProperty, newProperty );

        // then
        verify( report ).changedForWrongOwner();
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportPropertyChangedForWrongNeoStoreWithChain() throws Exception
    {
        // given
        PropertyRecord oldProperty = inUse( new PropertyRecord( 42 ) );
        PropertyRecord newProperty = notInUse( new PropertyRecord( 42 ) );
        PropertyRecord a = add( inUse( new PropertyRecord( 1 ) ) );
        PropertyRecord b = add( inUse( new PropertyRecord( 2 ) ) );
        a.setNextProp( b.getId() );
        b.setPrevProp( a.getId() );
        add( inUse( new NeoStoreRecord() ) ).setNextProp( a.getId() );

        // when
        ConsistencyReport.PropertyConsistencyReport report = checkChange( oldProperty, newProperty );

        // then
        verify( report ).changedForWrongOwner();
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportPropertyNotReferencedFromNode() throws Exception
    {
        // given
        PropertyRecord oldProperty = notInUse( new PropertyRecord( 42 ) );
        PropertyRecord newProperty = inUse( new PropertyRecord( 42 ) );
        newProperty.setNodeId( add( inUse( new NodeRecord( 1, false, NONE, NONE ) ) ).getId() );

        // when
        ConsistencyReport.PropertyConsistencyReport report = checkChange( oldProperty, newProperty );

        // then
        verify( report ).ownerDoesNotReferenceBack();
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportPropertyNotReferencedFromNodeWithChain() throws Exception
    {
        // given
        PropertyRecord oldProperty = notInUse( new PropertyRecord( 42 ) );
        PropertyRecord newProperty = inUse( new PropertyRecord( 42 ) );
        PropertyRecord a = add( inUse( new PropertyRecord( 1 ) ) );
        PropertyRecord b = add( inUse( new PropertyRecord( 2 ) ) );
        a.setNextProp( b.getId() );
        b.setPrevProp( a.getId() );
        newProperty.setNodeId( add( inUse( new NodeRecord( 1, false, NONE, a.getId() ) ) ).getId() );

        // when
        ConsistencyReport.PropertyConsistencyReport report = checkChange( oldProperty, newProperty );

        // then
        verify( report ).ownerDoesNotReferenceBack();
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportPropertyNotReferencedFromRelationship() throws Exception
    {
        // given
        PropertyRecord oldProperty = notInUse( new PropertyRecord( 42 ) );
        PropertyRecord newProperty = inUse( new PropertyRecord( 42 ) );
        newProperty.setRelId( add( inUse( new RelationshipRecord( 1, 10, 20, 0 ) ) ).getId() );

        // when
        ConsistencyReport.PropertyConsistencyReport report = checkChange( oldProperty, newProperty );

        // then
        verify( report ).ownerDoesNotReferenceBack();
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportPropertyNotReferencedFromRelationshipWithChain() throws Exception
    {
        // given
        PropertyRecord oldProperty = notInUse( new PropertyRecord( 42 ) );
        PropertyRecord newProperty = inUse( new PropertyRecord( 42 ) );
        RelationshipRecord rel = add( inUse( new RelationshipRecord( 1, 10, 20, 0 ) ) );
        PropertyRecord a = add( inUse( new PropertyRecord( 1 ) ) );
        PropertyRecord b = add( inUse( new PropertyRecord( 2 ) ) );
        a.setNextProp( b.getId() );
        b.setPrevProp( a.getId() );
        rel.setNextProp( a.getId() );
        newProperty.setRelId( rel.getId() );

        // when
        ConsistencyReport.PropertyConsistencyReport report = checkChange( oldProperty, newProperty );

        // then
        verify( report ).ownerDoesNotReferenceBack();
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportPropertyNotReferencedFromNeoStore() throws Exception
    {
        // given
        PropertyRecord oldProperty = notInUse( new PropertyRecord( 42 ) );
        PropertyRecord newProperty = inUse( new PropertyRecord( 42 ) );
        add( inUse( new NeoStoreRecord() ) );

        // when
        ConsistencyReport.PropertyConsistencyReport report = checkChange( oldProperty, newProperty );

        // then
        verify( report ).ownerDoesNotReferenceBack();
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportPropertyNotReferencedFromNeoStoreWithChain() throws Exception
    {
        // given
        PropertyRecord oldProperty = notInUse( new PropertyRecord( 42 ) );
        PropertyRecord newProperty = inUse( new PropertyRecord( 42 ) );
        PropertyRecord a = add( inUse( new PropertyRecord( 1 ) ) );
        PropertyRecord b = add( inUse( new PropertyRecord( 2 ) ) );
        a.setNextProp( b.getId() );
        b.setPrevProp( a.getId() );
        add( inUse( new NeoStoreRecord() ) ).setNextProp( a.getId() );

        // when
        ConsistencyReport.PropertyConsistencyReport report = checkChange( oldProperty, newProperty );

        // then
        verify( report ).ownerDoesNotReferenceBack();
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldNotReportMissingPropertyForDeletedNodeWithProperty()
    {
        // given
        PropertyRecord oldProperty = add( inUse( new PropertyRecord( 10 ) ) );
        NodeRecord oldNode = add( inUse( new NodeRecord( 20, false, 0, 0 ) ) );
        oldProperty.setNodeId( oldNode.getId() );
        oldNode.setNextProp( oldProperty.getId() );

        PropertyRecord newProperty = add( notInUse( new PropertyRecord( 10 ) ) );
        NodeRecord newNode = add( notInUse( new NodeRecord( 20, false, 0, 0 ) ) );
        newProperty.setNodeId( newNode.getId() );
        newNode.setNextProp( newProperty.getId() );

        // when
        ConsistencyReport.PropertyConsistencyReport report = checkChange( oldProperty, newProperty );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldNotReportMissingPropertyForDeletedRelationshipWithProperty()
    {
        // given
        NodeRecord oldNode1 = add( inUse( new NodeRecord( 1, false, NONE, NONE ) ) );
        NodeRecord oldNode2 = add( inUse( new NodeRecord( 2, false, NONE, NONE ) ) );

        RelationshipRecord oldRel = add( inUse( new RelationshipRecord( 42, 1, 2, 7 ) ) );
        oldNode1.setNextRel( oldRel.getId() );
        oldNode2.setNextRel( oldRel.getId() );

        PropertyRecord oldProperty = add( inUse( new PropertyRecord( 101 ) ) );
        oldProperty.setRelId( oldRel.getId() );
        oldRel.setNextProp( oldProperty.getId() );


        NodeRecord newNode1 = add( notInUse( new NodeRecord( 1, false, NONE, NONE ) ) );
        NodeRecord newNode2 = add( notInUse( new NodeRecord( 2, false, NONE, NONE ) ) );

        RelationshipRecord newRel = add( notInUse( new RelationshipRecord( 42, 1, 2, 7 ) ) );
        newNode1.setNextRel( newRel.getId() );
        newNode2.setNextRel( newRel.getId() );

        PropertyRecord newProperty = add( notInUse( new PropertyRecord( 101 ) ) );
        newProperty.setRelId( newRel.getId() );
        newRel.setNextProp( newProperty.getId() );

        // when
        ConsistencyReport.PropertyConsistencyReport report = checkChange( oldProperty, newProperty );

        // then
        verifyNoMoreInteractions( report );
    }
}
