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
package org.neo4j.legacy.consistency.store;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.legacy.consistency.store.CacheSmallStoresRecordAccess;
import org.neo4j.legacy.consistency.store.DiffRecordAccess;
import org.neo4j.legacy.consistency.store.DirectRecordReference;
import org.neo4j.legacy.consistency.store.RecordReference;

import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class CacheSmallStoresRecordAccessTest
{
    @Test
    public void shouldDelegateLookupForMostStores() throws Exception
    {
        // given
        DiffRecordAccess delegate = mock( DiffRecordAccess.class );
        CacheSmallStoresRecordAccess recordAccess = new CacheSmallStoresRecordAccess( delegate, null, null, null );

        // when
        recordAccess.node( 42 );
        recordAccess.relationship( 2001 );
        recordAccess.property( 2468 );
        recordAccess.string( 666 );
        recordAccess.array( 11 );

        // then
        verify( delegate ).node( 42 );
        verify( delegate ).relationship( 2001 );
        verify( delegate ).property( 2468 );
        verify( delegate ).string( 666 );
        verify( delegate ).array( 11 );
    }

    @Test
    public void shouldServePropertyKeysAndRelationshipLabelsFromSuppliedArrayCaches() throws Exception
    {
        // given
        DiffRecordAccess delegate = mock( DiffRecordAccess.class );
        PropertyKeyTokenRecord propertyKey0 = new PropertyKeyTokenRecord( 0 );
        PropertyKeyTokenRecord propertyKey2 = new PropertyKeyTokenRecord( 2 );
        PropertyKeyTokenRecord propertyKey1 = new PropertyKeyTokenRecord( 1 );
        RelationshipTypeTokenRecord relationshipType0 = new RelationshipTypeTokenRecord( 0 );
        RelationshipTypeTokenRecord relationshipType1 = new RelationshipTypeTokenRecord( 1 );
        RelationshipTypeTokenRecord relationshipType2 = new RelationshipTypeTokenRecord( 2 );
        LabelTokenRecord label0 = new LabelTokenRecord( 0 );
        LabelTokenRecord label1 = new LabelTokenRecord( 1 );
        LabelTokenRecord label2 = new LabelTokenRecord( 2 );

        CacheSmallStoresRecordAccess recordAccess = new CacheSmallStoresRecordAccess(
                delegate, new PropertyKeyTokenRecord[]{
                propertyKey0,
                propertyKey1,
                propertyKey2,
        }, new RelationshipTypeTokenRecord[]{
                relationshipType0,
                relationshipType1,
                relationshipType2,
        }, new LabelTokenRecord[]{
                label0,
                label1,
                label2,
        } );

        // when
        assertThat( recordAccess.propertyKey( 0 ), isDirectReferenceTo( propertyKey0 ) );
        assertThat( recordAccess.propertyKey( 1 ), isDirectReferenceTo( propertyKey1 ) );
        assertThat( recordAccess.propertyKey( 2 ), isDirectReferenceTo( propertyKey2 ) );
        assertThat( recordAccess.relationshipType( 0 ), isDirectReferenceTo( relationshipType0 ) );
        assertThat( recordAccess.relationshipType( 1 ), isDirectReferenceTo( relationshipType1 ) );
        assertThat( recordAccess.relationshipType( 2 ), isDirectReferenceTo( relationshipType2 ) );
        assertThat( recordAccess.label( 0 ), isDirectReferenceTo( label0 ) );
        assertThat( recordAccess.label( 1 ), isDirectReferenceTo( label1 ) );
        assertThat( recordAccess.label( 2 ), isDirectReferenceTo( label2 ) );

        // then
        verifyZeroInteractions( delegate );
    }

    @SuppressWarnings("unchecked")
    private static <T extends AbstractBaseRecord> Matcher<RecordReference<T>> isDirectReferenceTo( T record )
    {
        return (Matcher) new DirectReferenceMatcher<T>( record );
    }

    private static class DirectReferenceMatcher<T extends AbstractBaseRecord>
            extends TypeSafeMatcher<DirectRecordReference<T>>
    {
        private final T record;

        @SuppressWarnings("unchecked")
        DirectReferenceMatcher( T record )
        {
            super( (Class) DirectRecordReference.class );
            this.record = record;
        }

        @Override
        public boolean matchesSafely( DirectRecordReference<T> reference )
        {
            return record == reference.record();
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText( DirectRecordReference.class.getName() )
                       .appendText( "( " ).appendValue( record ).appendText( " )" );
        }
    }
}
