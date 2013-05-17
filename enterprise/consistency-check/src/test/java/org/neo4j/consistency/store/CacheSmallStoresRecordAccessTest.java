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
package org.neo4j.consistency.store;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenRecord;

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
        CacheSmallStoresRecordAccess recordAccess = new CacheSmallStoresRecordAccess( delegate, null, null );

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
        RelationshipTypeTokenRecord relationshipLabel0 = new RelationshipTypeTokenRecord( 0 );
        PropertyKeyTokenRecord propertyKey1 = new PropertyKeyTokenRecord( 1 );
        RelationshipTypeTokenRecord relationshipLabel1 = new RelationshipTypeTokenRecord( 1 );
        PropertyKeyTokenRecord propertyKey2 = new PropertyKeyTokenRecord( 2 );
        RelationshipTypeTokenRecord relationshipLabel2 = new RelationshipTypeTokenRecord( 2 );

        CacheSmallStoresRecordAccess recordAccess = new CacheSmallStoresRecordAccess(
                delegate, new PropertyKeyTokenRecord[]{
                propertyKey0,
                propertyKey1,
                propertyKey2,
        }, new RelationshipTypeTokenRecord[]{
                relationshipLabel0,
                relationshipLabel1,
                relationshipLabel2,
        } );

        // when
        assertThat( recordAccess.propertyKey( 0 ), isDirectReferenceTo( propertyKey0 ) );
        assertThat( recordAccess.relationshipType( 0 ), isDirectReferenceTo( relationshipLabel0 ) );
        assertThat( recordAccess.propertyKey( 1 ), isDirectReferenceTo( propertyKey1 ) );
        assertThat( recordAccess.relationshipType( 1 ), isDirectReferenceTo( relationshipLabel1 ) );
        assertThat( recordAccess.propertyKey( 2 ), isDirectReferenceTo( propertyKey2 ) );
        assertThat( recordAccess.relationshipType( 2 ), isDirectReferenceTo( relationshipLabel2 ) );

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
