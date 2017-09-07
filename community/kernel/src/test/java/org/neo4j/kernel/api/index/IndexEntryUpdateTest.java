/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.index;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class IndexEntryUpdateTest
{
    private final Value[] multiValue = new Value[]{Values.of( "value" ), Values.of( "value2" )};
    private final Value singleValue = Values.of( "value" );

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void indexEntryUpdatesShouldBeEqual()
    {
        IndexEntryUpdate<?> a = IndexEntryUpdate.add( 0, SchemaDescriptorFactory.forLabel( 3, 4 ), singleValue );
        IndexEntryUpdate<?> b = IndexEntryUpdate.add( 0, SchemaDescriptorFactory.forLabel( 3, 4 ), singleValue );
        assertThat( a, equalTo( b ) );
        assertThat( a.hashCode(), equalTo( b.hashCode() ) );
    }

    @Test
    public void addShouldRetainValues()
    {
        IndexEntryUpdate<?> single = IndexEntryUpdate.add( 0, SchemaDescriptorFactory.forLabel( 3, 4 ), singleValue );
        IndexEntryUpdate<?> multi = IndexEntryUpdate.add( 0, SchemaDescriptorFactory.forLabel( 3, 4, 5 ), multiValue );
        assertThat( single, not( equalTo( multi ) ) );
        assertThat( single.values(), equalTo( new Object[]{singleValue} ) );
        assertThat( multi.values(), equalTo( multiValue ) );
    }

    @Test
    public void removeShouldRetainValues()
    {
        IndexEntryUpdate<?> single = IndexEntryUpdate.remove( 0, SchemaDescriptorFactory.forLabel( 3, 4 ), singleValue );
        IndexEntryUpdate<?> multi = IndexEntryUpdate
                .remove( 0, SchemaDescriptorFactory.forLabel( 3, 4, 5 ), multiValue );
        assertThat( single, not( equalTo( multi ) ) );
        assertThat( single.values(), equalTo( new Object[]{singleValue} ) );
        assertThat( multi.values(), equalTo( multiValue ) );
    }

    @Test
    public void addShouldThrowIfAskedForChanged() throws Exception
    {
        IndexEntryUpdate<?> single = IndexEntryUpdate.add( 0, SchemaDescriptorFactory.forLabel( 3, 4 ), singleValue );
        thrown.expect( UnsupportedOperationException.class );
        single.beforeValues();
    }

    @Test
    public void removeShouldThrowIfAskedForChanged() throws Exception
    {
        IndexEntryUpdate<?> single = IndexEntryUpdate.remove( 0, SchemaDescriptorFactory.forLabel( 3, 4 ), singleValue );
        thrown.expect( UnsupportedOperationException.class );
        single.beforeValues();
    }

    @Test
    public void updatesShouldEqualRegardlessOfCreationMethod()
    {
        IndexEntryUpdate<?> singleAdd = IndexEntryUpdate.add( 0, SchemaDescriptorFactory.forLabel( 3, 4 ), singleValue );
        Value[] singleAsArray = {singleValue};
        IndexEntryUpdate<?> multiAdd = IndexEntryUpdate
                .add( 0, SchemaDescriptorFactory.forLabel( 3, 4 ), singleAsArray );
        IndexEntryUpdate<?> singleRemove = IndexEntryUpdate
                .remove( 0, SchemaDescriptorFactory.forLabel( 3, 4 ), singleValue );
        IndexEntryUpdate<?> multiRemove = IndexEntryUpdate
                .remove( 0, SchemaDescriptorFactory.forLabel( 3, 4 ), singleAsArray );
        IndexEntryUpdate<?> singleChange = IndexEntryUpdate
                .change( 0, SchemaDescriptorFactory.forLabel( 3, 4 ), singleValue, singleValue );
        IndexEntryUpdate<?> multiChange = IndexEntryUpdate
                .change( 0, SchemaDescriptorFactory.forLabel( 3, 4 ), singleAsArray, singleAsArray );
        assertThat( singleAdd, equalTo( multiAdd ) );
        assertThat( singleRemove, equalTo( multiRemove ) );
        assertThat( singleChange, equalTo( multiChange ) );
    }

    @Test
    public void changedShouldRetainValues() throws Exception
    {
        Value singleAfter = Values.of( "Hello" );
        IndexEntryUpdate<?> singleChange = IndexEntryUpdate
                .change( 0, SchemaDescriptorFactory.forLabel( 3, 4 ), singleValue, singleAfter );
        Value[] multiAfter = {Values.of( "Hello" ), Values.of( "Hi" )};
        IndexEntryUpdate<?> multiChange = IndexEntryUpdate
                .change( 0, SchemaDescriptorFactory.forLabel( 3, 4, 5 ), multiValue, multiAfter );
        assertThat( new Object[]{singleValue}, equalTo( singleChange.beforeValues() ) );
        assertThat( new Object[]{singleAfter}, equalTo( singleChange.values() ) );
        assertThat( multiValue, equalTo( multiChange.beforeValues() ) );
        assertThat( multiAfter, equalTo( multiChange.values() ) );
    }
}
