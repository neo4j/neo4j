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
package org.neo4j.kernel.api.index;

import org.junit.jupiter.api.Test;

import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class IndexEntryUpdateTest
{
    private final Value[] multiValue = new Value[]{Values.of( "value" ), Values.of( "value2" )};
    private final Value singleValue = Values.of( "value" );

    @Test
    void indexEntryUpdatesShouldBeEqual()
    {
        IndexEntryUpdate<?> a = IndexEntryUpdate.add( 0, SchemaDescriptor.forLabel( 3, 4 ), singleValue );
        IndexEntryUpdate<?> b = IndexEntryUpdate.add( 0, SchemaDescriptor.forLabel( 3, 4 ), singleValue );
        assertThat( a ).isEqualTo( b );
        assertThat( a.hashCode() ).isEqualTo( b.hashCode() );
    }

    @Test
    void addShouldRetainValues()
    {
        IndexEntryUpdate<?> single = IndexEntryUpdate.add( 0, SchemaDescriptor.forLabel( 3, 4 ), singleValue );
        IndexEntryUpdate<?> multi = IndexEntryUpdate.add( 0, SchemaDescriptor.forLabel( 3, 4, 5 ), multiValue );
        assertThat( single ).isNotEqualTo( multi );
        assertThat( single.values() ).isEqualTo( new Object[]{singleValue} );
        assertThat( multi.values() ).isEqualTo( multiValue );
    }

    @Test
    void removeShouldRetainValues()
    {
        IndexEntryUpdate<?> single = IndexEntryUpdate.remove( 0, SchemaDescriptor.forLabel( 3, 4 ), singleValue );
        IndexEntryUpdate<?> multi = IndexEntryUpdate
                .remove( 0, SchemaDescriptor.forLabel( 3, 4, 5 ), multiValue );
        assertThat( single ).isNotEqualTo( multi );
        assertThat( single.values() ).isEqualTo( new Object[]{singleValue} );
        assertThat( multi.values() ).isEqualTo( multiValue );
    }

    @Test
    void addShouldThrowIfAskedForChanged()
    {
        IndexEntryUpdate<?> single = IndexEntryUpdate.add( 0, SchemaDescriptor.forLabel( 3, 4 ), singleValue );
        assertThrows( UnsupportedOperationException.class, single::beforeValues );
    }

    @Test
    void removeShouldThrowIfAskedForChanged()
    {
        IndexEntryUpdate<?> single = IndexEntryUpdate.remove( 0, SchemaDescriptor.forLabel( 3, 4 ), singleValue );
        assertThrows( UnsupportedOperationException.class, single::beforeValues );
    }

    @Test
    void updatesShouldEqualRegardlessOfCreationMethod()
    {
        IndexEntryUpdate<?> singleAdd = IndexEntryUpdate.add( 0, SchemaDescriptor.forLabel( 3, 4 ), singleValue );
        Value[] singleAsArray = {singleValue};
        IndexEntryUpdate<?> multiAdd = IndexEntryUpdate
                .add( 0, SchemaDescriptor.forLabel( 3, 4 ), singleAsArray );
        IndexEntryUpdate<?> singleRemove = IndexEntryUpdate
                .remove( 0, SchemaDescriptor.forLabel( 3, 4 ), singleValue );
        IndexEntryUpdate<?> multiRemove = IndexEntryUpdate
                .remove( 0, SchemaDescriptor.forLabel( 3, 4 ), singleAsArray );
        IndexEntryUpdate<?> singleChange = IndexEntryUpdate
                .change( 0, SchemaDescriptor.forLabel( 3, 4 ), singleValue, singleValue );
        IndexEntryUpdate<?> multiChange = IndexEntryUpdate
                .change( 0, SchemaDescriptor.forLabel( 3, 4 ), singleAsArray, singleAsArray );
        assertThat( singleAdd ).isEqualTo( multiAdd );
        assertThat( singleRemove ).isEqualTo( multiRemove );
        assertThat( singleChange ).isEqualTo( multiChange );
    }

    @Test
    void changedShouldRetainValues()
    {
        Value singleAfter = Values.of( "Hello" );
        IndexEntryUpdate<?> singleChange = IndexEntryUpdate
                .change( 0, SchemaDescriptor.forLabel( 3, 4 ), singleValue, singleAfter );
        Value[] multiAfter = {Values.of( "Hello" ), Values.of( "Hi" )};
        IndexEntryUpdate<?> multiChange = IndexEntryUpdate
                .change( 0, SchemaDescriptor.forLabel( 3, 4, 5 ), multiValue, multiAfter );
        assertThat( new Object[]{singleValue} ).isEqualTo( singleChange.beforeValues() );
        assertThat( new Object[]{singleAfter} ).isEqualTo( singleChange.values() );
        assertThat( multiValue ).isEqualTo( multiChange.beforeValues() );
        assertThat( multiAfter ).isEqualTo( multiChange.values() );
    }
}
