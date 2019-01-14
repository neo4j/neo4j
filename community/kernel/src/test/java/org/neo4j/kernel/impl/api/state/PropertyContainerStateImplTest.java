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
package org.neo4j.kernel.impl.api.state;

import org.junit.Test;

import java.util.Iterator;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.properties.PropertyKeyValue;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.values.storable.Values;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class PropertyContainerStateImplTest
{
    @Test
    public void shouldListAddedProperties()
    {
        // Given
        PropertyContainerStateImpl state = new PropertyContainerStateImpl( 1 );
        state.addProperty( 1, Values.of( "Hello" ) );
        state.addProperty( 2, Values.of( "Hello" ) );
        state.removeProperty( 1 );

        // When
        Iterator<StorageProperty> added = state.addedProperties();

        // Then
        assertThat( Iterators.asList( added ),
                equalTo( asList( new PropertyKeyValue( 2, Values.of( "Hello" ) ) ) ) );
    }

    @Test
    public void shouldListAddedPropertiesEvenIfPropertiesHaveBeenReplaced()
    {
        // Given
        PropertyContainerStateImpl state = new PropertyContainerStateImpl( 1 );
        state.addProperty( 1, Values.of( "Hello" ) );
        state.addProperty( 1, Values.of( "WAT" ) );
        state.addProperty( 2, Values.of( "Hello" ) );

        // When
        Iterator<StorageProperty> added = state.addedProperties();

        // Then
        assertThat( Iterators.asList( added ),
                equalTo( asList(
                        new PropertyKeyValue( 1, Values.of( "WAT" ) ),
                        new PropertyKeyValue( 2, Values.of( "Hello" ) ) )
                ) );
    }

    @Test
    public void shouldConvertAddRemoveToChange()
    {
        // Given
        PropertyContainerStateImpl state = new PropertyContainerStateImpl( 1 );

        // When
        state.removeProperty( 4 );
        state.addProperty( 4, Values.of( "another value" ) );

        // Then
        assertThat( Iterators.asList( state.changedProperties() ),
                equalTo( asList( new PropertyKeyValue( 4, Values.of( "another value" ) ) ) ) );
        assertFalse( state.addedProperties().hasNext() );
        assertFalse( state.removedProperties().hasNext() );
    }
}
