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
package org.neo4j.kernel.impl.api.state;

import org.junit.Test;

import java.util.Iterator;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.storageengine.api.StorageProperty;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.neo4j.kernel.api.properties.Property.stringProperty;

public class PropertyContainerStateImplTest
{
    @Test
    public void shouldListAddedProperties() throws Exception
    {
        // Given
        PropertyContainerStateImpl state = new PropertyContainerStateImpl( 1 );
        state.addProperty( stringProperty( 1, "Hello" ) );
        state.addProperty( stringProperty( 2, "Hello" ) );
        state.removeProperty( stringProperty( 1, "Hello" ) );

        // When
        Iterator<StorageProperty> added = state.addedProperties();

        // Then
        assertThat( Iterators.asList( added ),
                equalTo( asList( stringProperty( 2, "Hello" ) ) ) );
    }

    @Test
    public void shouldListAddedPropertiesEvenIfPropertiesHaveBeenReplaced() throws Exception
    {
        // Given
        PropertyContainerStateImpl state = new PropertyContainerStateImpl( 1 );
        state.addProperty( stringProperty( 1, "Hello" ) );
        state.addProperty( stringProperty( 1, "WAT" ) );
        state.addProperty( stringProperty( 2, "Hello" ) );

        // When
        Iterator<StorageProperty> added = state.addedProperties();

        // Then
        assertThat( Iterators.asList( added ),
                equalTo( asList( stringProperty( 1, "WAT" ), stringProperty( 2, "Hello" ) ) ) );
    }

    @Test
    public void shouldConvertAddRemoveToChange() throws Exception
    {
        // Given
        PropertyContainerStateImpl state = new PropertyContainerStateImpl( 1 );

        // When
        state.removeProperty( stringProperty( 4, "a value" ) );
        state.addProperty( stringProperty( 4, "another value" ) );

        // Then
        assertThat( Iterators.asList( state.changedProperties() ),
                equalTo( asList( stringProperty( 4, "another value" ) ) ) );
        assertFalse( state.addedProperties().hasNext() );
        assertFalse( state.removedProperties().hasNext() );
    }
}
