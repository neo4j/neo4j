/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.input;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

import static org.neo4j.unsafe.impl.batchimport.input.UpdateBehaviour.ADD;

public class InputEntityTest
{
    @Test
    public void shouldAddProperties() throws Exception
    {
        // GIVEN
        InputEntity entity = entity(
                "first", "Yeah",
                "second", "Yo" );

        // WHEN
        entity.updateProperties( ADD, "third", "Yee" );

        // THEN
        assertArrayEquals( new Object[] {
                "first", "Yeah",
                "second", "Yo",
                "third", "Yee"
        }, entity.properties() );
    }

    @Test
    public void shouldAddToExistingProperty() throws Exception
    {
        // GIVEN
        InputEntity entity = entity(
                "first", "Yeah",
                "second", "Yo" );

        // WHEN
        entity.updateProperties( ADD, "second", "Ya" );

        // THEN
        assertArrayEquals( new Object[] {
                "first", "Yeah",
                "second", new String[] {"Yo", "Ya"},
        }, entity.properties() );
    }

    @Test
    public void shouldAddToExistingArrayProperty() throws Exception
    {
        // GIVEN
        InputEntity entity = entity(
                "first", "Yeah",
                "second", "Yo" );

        // WHEN
        entity.updateProperties( ADD, "second", "Ya" );
        entity.updateProperties( ADD, "second", "Yi" );

        // THEN
        assertArrayEquals( new Object[] {
                "first", "Yeah",
                "second", new String[] {"Yo", "Ya", "Yi"},
        }, entity.properties() );
    }

    @Test
    public void shouldSetProperties() throws Exception
    {
        // GIVEN
        InputEntity entity = entity(
                "first", "Yeah",
                "second", "Yo" );

        // WHEN
        entity.setProperties( "third", "Yee" );

        // THEN
        assertArrayEquals( new Object[] { "third", "Yee" }, entity.properties() );
    }

    @Test
    public void shouldRemovePropertyInTheMiddle() throws Exception
    {
        // GIVEN
        InputEntity entity = entity(
                "first", "Yeah",
                "second", "Yo",
                "third", "Hey" );

        // WHEN
        entity.removeProperty( "second" );

        // THEN
        assertArrayEquals( new Object[] {
                "first", "Yeah",
                "third", "Hey",
        }, entity.properties() );
    }

    @Test
    public void shouldRemovePropertyInTheEnd() throws Exception
    {
        // GIVEN
        InputEntity entity = entity(
                "first", "Yeah",
                "second", "Yo",
                "third", "Hey" );

        // WHEN
        entity.removeProperty( "third" );

        // THEN
        assertArrayEquals( new Object[] {
                "first", "Yeah",
                "second", "Yo",
        }, entity.properties() );
    }

    @Test
    public void shouldRemoveLastProperty() throws Exception
    {
        // GIVEN
        InputEntity entity = entity(
                "first", "Yeah" );

        // WHEN
        entity.removeProperty( "first" );

        // THEN
        assertArrayEquals( new Object[] {}, entity.properties() );
    }

    private InputEntity entity( Object... properties )
    {
        return new InputNode( "source", 1, 0, "id", properties, null, InputEntity.NO_LABELS, null );
    }
}
