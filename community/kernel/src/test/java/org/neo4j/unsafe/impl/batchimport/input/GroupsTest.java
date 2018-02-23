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
package org.neo4j.unsafe.impl.batchimport.input;

import org.junit.jupiter.api.Test;

import org.neo4j.test.Race;

import static java.lang.Runtime.getRuntime;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.unsafe.impl.batchimport.input.Group.GLOBAL;
import static org.neo4j.unsafe.impl.batchimport.input.Groups.LOWEST_NONGLOBAL_ID;

public class GroupsTest
{
    @Test
    public void shouldHandleConcurrentGetOrCreate() throws Throwable
    {
        // GIVEN
        Groups groups = new Groups();
        Race race = new Race();
        String name = "MyGroup";
        for ( int i = 0; i < getRuntime().availableProcessors(); i++ )
        {
            race.addContestant( () ->
            {
                Group group = groups.getOrCreate( name );
                assertEquals( LOWEST_NONGLOBAL_ID, group.id() );
            } );
        }

        // WHEN
        race.go();

        // THEN
        Group otherGroup = groups.getOrCreate( "MyOtherGroup" );
        assertEquals( LOWEST_NONGLOBAL_ID + 1, otherGroup.id() );
    }

    @Test
    public void shouldSupportMixedGroupModeInGetOrCreate()
    {
        // given
        Groups groups = new Groups();
        assertEquals( GLOBAL, groups.getOrCreate( null ) );

        // when
        assertNotEquals( GLOBAL, groups.getOrCreate( "Something" ) );
    }

    @Test
    public void shouldSupportMixedGroupModeInGetOrCreate2()
    {
        // given
        Groups groups = new Groups();
        assertNotEquals( GLOBAL, groups.getOrCreate( "Something" ) );

        // when
        assertEquals( GLOBAL, groups.getOrCreate( null ) );
    }

    @Test
    public void shouldGetCreatedGroup()
    {
        // given
        Groups groups = new Groups();
        String name = "Something";
        Group createdGroup = groups.getOrCreate( name );

        // when
        Group gottenGroup = groups.get( name );

        // then
        assertSame( createdGroup, gottenGroup );
    }

    @Test
    public void shouldGetGlobalGroup()
    {
        // given
        Groups groups = new Groups();
        groups.getOrCreate( null );

        // when
        Group group = groups.get( null );

        // then
        assertSame( GLOBAL, group );
    }

    @Test
    public void shouldSupportMixedGroupModeInGet()
    {
        // given
        Groups groups = new Groups();
        groups.getOrCreate( "Something" );

        // when
        assertEquals( GLOBAL, groups.get( null ) );
    }

    @Test
    public void shouldFailOnGettingNonExistentGroup()
    {
        assertThrows( HeaderException.class, () -> {
            // given
            Groups groups = new Groups();

            // when
            groups.get( "Something" );
        } );
    }
}
