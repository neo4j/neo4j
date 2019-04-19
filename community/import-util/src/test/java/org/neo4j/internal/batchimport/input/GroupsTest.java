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
package org.neo4j.internal.batchimport.input;

import org.junit.jupiter.api.Test;

import org.neo4j.test.Race;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GroupsTest
{
    @Test
    void shouldHandleConcurrentGetOrCreate() throws Throwable
    {
        // GIVEN
        Groups groups = new Groups();
        Race race = new Race();
        String name = "MyGroup";
        for ( int i = 0; i < Runtime.getRuntime().availableProcessors(); i++ )
        {
            race.addContestant( () ->
            {
                Group group = groups.getOrCreate( name );
                assertEquals( Groups.LOWEST_NONGLOBAL_ID, group.id() );
            } );
        }

        // WHEN
        race.go();

        // THEN
        Group otherGroup = groups.getOrCreate( "MyOtherGroup" );
        assertEquals( Groups.LOWEST_NONGLOBAL_ID + 1, otherGroup.id() );
    }

    @Test
    void shouldSupportMixedGroupModeInGetOrCreate()
    {
        // given
        Groups groups = new Groups();
        assertEquals( Group.GLOBAL, groups.getOrCreate( null ) );

        // when
        assertNotEquals( Group.GLOBAL, groups.getOrCreate( "Something" ) );
    }

    @Test
    void shouldSupportMixedGroupModeInGetOrCreate2()
    {
        // given
        Groups groups = new Groups();
        assertNotEquals( Group.GLOBAL, groups.getOrCreate( "Something" ) );

        // when
        assertEquals( Group.GLOBAL, groups.getOrCreate( null ) );
    }

    @Test
    void shouldGetCreatedGroup()
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
    void shouldGetGlobalGroup()
    {
        // given
        Groups groups = new Groups();
        groups.getOrCreate( null );

        // when
        Group group = groups.get( null );

        // then
        assertSame( Group.GLOBAL, group );
    }

    @Test
    void shouldSupportMixedGroupModeInGet()
    {
        // given
        Groups groups = new Groups();
        groups.getOrCreate( "Something" );

        // when
        assertEquals( Group.GLOBAL, groups.get( null ) );
    }

    @Test
    void shouldFailOnGettingNonExistentGroup()
    {
        // given
        Groups groups = new Groups();

        // when
        assertThrows( HeaderException.class, () -> groups.get( "Something" ) );
    }
}
