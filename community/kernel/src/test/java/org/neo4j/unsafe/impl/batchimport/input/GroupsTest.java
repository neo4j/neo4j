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
package org.neo4j.unsafe.impl.batchimport.input;

import org.junit.Test;

import org.neo4j.test.Race;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class GroupsTest
{
    @Test
    public void shouldHandleConcurrentGetOrCreate() throws Throwable
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
                assertEquals( 0, group.id() );
            } );
        }

        // WHEN
        race.go();

        // THEN
        Group otherGroup = groups.getOrCreate( "MyOtherGroup" );
        assertEquals( 1, otherGroup.id() );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldFailOnMixedGroupModeInGetOrCreate() throws Exception
    {
        // given
        Groups groups = new Groups();
        groups.getOrCreate( null );

        // when
        groups.getOrCreate( "Something" );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldFailOnMixedGroupModeInGetOrCreate2() throws Exception
    {
        // given
        Groups groups = new Groups();
        groups.getOrCreate( "Something" );

        // when
        groups.getOrCreate( null );
    }

    @Test
    public void shouldGetCreatedGroup() throws Exception
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
    public void shouldGetGlobalGroup() throws Exception
    {
        // given
        Groups groups = new Groups();
        groups.getOrCreate( null );

        // when
        Group group = groups.get( null );

        // then
        assertSame( Group.GLOBAL, group );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldFailOnMixedGroupModeInGet() throws Exception
    {
        // given
        Groups groups = new Groups();
        groups.getOrCreate( "Something" );

        // when
        groups.get( null );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldFailOnMixedGroupModeInGet2() throws Exception
    {
        // given
        Groups groups = new Groups();
        groups.getOrCreate( null );

        // when
        groups.get( "Something" );
    }
}
