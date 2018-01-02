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
package org.neo4j.server.scripting.javascript;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

public class TestWhiteListClassShutter
{

    @Test
    public void shouldAllowWhiteListedClasses() throws Exception
    {
        // Given
        Set<String> whiteList = new HashSet<String>();
        whiteList.add( getClass().getName() );
        WhiteListClassShutter shutter = new WhiteListClassShutter(whiteList);

        // When
        boolean visible = shutter.visibleToScripts( getClass().getName() );

        // Then
        assertThat(visible, is(true));
    }

    @Test
    public void shouldDisallowUnlistedClasses() throws Exception
    {
        WhiteListClassShutter shutter = new WhiteListClassShutter(new HashSet<String>());

        // When
        boolean visible = shutter.visibleToScripts( getClass().getName() );

        // Then
        assertThat(visible, is(false));
    }

}
