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
package org.neo4j.commandline.arguments.common;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.neo4j.helpers.Args;

import static org.junit.Assert.assertEquals;

public class DatabaseTest
{
    @Rule
    public ExpectedException expected = ExpectedException.none();

    private Database arg = new Database();

    @Test
    public void parseDatabaseShouldThrowOnPath()
    {
        Path path = Paths.get( "data", "databases", "graph.db" );
        expected.expect( IllegalArgumentException.class );
        expected.expectMessage( "'database' should be a name but you seem to have specified a path: " + path );
        arg.parse( Args.parse( "--database=" + path ) );
    }

    @Test
    public void parseDatabaseName()
    {
        assertEquals( "bob.db", arg.parse( Args.parse( "--database=bob.db" ) ) );
    }
}
