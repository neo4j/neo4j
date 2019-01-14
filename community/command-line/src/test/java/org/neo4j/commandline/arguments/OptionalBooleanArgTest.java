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
package org.neo4j.commandline.arguments;

import org.junit.Test;

import org.neo4j.helpers.Args;

import static org.junit.Assert.assertEquals;

public class OptionalBooleanArgTest
{

    @Test
    public void parsesValues1()
    {
        OptionalBooleanArg arg = new OptionalBooleanArg( "foo", false, "" );

        assertEquals( "false", arg.parse( Args.parse() ) );
        assertEquals( "false", arg.parse( Args.parse( "--foo=false" ) ) );
        assertEquals( "true", arg.parse( Args.parse( "--foo=true" ) ) );
        assertEquals( "true", arg.parse( Args.parse( "--foo" ) ) );
    }

    @Test
    public void parsesValues2()
    {
        OptionalBooleanArg arg = new OptionalBooleanArg( "foo", true, "" );

        assertEquals( "true", arg.parse( Args.parse() ) );
        assertEquals( "false", arg.parse( Args.parse( "--foo=false" ) ) );
        assertEquals( "true", arg.parse( Args.parse( "--foo=true" ) ) );
        assertEquals( "true", arg.parse( Args.parse( "--foo" ) ) );
    }

    @Test
    public void usageTest()
    {
        OptionalBooleanArg arg = new OptionalBooleanArg( "foo", true, "" );

        assertEquals( "[--foo[=<true|false>]]", arg.usage() );
    }
}
