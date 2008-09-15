/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.shell;

import junit.framework.TestCase;
import org.neo4j.impl.shell.NeoShellServer;
import org.neo4j.util.shell.AppCommandParser;

public class NeoShellTest extends TestCase
{
    private AppCommandParser parse( String line ) throws Exception
    {
        return new AppCommandParser( new NeoShellServer( null ), line );
    }

    public void testParserEasy() throws Exception
    {
        AppCommandParser parser = this.parse( "ls -la" );
        assertEquals( "ls", parser.getAppName() );
        assertEquals( 2, parser.options().size() );
        assertTrue( parser.options().containsKey( "l" ) );
        assertTrue( parser.options().containsKey( "a" ) );
        assertTrue( parser.arguments().isEmpty() );
    }

    public void testParserArguments() throws Exception
    {
        AppCommandParser parser = this
            .parse( "set -t java.lang.Integer key value" );
        assertEquals( "set", parser.getAppName() );
        assertTrue( parser.options().containsKey( "t" ) );
        assertEquals( "java.lang.Integer", parser.options().get( "t" ) );
        assertEquals( 2, parser.arguments().size() );
        assertEquals( "key", parser.arguments().get( 0 ) );
        assertEquals( "value", parser.arguments().get( 1 ) );

        assertException( "set -tsd" );
    }

    private void assertException( String command )
    {
        try
        {
            this.parse( command );
            fail( "Should fail" );
        }
        catch ( Exception e )
        {
            // Good
        }
    }
}