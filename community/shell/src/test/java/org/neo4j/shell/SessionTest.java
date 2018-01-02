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
package org.neo4j.shell;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

public class SessionTest
{
    private Session session;

    @Before
    public void setUp() throws Exception
    {
        session = new Session( 1 );
    }

    @Test(expected = ShellException.class)
    public void cannotSetInvalidVariableName() throws ShellException
    {
        session.set( "foo bar", 42 );
    }

    @Test
    public void canSetVariableName() throws ShellException
    {
        session.set( "_foobar", 42 );
    }

    @Test(expected = ShellException.class)
    public void cannotGetInvalidVariableName() throws ShellException
    {
        session.get( "foo bar" );
    }

    @Test
    public void canGetVariableName() throws ShellException
    {
        session.set( "_foobar", 42);
        assertEquals( 42, session.get( "_foobar" ));
    }

    @Test(expected = ShellException.class)
    public void cannotRemoveInvalidVariableName() throws ShellException
    {
        session.remove( "foo bar" );
    }

    @Test
    public void canRemoveVariableName() throws ShellException
    {
        session.set( "_foobar", 42);
        assertEquals( 42, session.remove( "_foobar" ));
    }

    @Test
    public void canCheckInvalidVariableName() throws ShellException
    {
        assertEquals( false, session.has( "foo bar" ));
    }

    @Test
    public void canCheckVariableName() throws ShellException
    {
        assertEquals( false, session.has( "_foobar" ));
        session.set( "_foobar", 42 );
        assertEquals( true, session.has( "_foobar" ));
    }
}
