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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SessionTest
{
    private Session session;

    @BeforeEach
    void setUp() throws Exception
    {
        session = new Session( 1 );
    }

    @Test
    void cannotSetInvalidVariableName()
    {
        assertThrows( ShellException.class, () -> session.set( "foo bar", 42 ) );
    }

    @Test
    void canSetVariableName() throws ShellException
    {
        session.set( "_foobar", 42 );
    }

    @Test
    void cannotGetInvalidVariableName()
    {
        assertThrows( ShellException.class, () -> session.get( "foo bar" ) );
    }

    @Test
    void canGetVariableName() throws ShellException
    {
        session.set( "_foobar", 42 );
        assertEquals( 42, session.get( "_foobar" ) );
    }

    @Test
    void cannotRemoveInvalidVariableName()
    {
        assertThrows( ShellException.class, () -> session.remove( "foo bar" ) );
    }

    @Test
    void canRemoveVariableName() throws ShellException
    {
        session.set( "_foobar", 42 );
        assertEquals( 42, session.remove( "_foobar" ) );
    }

    @Test
    void canCheckInvalidVariableName()
    {
        assertEquals( false, session.has( "foo bar" ) );
    }

    @Test
    void canCheckVariableName() throws ShellException
    {
        assertEquals( false, session.has( "_foobar" ) );
        session.set( "_foobar", 42 );
        assertEquals( true, session.has( "_foobar" ) );
    }
}
