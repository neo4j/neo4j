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
package org.neo4j.shell.impl;

import java.text.SimpleDateFormat;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.ShellServer;
import org.neo4j.shell.impl.BashVariableInterpreter.Replacer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BashVariableInterpreterTest
{
    @Test
    public void shouldInterpretDate() throws Exception
    {
        // WHEN
        String interpreted = interpreter.interpret( "Date:\\d", server, session );
        
        // THEN
        String datePart = interpreted.substring( "Date:".length() );
        assertNotNull( new SimpleDateFormat( "EEE MMM dd" ).parse( datePart ) );
    }
    
    @Test
    public void shouldInterpretTime() throws Exception
    {
        // WHEN
        String interpreted = interpreter.interpret( "Time:\\t", server, session );
        
        // THEN
        String datePart = interpreted.substring( "Time:".length() );
        assertNotNull( new SimpleDateFormat( "HH:mm:ss" ).parse( datePart ) );
    }
    
    @Test
    public void customInterpreter() throws Exception
    {
        // GIVEN
        interpreter.addReplacer( "test", new Replacer()
        {
            @Override
            public String getReplacement( ShellServer server, Session session ) throws ShellException
            {
                return "Hello";
            }
        } );
        
        // WHEN
        String interpreted = interpreter.interpret( "\\test world", server, session );
        
        // THEN
        assertEquals( "Hello world", interpreted );
    }
    
    private final BashVariableInterpreter interpreter = new BashVariableInterpreter();
    private final Session session = new Session( 0 );
    private ShellServer server;
    
    @Before
    public void before() throws Exception
    {
        server = mock( ShellServer.class );
        when( server.getName() ).thenReturn( "Server" );
    }
}
