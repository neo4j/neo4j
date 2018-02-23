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

import java.rmi.RemoteException;
import java.util.HashMap;

import org.junit.jupiter.api.Test;

import org.neo4j.helpers.Cancelable;
import org.neo4j.shell.impl.AbstractClient;
import org.neo4j.shell.impl.CollectingOutput;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CtrlCTest
{
    public class StubCtrlCHandler implements CtrlCHandler
    {
        public volatile boolean installed;

        @Override
        public Cancelable install( Runnable action )
        {
            installed = true;
            return () -> installed = false;
        }
    }

    @Test
    public void shouldInstallProvidedHandlerAfterReadingUserInput()
    {
        final StubCtrlCHandler handler = new StubCtrlCHandler();
        AbstractClient client = new AbstractClient( new HashMap<>(), handler )
        {
            @Override
            public ShellServer getServer()
            {
                try
                {
                    return new FakeShellServer( null );
                }
                catch ( RemoteException e )
                {
                    throw new RuntimeException( e );
                }
            }

            @Override
            public Output getOutput()
            {
                try
                {
                    return new CollectingOutput();
                }
                catch ( RemoteException e )
                {
                    throw new RuntimeException( e );
                }
            }

            @Override
            public String readLine( String ignored )
            {
                assertFalse( handler.installed, "handler installed, expected it to not be there" );
                return "CYPHER 2.1 RETURN 42;";
            }

            @Override
            public void evaluate( String output )
            {
                assertTrue( handler.installed, "handler not installed, but expected to be there" );
                end();
            }
        };
        client.grabPrompt();
        assertFalse( handler.installed, "handler installed, expected it to not be there" );
    }
}
