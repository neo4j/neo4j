/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.test.server;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;

import java.util.concurrent.Callable;

import org.neo4j.server.NeoServer;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.test.Mute;

import static org.neo4j.test.Mute.muteAll;

public class SharedServerTestBase
{
    private static boolean useExternal = Boolean.valueOf( System.getProperty( "neo-server.external", "false" ) );

    protected static NeoServer server()
    {
        return server;
    }

    protected final void cleanDatabase()
    {
        if ( !useExternal )
        {
            ServerHelper.cleanTheDatabase( server );
        }
    }

    private static NeoServer server;

	@Rule
	public Mute mute = muteAll();

    @BeforeClass
    public static void allocateServer() throws Throwable
    {
        if ( !useExternal )
        {
            muteAll().call( new Callable<Void>()
            {
                @Override
                public Void call() throws Exception
                {
                    server = ServerHolder.allocate();
                    return null;
                }
            } );
        }
    }

    @AfterClass
    public static void releaseServer() throws Exception
    {
        if ( !useExternal )
        {
            try
            {
                muteAll().call( new Callable<Void>()
                {
                    @Override
                    public Void call() throws Exception
                    {
                        ServerHolder.release( server );
                        return null;
                    }
                } );
            }
            finally
            {
                server = null;
            }
        }
    }
}
