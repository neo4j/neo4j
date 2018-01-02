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

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Map;

import org.neo4j.shell.CtrlCHandler;
import org.neo4j.shell.InterruptSignalHandler;
import org.neo4j.shell.Output;
import org.neo4j.shell.ShellClient;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.ShellServer;

/**
 * An implementation of {@link ShellClient} optimized to use with a server
 * in the same JVM.
 */
public class SameJvmClient extends AbstractClient
{
    private final Output out;
    private final ShellServer server;

    public SameJvmClient( Map<String, Serializable> initialSession, ShellServer server,
                          CtrlCHandler ctrlcHandler ) throws ShellException
    {
        this( initialSession, server, new SystemOutput(), ctrlcHandler );
    }

    public SameJvmClient( Map<String, Serializable> initialSession, ShellServer server,
                          Output out ) throws ShellException
    {
        this( initialSession, server, out, InterruptSignalHandler.getHandler() );
    }

    /**
     * @param server the server to communicate with.
     */
    public SameJvmClient( Map<String, Serializable> initialSession, ShellServer server, Output out,
                          CtrlCHandler ctrlcHandler ) throws ShellException
    {
        super( initialSession, ctrlcHandler );
        this.out = out;
        this.server = server;
        try
        {
            sayHi( server );
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException( "Will not happen since this is in the same JVM", e );
        }

        init();
        updateTimeForMostRecentConnection();
    }

    @Override
    public Output getOutput()
    {
        return this.out;
    }

    @Override
    public ShellServer getServer()
    {
        return this.server;
    }
}
