/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.ha.shell;

import java.rmi.RemoteException;

import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Continuation;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.kernel.apps.NonTransactionProvidingApp;

public class Pullupdates extends NonTransactionProvidingApp
{
    @Override
    protected Continuation exec( AppCommandParser parser, Session session, Output out )
            throws ShellException
    {
        try
        {
            getServer().getDb().getDependencyResolver().resolveDependency( UpdatePuller.class ).pullUpdates();
        }
        catch ( IllegalArgumentException e )
        {
            throw new ShellException( "Couldn't pull updates. Not a highly available database?" );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
        return Continuation.INPUT_COMPLETE;
    }
}
