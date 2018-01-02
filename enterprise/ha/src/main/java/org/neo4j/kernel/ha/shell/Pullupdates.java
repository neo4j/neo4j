/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
            throws ShellException, RemoteException
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
