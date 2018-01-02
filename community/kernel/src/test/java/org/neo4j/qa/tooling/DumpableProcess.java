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
package org.neo4j.qa.tooling;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import org.junit.Ignore;

@Ignore( "Not a test, just a helper for DumpProcessInformationTest" )
public class DumpableProcess extends UnicastRemoteObject
{
    public DumpableProcess() throws RemoteException
    {
        super();
    }

    public static void main( String[] args ) throws Exception
    {
        new DumpableProcess().traceableMethod( args[0] );
    }

    public synchronized void traceableMethod( String signal ) throws Exception
    {
        // The parent process will listen to this signal to know that it's here.
        System.out.println( signal );

        wait();
    }
}
