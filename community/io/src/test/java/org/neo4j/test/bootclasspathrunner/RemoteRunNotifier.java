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
package org.neo4j.test.bootclasspathrunner;

import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;
import org.junit.runner.notification.StoppedByUserException;

import java.rmi.Remote;
import java.rmi.RemoteException;

interface RemoteRunNotifier extends Remote
{
    void addListener( RunListener listener ) throws RemoteException;

    void fireTestRunFinished( Result result ) throws RemoteException;

    void pleaseStop() throws RemoteException;

    void fireTestIgnored( Description description ) throws RemoteException;

    void fireTestStarted( Description description ) throws StoppedByUserException, RemoteException;

    void fireTestFinished( Description description ) throws RemoteException;

    void fireTestAssumptionFailed( Failure failure ) throws RemoteException;

    void addFirstListener( RunListener listener ) throws RemoteException;

    void removeListener( RunListener listener ) throws RemoteException;

    void fireTestRunStarted( Description description ) throws RemoteException;

    void fireTestFailure( Failure failure ) throws RemoteException;
}
