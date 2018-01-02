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
import org.junit.runner.notification.RunNotifier;
import org.junit.runner.notification.StoppedByUserException;

import java.rmi.RemoteException;

class DelegatingRunNotifier extends RunNotifier
{
    private final RemoteRunNotifier delegate;

    public DelegatingRunNotifier( RemoteRunNotifier delegate )
    {
        this.delegate = delegate;
    }

    public void addListener( RunListener listener )
    {
        try
        {
            delegate.addListener( listener );
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException( e );
        }
    }

    public void fireTestFinished( Description description )
    {
        try
        {
            delegate.fireTestFinished( description );
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException( e );
        }
    }

    public void fireTestIgnored( Description description )
    {
        try
        {
            delegate.fireTestIgnored( description );
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException( e );
        }
    }

    public void fireTestAssumptionFailed( Failure failure )
    {
        try
        {
            delegate.fireTestAssumptionFailed( failure );
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException( e );
        }
    }

    public void fireTestFailure( Failure failure )
    {
        try
        {
            delegate.fireTestFailure( failure );
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException( e );
        }
    }

    public void addFirstListener( RunListener listener )
    {
        try
        {
            delegate.addFirstListener( listener );
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException( e );
        }
    }

    public void fireTestRunFinished( Result result )
    {
        try
        {
            delegate.fireTestRunFinished( result );
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException( e );
        }
    }

    public void fireTestRunStarted( Description description )
    {
        try
        {
            delegate.fireTestRunStarted( description );
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException( e );
        }
    }

    public void fireTestStarted( Description description ) throws StoppedByUserException
    {
        try
        {
            delegate.fireTestStarted( description );
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException( e );
        }
    }

    public void removeListener( RunListener listener )
    {
        try
        {
            delegate.removeListener( listener );
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException( e );
        }
    }

    public void pleaseStop()
    {
        try
        {
            delegate.pleaseStop();
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException( e );
        }
    }
}
