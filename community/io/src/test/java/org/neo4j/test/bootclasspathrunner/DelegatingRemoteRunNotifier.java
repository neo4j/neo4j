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

class DelegatingRemoteRunNotifier implements RemoteRunNotifier
{
    private final RunNotifier delegate;

    public DelegatingRemoteRunNotifier( RunNotifier delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public void addListener( RunListener listener )
    {
        delegate.addListener( listener );
    }

    @Override
    public void fireTestRunFinished( Result result )
    {
        delegate.fireTestRunFinished( result );
    }

    @Override
    public void pleaseStop()
    {
        delegate.pleaseStop();
    }

    @Override
    public void fireTestIgnored( Description description )
    {
        delegate.fireTestIgnored( description );
    }

    @Override
    public void fireTestStarted( Description description ) throws StoppedByUserException
    {
        delegate.fireTestStarted( description );
    }

    @Override
    public void fireTestFinished( Description description )
    {
        delegate.fireTestFinished( description );
    }

    @Override
    public void fireTestAssumptionFailed( Failure failure )
    {
        delegate.fireTestAssumptionFailed( failure );
    }

    @Override
    public void addFirstListener( RunListener listener )
    {
        delegate.addFirstListener( listener );
    }

    @Override
    public void removeListener( RunListener listener )
    {
        delegate.removeListener( listener );
    }

    @Override
    public void fireTestRunStarted( Description description )
    {
        delegate.fireTestRunStarted( description );
    }

    @Override
    public void fireTestFailure( Failure failure )
    {
        delegate.fireTestFailure( failure );
    }
}
