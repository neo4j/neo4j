/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.causalclustering.readreplica;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.neo4j.causalclustering.catchup.tx.CatchupPollingProcess;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.concurrent.TimeUnit.MINUTES;

class WaitForUpToDateStore extends LifecycleAdapter
{
    private final CatchupPollingProcess catchupProcess;
    private final Log log;

    WaitForUpToDateStore( CatchupPollingProcess catchupProcess, LogProvider logProvider )
    {
        this.catchupProcess = catchupProcess;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void start() throws Throwable
    {
        waitForUpToDateStore();
    }

    private void waitForUpToDateStore() throws InterruptedException, ExecutionException
    {
        boolean upToDate = false;
        do
        {
            try
            {
                upToDate = catchupProcess.upToDateFuture().get( 1, MINUTES );
            }
            catch ( TimeoutException e )
            {
                log.warn( "Waiting for up-to-date store is taking an unusually long time" );
            }
        }
        while ( !upToDate );
    }
}
