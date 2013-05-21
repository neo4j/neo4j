/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.ha;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState;

/**
 * The instance guard is what ensures that the database will only take calls when it is in an ok state. It changes
 * whenever the ClusterMemberState changes, and then allows query handling to easily determine if it is ok to call
 * the database by calling {@link #await(long)}.
 */
public class InstanceAccessGuard
{
    private volatile CountDownLatch latch;

    public InstanceAccessGuard()
    {
        enter();
    }

    synchronized void enter()
    {
        if ( latch != null )
        {
            return;
        }
        latch = new CountDownLatch( 1 );
    }

    public boolean await( long millis )
    {
        CountDownLatch localLatch = latch;
        if ( localLatch == null )
        {
            return true;
        }
        try
        {
            return localLatch.await( millis, TimeUnit.MILLISECONDS );
        }
        catch ( InterruptedException e )
        {
            return false;
        }
    }

    synchronized void exit()
    {
        if ( latch == null )
        {
            return;
        }
        latch.countDown();
        latch = null;
    }

    public void setState( HighAvailabilityMemberState state )
    {
        if ( state.isAccessAllowed( null ) )
        {
            exit();
        }
        else
        {
            enter();
        }
    }
}
