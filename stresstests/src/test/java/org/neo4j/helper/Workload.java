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
package org.neo4j.helper;

import org.neo4j.causalclustering.stresstests.Control;

public abstract class Workload implements Runnable
{
    private final Control control;
    private final long sleepTimeMillis;

    public Workload( Control control )
    {
        this( control, 0 );
    }

    @SuppressWarnings( "WeakerAccess" )
    public Workload( Control control, long sleepTimeMillis )
    {
        this.control = control;
        this.sleepTimeMillis = sleepTimeMillis;
    }

    @Override
    public final void run()
    {
        try
        {
            while ( control.keepGoing() )
            {
                doWork();
                if ( sleepTimeMillis != 0 )
                {
                    Thread.sleep( sleepTimeMillis );
                }
            }
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
        }
        catch ( Throwable t )
        {
            control.onFailure( t );
        }
    }

    protected abstract void doWork() throws Exception;

    @SuppressWarnings( "RedundantThrows" )
    public void prepare() throws Exception
    {
    }

    @SuppressWarnings( "RedundantThrows" )
    public void validate() throws Exception
    {
    }
}
