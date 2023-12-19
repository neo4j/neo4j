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
