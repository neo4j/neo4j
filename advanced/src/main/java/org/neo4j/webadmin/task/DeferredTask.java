/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.webadmin.task;

/**
 * This is used primarily to perform long-running tasks without having to keep
 * web clients waiting, or to perform some task that requires the server to be
 * shut down.
 * 
 * The current implementation spawns one thread per deferred task instance,
 * which may not be desirable if you end up spawning lots of tasks. This is a
 * KISS implementation, extend it with some task queueing magic if the need
 * arises.
 * 
 * @author Jacob Hansson <jacob@voltvoodoo.com>
 * 
 */
public class DeferredTask implements Runnable
{

    protected Runnable task;
    protected long timeout;

    public static void defer( Runnable task )
    {
        defer( task, 0 );
    }

    public static void defer( Runnable task, long timeout )
    {

        Thread runner = new Thread( new DeferredTask( task, timeout ),
                "Deferred task" );
        runner.start();
    }

    //
    // CONSTRUCT
    //

    protected DeferredTask( Runnable task, long timeout )
    {
        this.timeout = timeout;
        this.task = task;
    }

    //
    // PUBLIC
    //

    public void run()
    {
        try
        {
            Thread.sleep( timeout );
            this.task.run();
        }
        catch ( InterruptedException e )
        {
            e.printStackTrace();
        }
    }
}
