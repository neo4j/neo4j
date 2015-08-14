/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.consistency.checking.full;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Base class for workers that processes records during consistency check.
 */
public abstract class RecordCheckWorker<RECORD> implements Runnable
{
    private volatile boolean done;
    protected final BlockingQueue<RECORD> recordsQ;

    public RecordCheckWorker( BlockingQueue<RECORD> recordsQ )
    {
        this.recordsQ = recordsQ;
    }

    public void done()
    {
        done = true;
    }

    @Override
    public void run()
    {
        while ( !done || !recordsQ.isEmpty() )
        {
            RECORD record;
            try
            {
                record = recordsQ.poll( 10, TimeUnit.MILLISECONDS );
                if ( record != null )
                {
                    process( record );
                }
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
                break;
            }
        }
    }

    protected abstract void process( RECORD record );
}
