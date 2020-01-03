/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.dbms.archive;

import java.io.PrintStream;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.graphdb.Resource;
import org.neo4j.io.ByteUnit;

class ArchiveProgressPrinter
{
    private final AtomicBoolean printUpdate;
    private final PrintStream output;
    private final boolean interactive;
    private long currentBytes;
    private long currentFiles;
    private boolean done;
    long maxBytes;
    long maxFiles;

    ArchiveProgressPrinter( PrintStream output )
    {
        this.output = output;
        printUpdate = new AtomicBoolean();
        interactive = System.console() != null;
    }

    Resource startPrinting()
    {
        ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
        ScheduledFuture<?> timerFuture = timer.scheduleAtFixedRate( this::printOnNextUpdate, 0, interactive ? 100 : 5_000, TimeUnit.MILLISECONDS );
        return () ->
        {
            timerFuture.cancel( false );
            timer.shutdown();
            try
            {
                timer.awaitTermination( 10, TimeUnit.SECONDS );
            }
            catch ( InterruptedException ignore )
            {
            }
            done();
            printProgress();
        };
    }

    void printOnNextUpdate()
    {
        printUpdate.set( true );
    }

    void reset()
    {
        maxBytes = 0;
        maxFiles = 0;
        currentBytes = 0;
        currentFiles = 0;
    }

    void beginFile()
    {
        currentFiles++;
    }

    void addBytes( long n )
    {
        currentBytes += n;
        if ( printUpdate.get() )
        {
            printProgress();
            printUpdate.set( false );
        }
    }

    void endFile()
    {
        printProgress();
    }

    void done()
    {
        done = true;
    }

    void printProgress()
    {
        if ( output != null )
        {
            char lineSep = interactive ? '\r' : '\n';
            if ( done )
            {
                output.println( lineSep + "Done: " + currentFiles + " files, " + ByteUnit.bytesToString( currentBytes ) + " processed." );
            }
            else if ( maxFiles > 0 && maxBytes > 0 )
            {
                double progress = (currentBytes / (double) maxBytes) * 100;
                output.print( lineSep + "Files: " + currentFiles + '/' + maxFiles + ", data: " + String.format( "%4.1f%%", progress ) );
            }
            else
            {
                output.print( lineSep + "Files: " + currentFiles + "/?" + ", data: ??.?%" );
            }
        }
    }
}
