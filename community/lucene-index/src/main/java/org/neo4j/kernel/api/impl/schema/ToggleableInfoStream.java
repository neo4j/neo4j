/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.schema;

import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.PrintStreamInfoStream;

import java.io.IOException;
import java.nio.file.attribute.FileTime;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.logging.Log;

/**
 * This class is more or less copied from {@link PrintStreamInfoStream}
 * with the change that it prints to {@link Log} instead of {@link java.io.PrintStream}.
 */
public class ToggleableInfoStream extends InfoStream
{
    private static final AtomicInteger MESSAGE_ID = new AtomicInteger();
    private static volatile boolean VERBOSE_LUCENE_TO_LOG = false;
    private final Log log;
    private final int messageID;

    ToggleableInfoStream( Log log )
    {
        this.log = log;
        this.messageID = MESSAGE_ID.getAndIncrement();
    }

    @Override
    public void message( String component, String message )
    {
        // todo if toggle
        if ( VERBOSE_LUCENE_TO_LOG )
        {
            log.debug( "Lucene:" + component + " " + messageID + " [" + Thread.currentThread().getName() + "]: " + message );
        }
    }

    @Override
    public boolean isEnabled( String component )
    {
        return true;
    }

    @Override
    public void close() throws IOException
    {   // no-op
    }

    /** Returns the current time as string for insertion into log messages. */
    protected String getTimestamp() {
        // We "misuse" Java 7 FileTime API here, because it returns a nice ISO-8601 string with milliseconds (UTC timezone).
        // The alternative, SimpleDateFormat is not thread safe!
        return FileTime.fromMillis(System.currentTimeMillis()).toString();
    }

    public static void toggle( boolean enable )
    {
        VERBOSE_LUCENE_TO_LOG = enable;
    }

    public static boolean isEnabled()
    {
        return VERBOSE_LUCENE_TO_LOG;
    }
}
