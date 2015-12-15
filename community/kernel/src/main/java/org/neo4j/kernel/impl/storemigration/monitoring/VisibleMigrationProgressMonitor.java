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
package org.neo4j.kernel.impl.storemigration.monitoring;

import org.neo4j.logging.Log;

import static java.lang.String.format;

public class VisibleMigrationProgressMonitor implements MigrationProgressMonitor
{
    static final String MESSAGE_STARTED = "Starting upgrade of database";
    static final String MESSAGE_COMPLETED = "Successfully finished upgrade of database";

    private final Log log;

    public VisibleMigrationProgressMonitor( Log log )
    {
        this.log = log;
    }

    @Override
    public void started()
    {
        log.info( MESSAGE_STARTED );
    }

    @Override
    public Section startSection( String name )
    {
        log.info( "Migrating " + name + ":" );

        return new ProgressSection();
    }

    @Override
    public void completed()
    {
        log.info( MESSAGE_COMPLETED );
    }

    private class ProgressSection implements Section
    {
        private static final int STRIDE = 10;

        private long current;
        private int currentPercent;
        private long max;

        @Override
        public void progress( long add )
        {
            current += add;
            int percent = max == 0 ? 100 : (int) (current*100/max);
            ensurePercentReported( percent );
        }

        private void ensurePercentReported( int percent )
        {
            while ( currentPercent < percent )
            {
                reportPercent( ++currentPercent );
            }
        }

        private void reportPercent( int percent )
        {
            if ( percent % STRIDE == 0 )
            {
                log.info( format( "  %d%% completed", percent ) );
            }
        }

        @Override
        public void start( long max )
        {
            this.max = max;
        }

        @Override
        public void completed()
        {
            ensurePercentReported( 100 );
        }
    }
}
