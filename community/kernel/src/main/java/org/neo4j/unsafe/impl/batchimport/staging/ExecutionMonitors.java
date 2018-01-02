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
package org.neo4j.unsafe.impl.batchimport.staging;

import java.io.InputStream;

import static org.neo4j.unsafe.impl.batchimport.staging.HumanUnderstandableExecutionMonitor.NO_MONITOR;

/**
 * Common {@link ExecutionMonitor} implementations.
 */
public class ExecutionMonitors
{
    private ExecutionMonitors()
    {
        throw new AssertionError( "No instances allowed" );
    }

    public static ExecutionMonitor defaultVisible()
    {
        return defaultVisible( System.in );
    }

    public static ExecutionMonitor defaultVisible( InputStream in )
    {
        ProgressRestoringMonitor monitor = new ProgressRestoringMonitor();
        return new MultiExecutionMonitor(
                new HumanUnderstandableExecutionMonitor( System.out, NO_MONITOR, monitor ),
                new OnDemandDetailsExecutionMonitor( System.out, in, monitor ) );
    }

    private static final ExecutionMonitor INVISIBLE = new ExecutionMonitor()
    {
        @Override
        public void start( StageExecution execution )
        {   // Do nothing
        }

        @Override
        public void end( StageExecution execution, long totalTimeMillis )
        {   // Do nothing
        }

        @Override
        public long nextCheckTime()
        {
            return Long.MAX_VALUE;
        }

        @Override
        public void check( StageExecution execution )
        {   // Do nothing
        }

        @Override
        public void done( long totalTimeMillis, String additionalInformation )
        {   // Do nothing
        }
    };

    public static ExecutionMonitor invisible()
    {
        return INVISIBLE;
    }
}
