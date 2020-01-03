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
package org.neo4j.diagnostics.utils;

import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Properties;

public class DumpUtils
{
    private DumpUtils()
    {
    }

    /**
     * Creates threads dump and try to mimic JVM stack trace as much as possible to allow existing analytics tools to be used
     *
     * @return string that contains thread dump
     */
    public static String threadDump()
    {
        ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
        Properties systemProperties = System.getProperties();

        return threadDump( threadMxBean, systemProperties );
    }

    /**
     * Creates threads dump and try to mimic JVM stack trace as much as possible to allow existing analytics tools to be used
     *
     * @param threadMxBean bean to use for thread dump
     * @param systemProperties dumped vm system properties
     * @return string that contains thread dump
     */
    public static String threadDump( ThreadMXBean threadMxBean, Properties systemProperties )
    {
        ThreadInfo[] threadInfos = threadMxBean.dumpAllThreads( true, true );

        // Reproduce JVM stack trace as far as possible to allow existing analytics tools to be used
        String vmName = systemProperties.getProperty( "java.vm.name" );
        String vmVersion = systemProperties.getProperty( "java.vm.version" );
        String vmInfoString = systemProperties.getProperty( "java.vm.info" );

        StringBuilder sb = new StringBuilder();
        sb.append( String.format( "Full thread dump %s (%s %s):\n\n", vmName, vmVersion, vmInfoString ) );
        for ( ThreadInfo threadInfo : threadInfos )
        {
            sb.append( String.format( "\"%s\" #%d\n", threadInfo.getThreadName(), threadInfo.getThreadId() ) );
            sb.append( "   java.lang.Thread.State: " ).append( threadInfo.getThreadState() ).append( "\n" );

            StackTraceElement[] stackTrace = threadInfo.getStackTrace();
            for ( int i = 0; i < stackTrace.length; i++ )
            {
                StackTraceElement e = stackTrace[i];
                sb.append( "\tat " ).append( e.toString() ).append( '\n' );

                // First stack element info can be found in the thread state
                if ( i == 0 && threadInfo.getLockInfo() != null )
                {
                    Thread.State ts = threadInfo.getThreadState();
                    switch ( ts )
                    {
                    case BLOCKED:
                        sb.append( "\t-  blocked on " ).append( threadInfo.getLockInfo() ).append( '\n' );
                        break;
                    case WAITING:
                        sb.append( "\t-  waiting on " ).append( threadInfo.getLockInfo() ).append( '\n' );
                        break;
                    case TIMED_WAITING:
                        sb.append( "\t-  waiting on " ).append( threadInfo.getLockInfo() ).append( '\n' );
                        break;
                    default:
                    }
                }
                for ( MonitorInfo mi : threadInfo.getLockedMonitors() )
                {
                    if ( mi.getLockedStackDepth() == i )
                    {
                        sb.append( "\t-  locked " ).append( mi ).append( '\n' );
                    }
                }
            }
        }

        return sb.toString();
    }
}
