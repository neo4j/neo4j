/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.util;

import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Properties;

public final class DumpUtils
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
                sb.append( "\tat " ).append( e ).append( '\n' );

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
