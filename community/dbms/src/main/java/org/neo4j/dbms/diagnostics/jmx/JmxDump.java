/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.dbms.diagnostics.jmx;

import com.sun.management.HotSpotDiagnosticMXBean;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import javax.management.InstanceNotFoundException;
import javax.management.JMX;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.neo4j.diagnostics.DiagnosticsReportSource;
import org.neo4j.diagnostics.DiagnosticsReportSources;
import org.neo4j.diagnostics.DiagnosticsReporterProgress;
import org.neo4j.diagnostics.ProgressAwareInputStream;

/**
 * Encapsulates remoting functionality for collecting diagnostics information on running instances.
 */
public class JmxDump
{
    private final MBeanServerConnection mBeanServer;
    private Properties systemProperties;

    private JmxDump( MBeanServerConnection mBeanServer )
    {
        this.mBeanServer = mBeanServer;
    }

    public static JmxDump connectTo( String jmxAddress ) throws IOException
    {
        JMXServiceURL url = new JMXServiceURL( jmxAddress );
        JMXConnector connect = JMXConnectorFactory.connect( url );

        return new JmxDump( connect.getMBeanServerConnection() );
    }

    public void attachSystemProperties( Properties systemProperties )
    {
        this.systemProperties = systemProperties;
    }

    /**
     * Captures a thread dump of the running instance.
     *
     * @return a diagnostics source the will emmit a thread dump.
     */
    public DiagnosticsReportSource threadDump()
    {
        return DiagnosticsReportSources.newDiagnosticsString( "threaddump.txt", () ->
        {
            String result;
            try
            {
                // Try to invoke real thread dump
                result = (String) mBeanServer
                        .invoke( new ObjectName( "com.sun.management:type=DiagnosticCommand" ), "threadPrint",
                                new Object[]{null}, new String[]{String[].class.getName()} );
            }
            catch ( InstanceNotFoundException | ReflectionException | MBeanException | MalformedObjectNameException | IOException exception )
            {
                // Failed, do a poor mans attempt
                result = getMXThreadDump();
            }

            return result;
        } );
    }

    /**
     * If "DiagnosticCommand" bean isn't available, for reasons unknown, try our best to reproduce the output. For obvious
     * reasons we can't get everything, since it's not exposed in java.
     *
     * @return a thread dump.
     */
    private String getMXThreadDump()
    {
        ThreadMXBean threadMxBean;
        try
        {
            threadMxBean = ManagementFactory.getPlatformMXBean( mBeanServer, ThreadMXBean.class );
        }
        catch ( IOException e )
        {
            return "ERROR: Unable to produce any thread dump";
        }

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
                sb.append( "\tat " ).append( e.toString() );

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
                        sb.append( "\t-  locked ").append( mi ).append( '\n' );
                    }
                }
            }
        }

        return sb.toString();
    }

    public DiagnosticsReportSource heapDump()
    {
        return new DiagnosticsReportSource()
        {
            @Override
            public String destinationPath()
            {
                return "heapdump.hprof";
            }

            @Override
            public void addToArchive( Path archiveDestination, DiagnosticsReporterProgress progress )
                    throws IOException
            {
                // Heap dump has to target an actual file, we cannot stream directly to the archive
                progress.info( "dumping..." );
                Path tempFile = Files.createTempFile("neo4j-heapdump", ".hprof");
                Files.deleteIfExists( tempFile );
                heapDump( tempFile.toAbsolutePath().toString() );

                // Track progress of archiving process
                progress.info( "archiving..." );
                long size = Files.size( tempFile );
                InputStream in = Files.newInputStream( tempFile );
                try ( ProgressAwareInputStream inStream = new ProgressAwareInputStream( in, size, progress::percentChanged ) )
                {
                    Files.copy( inStream, archiveDestination );
                }

                Files.delete( tempFile );
            }

            @Override
            public long estimatedSize( DiagnosticsReporterProgress progress ) throws IOException
            {
                MemoryMXBean bean = ManagementFactory.getPlatformMXBean( mBeanServer, MemoryMXBean.class );
                long totalMemory = bean.getHeapMemoryUsage().getCommitted() + bean.getNonHeapMemoryUsage().getCommitted();

                // We first write raw to disk then write to archive, 5x compression is a reasonable worst case estimation
                return (long) (totalMemory * 1.2);
            }
        };
    }

    /**
     * @param destination file path to send heap dump to, has to end with ".hprof"
     */
    private void heapDump( String destination ) throws IOException
    {
        HotSpotDiagnosticMXBean hotSpotDiagnosticMXBean =
                ManagementFactory.getPlatformMXBean( mBeanServer, HotSpotDiagnosticMXBean.class );
        hotSpotDiagnosticMXBean.dumpHeap( destination, false );
    }

    public DiagnosticsReportSource systemProperties()
    {
        return new DiagnosticsReportSource()
        {
            @Override
            public String destinationPath()
            {
                return "vm.prop";
            }

            @Override
            public void addToArchive( Path archiveDestination, DiagnosticsReporterProgress progress )
                    throws IOException
            {
                try ( PrintStream printStream = new PrintStream( Files.newOutputStream( archiveDestination ) ) )
                {
                    systemProperties.list( printStream );
                }
            }

            @Override
            public long estimatedSize( DiagnosticsReporterProgress progress )
            {
                return 0;
            }
        };
    }

    public DiagnosticsReportSource environmentVariables()
    {
        return newReportsBeanSource( "env.prop", Reports::getEnvironmentVariables );
    }

    public DiagnosticsReportSource listTransactions()
    {
        return newReportsBeanSource( "listTransactions.txt", Reports::listTransactions );
    }

    private DiagnosticsReportSource newReportsBeanSource( String destination, ReportsInvoker reportsInvoker )
    {
        return DiagnosticsReportSources.newDiagnosticsString( destination, () ->
        {
            try
            {
                ObjectName name = new ObjectName( "org.neo4j:instance=kernel#0,name=Reports" );
                Reports reportsBean = JMX.newMBeanProxy( mBeanServer, name, Reports.class );
                return reportsInvoker.invoke( reportsBean );
            }
            catch ( MalformedObjectNameException ignored )
            {
            }
            return "Unable to invoke ReportsBean";
        } );
    }

    private interface ReportsInvoker
    {
        String invoke( Reports r );
    }
}
