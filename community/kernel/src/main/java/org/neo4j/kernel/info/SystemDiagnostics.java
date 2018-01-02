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
package org.neo4j.kernel.info;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.lang.management.CompilationMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.neo4j.kernel.impl.util.OsBeanUtil;
import org.neo4j.logging.Logger;

import static java.net.NetworkInterface.getNetworkInterfaces;
import static org.neo4j.helpers.Format.bytes;
import static org.neo4j.io.fs.FileUtils.newBufferedFileReader;
import static org.neo4j.kernel.impl.util.Charsets.UTF_8;

enum SystemDiagnostics implements DiagnosticsProvider
{
    SYSTEM_MEMORY( "System memory information:" )
    {
        @Override
        void dump( Logger logger )
        {
            logOsBeanValue( logger, "Total Physical memory: ", OsBeanUtil.getTotalPhysicalMemory() );
            logOsBeanValue( logger, "Free Physical memory: ", OsBeanUtil.getFreePhysicalMemory() );
            logOsBeanValue( logger, "Committed virtual memory: ", OsBeanUtil.getCommittedVirtualMemory() );
            logOsBeanValue( logger, "Total swap space: ", OsBeanUtil.getTotalSwapSpace() );
            logOsBeanValue( logger, "Free swap space: ", OsBeanUtil.getFreeSwapSpace() );
        }
    },
    JAVA_MEMORY( "JVM memory information:" )
    {
        @Override
        void dump( Logger logger )
        {
            logger.log( "Free  memory: " + bytes( Runtime.getRuntime().freeMemory() ) );
            logger.log( "Total memory: " + bytes( Runtime.getRuntime().totalMemory() ) );
            logger.log( "Max   memory: " + bytes( Runtime.getRuntime().maxMemory() ) );
            for ( GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans() )
            {
                logger.log( "Garbage Collector: " + gc.getName() + ": " + Arrays.toString( gc.getMemoryPoolNames() ) );
            }
            for ( MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans() )
            {
                MemoryUsage usage = pool.getUsage();
                logger.log( String.format( "Memory Pool: %s (%s): committed=%s, used=%s, max=%s, threshold=%s",
                        pool.getName(), pool.getType(), usage == null ? "?" : bytes( usage.getCommitted() ),
                        usage == null ? "?" : bytes( usage.getUsed() ), usage == null ? "?" : bytes( usage.getMax() ),
                        pool.isUsageThresholdSupported() ? bytes( pool.getUsageThreshold() ) : "?" ) );
            }
        }
    },
    OPERATING_SYSTEM( "Operating system information:" )
    {
        @Override
        void dump( Logger logger )
        {
            OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
            RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
            logger.log( String.format( "Operating System: %s; version: %s; arch: %s; cpus: %s", os.getName(),
                    os.getVersion(), os.getArch(), os.getAvailableProcessors() ) );
            logOsBeanValue( logger, "Max number of file descriptors: ", OsBeanUtil.getMaxFileDescriptors() );
            logOsBeanValue( logger, "Number of open file descriptors: ", OsBeanUtil.getOpenFileDescriptors() );
            logger.log( "Process id: " + runtime.getName() );
            logger.log( "Byte order: " + ByteOrder.nativeOrder() );
            logger.log( "Local timezone: " + getLocalTimeZone() );
        }

        private String getLocalTimeZone()
        {
            TimeZone tz = Calendar.getInstance().getTimeZone();
            return tz.getID();
        }
    },
    JAVA_VIRTUAL_MACHINE( "JVM information:" )
    {
        @Override
        void dump( Logger logger )
        {
            RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
            logger.log( "VM Name: " + runtime.getVmName() );
            logger.log( "VM Vendor: " + runtime.getVmVendor() );
            logger.log( "VM Version: " + runtime.getVmVersion() );
            CompilationMXBean compiler = ManagementFactory.getCompilationMXBean();
            logger.log( "JIT compiler: " + ( ( compiler == null ) ? "unknown" : compiler.getName() ) );
            logger.log( "VM Arguments: " + runtime.getInputArguments() );
        }
    },
    CLASSPATH( "Java classpath:" )
    {
        @Override
        void dump( Logger logger )
        {
            RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
            Collection<String> classpath;
            if ( runtime.isBootClassPathSupported() )
            {
                classpath = buildClassPath( getClass().getClassLoader(),
                        new String[] { "bootstrap", "classpath" },
                        runtime.getBootClassPath(), runtime.getClassPath() );
            }
            else
            {
                classpath = buildClassPath( getClass().getClassLoader(),
                        new String[] { "classpath" }, runtime.getClassPath() );
            }
            for ( String path : classpath )
            {
                logger.log( path );
            }
        }

        private Collection<String> buildClassPath( ClassLoader loader, String[] pathKeys, String... classPaths )
        {
            Map<String, String> paths = new HashMap<String, String>();
            assert pathKeys.length == classPaths.length;
            for ( int i = 0; i < classPaths.length; i++ )
                for ( String path : classPaths[i].split( File.pathSeparator ) )
                    paths.put( canonicalize( path ), pathValue( paths, pathKeys[i], path ) );
            for ( int level = 0; loader != null; level++ )
            {
                if ( loader instanceof URLClassLoader )
                {
                    URLClassLoader urls = (URLClassLoader) loader;
                    for ( URL url : urls.getURLs() )
                        if ( "file".equalsIgnoreCase( url.getProtocol() ) )
                            paths.put( url.toString(), pathValue( paths, "loader." + level, url.getPath() ) );
                }
                loader = loader.getParent();
            }
            List<String> result = new ArrayList<String>( paths.size() );
            for ( Map.Entry<String, String> path : paths.entrySet() )
            {
                result.add( " [" + path.getValue() + "] " + path.getKey() );
            }
            return result;
        }

        private String pathValue( Map<String, String> paths, String key, String path )
        {
            String value;
            if ( null != ( value = paths.remove( canonicalize( path ) ) ) )
            {
                value += " + " + key;
            }
            else
            {
                value = key;
            }
            return value;
        }
    },
    LIBRARY_PATH( "Library path:" )
    {
        @Override
        void dump( Logger logger )
        {
            RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
            for ( String path : runtime.getLibraryPath().split( File.pathSeparator ) )
            {
                logger.log( canonicalize( path ) );
            }
        }
    },
    SYSTEM_PROPERTIES( "System.properties:" )
    {
        @Override
        void dump( Logger logger )
        {
            for ( Object property : System.getProperties().keySet() )
            {
                if ( property instanceof String )
                {
                    String key = (String) property;
                    if ( key.startsWith( "java." ) || key.startsWith( "os." ) || key.endsWith( ".boot.class.path" )
                         || key.equals( "line.separator" ) ) continue;
                    logger.log( key + " = " + System.getProperty( key ) );
                }
            }            
        }
    },
    LINUX_SCHEDULERS( "Linux scheduler information:" )
    {
        private final File SYS_BLOCK = new File( "/sys/block" );

        @Override
        boolean isApplicable()
        {
            return SYS_BLOCK.isDirectory();
        }

        @Override
        void dump( Logger logger )
        {
            for ( File subdir : SYS_BLOCK.listFiles( new java.io.FileFilter()
            {
                @Override
                public boolean accept( File path )
                {
                    return path.isDirectory();
                }
            } ) )
            {
                File scheduler = new File( subdir, "queue/scheduler" );
                if ( scheduler.isFile() )
                {
                    try
                    {
                        BufferedReader reader = newBufferedFileReader( scheduler, UTF_8 );
                        try
                        {
                            for ( String line; null != ( line = reader.readLine() ); )
                                logger.log( line );
                        }
                        finally
                        {
                            reader.close();
                        }
                    }
                    catch ( IOException e )
                    {
                        // ignore
                    }
                }
            }
        }
    },
    NETWORK( "Network information:" )
    {
        @Override
        void dump( Logger logger )
        {
            try
            {
                Enumeration<NetworkInterface> networkInterfaces = getNetworkInterfaces();

                while ( networkInterfaces.hasMoreElements() )
                {
                    NetworkInterface iface = networkInterfaces.nextElement();
                    logger.log( String.format( "Interface %s:", iface.getDisplayName() ) );

                    Enumeration<InetAddress> addresses = iface.getInetAddresses();
                    while ( addresses.hasMoreElements() )
                    {
                        InetAddress address = addresses.nextElement();
                        String hostAddress = address.getHostAddress();
                        logger.log( "    address: %s", hostAddress );
                    }
                }
            } catch ( SocketException e )
            {
                logger.log( "ERROR: failed to inspect network interfaces and addresses: " + e.getMessage() );
            }
        }
    },
    ;
    
    private final String message;

    private SystemDiagnostics(String message) {
        this.message = message;
    }
    
    static void registerWith( DiagnosticsManager manager )
    {
        for ( SystemDiagnostics provider : values() )
        {
            if ( provider.isApplicable() ) manager.appendProvider( provider );
        }
    }

    boolean isApplicable()
    {
        return true;
    }

    @Override
    public String getDiagnosticsIdentifier()
    {
        return name();
    }

    @Override
    public void acceptDiagnosticsVisitor( Object visitor )
    {
        // nothing visits this
    }

    @Override
    public void dump( DiagnosticsPhase phase, Logger logger )
    {
        if ( phase.isInitialization() || phase.isExplicitlyRequested() )
        {
            logger.log( message );
            dump( logger );
        }
    }

    abstract void dump( Logger logger );

    private static String canonicalize( String path )
    {
        try
        {
            return new File( path ).getCanonicalFile().getAbsolutePath();
        }
        catch ( IOException e )
        {
            return new File( path ).getAbsolutePath();
        }
    }

    private static void logOsBeanValue( Logger logger, String message, long value )
    {
        if ( value != OsBeanUtil.VALUE_UNAVAILABLE )
        {
            logger.log( message + bytes( value ) );
        }
    }
}
