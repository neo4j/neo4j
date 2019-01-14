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
package org.neo4j.kernel.info;

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
import java.nio.file.Files;
import java.time.zone.ZoneRulesProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Stream;

import org.neo4j.io.os.OsBeanUtil;
import org.neo4j.logging.Logger;

import static java.net.NetworkInterface.getNetworkInterfaces;
import static org.neo4j.helpers.Format.bytes;

enum SystemDiagnostics implements DiagnosticsProvider
{
    SYSTEM_MEMORY( "System memory information:" )
    {
        @Override
        void dump( Logger logger )
        {
            logBytes( logger, "Total Physical memory: ", OsBeanUtil.getTotalPhysicalMemory() );
            logBytes( logger, "Free Physical memory: ", OsBeanUtil.getFreePhysicalMemory() );
            logBytes( logger, "Committed virtual memory: ", OsBeanUtil.getCommittedVirtualMemory() );
            logBytes( logger, "Total swap space: ", OsBeanUtil.getTotalSwapSpace() );
            logBytes( logger, "Free swap space: ", OsBeanUtil.getFreeSwapSpace() );
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
            logLong( logger, "Max number of file descriptors: ", OsBeanUtil.getMaxFileDescriptors() );
            logLong( logger, "Number of open file descriptors: ", OsBeanUtil.getOpenFileDescriptors() );
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
            Map<String, String> paths = new HashMap<>();
            assert pathKeys.length == classPaths.length;
            for ( int i = 0; i < classPaths.length; i++ )
            {
                for ( String path : classPaths[i].split( File.pathSeparator ) )
                {
                    paths.put( canonicalize( path ), pathValue( paths, pathKeys[i], path ) );
                }
            }
            for ( int level = 0; loader != null; level++ )
            {
                if ( loader instanceof URLClassLoader )
                {
                    URLClassLoader urls = (URLClassLoader) loader;
                    URL[] classLoaderUrls = urls.getURLs();
                    if ( classLoaderUrls != null )
                    {
                        for ( URL url : classLoaderUrls )
                        {
                            if ( "file".equalsIgnoreCase( url.getProtocol() ) )
                            {
                                paths.put( url.toString(), pathValue( paths, "loader." + level, url.getPath() ) );
                            }
                        }
                    }
                    else
                    {
                        paths.put( loader.toString(), "<ClassLoader unexpectedly has null URL array>" );
                    }
                }
                loader = loader.getParent();
            }
            List<String> result = new ArrayList<>( paths.size() );
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
                    if ( key.startsWith( "java." ) || key.startsWith( "os." ) || key.endsWith( ".boot.class.path" ) ||
                            key.equals( "line.separator" ) )
                    {
                        continue;
                    }
                    logger.log( key + " = " + System.getProperty( key ) );
                }
            }
        }
    },
    TIMEZONE_DATABASE( "(IANA) TimeZone Database Version:" )
    {
        @Override
        void dump( Logger logger )
        {
            Map<String,Integer> versions = new HashMap<>();
            for ( String tz : ZoneRulesProvider.getAvailableZoneIds() )
            {
                for ( String version : ZoneRulesProvider.getVersions( tz ).keySet() )
                {
                    versions.compute( version, ( key, value ) -> value == null ? 1 : (value + 1) );
                }
            }
            String[] sorted = versions.keySet().toArray( new String[0] );
            Arrays.sort( sorted );
            for ( String tz : sorted )
            {
                logger.log( "  TimeZone version: %s (available for %d zone identifiers)", tz, versions.get( tz ) );
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
            File[] files = SYS_BLOCK.listFiles( File::isDirectory );
            if ( files != null )
            {
                for ( File subdir : files )
                {
                    File scheduler = new File( subdir, "queue/scheduler" );
                    if ( scheduler.isFile() )
                    {
                        try ( Stream<String> lines = Files.lines( scheduler.toPath() ) )
                        {
                            lines.forEach( logger::log );
                        }
                        catch ( IOException e )
                        {
                            // ignore
                        }
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
            }
            catch ( SocketException e )
            {
                logger.log( "ERROR: failed to inspect network interfaces and addresses: " + e.getMessage() );
            }
        }
    },
    ;

    private final String message;

    SystemDiagnostics( String message )
    {
        this.message = message;
    }

    static void registerWith( DiagnosticsManager manager )
    {
        for ( SystemDiagnostics provider : values() )
        {
            if ( provider.isApplicable() )
            {
                manager.appendProvider( provider );
            }
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

    private static void logBytes( Logger logger, String message, long value )
    {
        if ( value != OsBeanUtil.VALUE_UNAVAILABLE )
        {
            logger.log( message + bytes( value ) );
        }
    }

    private static void logLong( Logger logger, String message, long value )
    {
        if ( value != OsBeanUtil.VALUE_UNAVAILABLE )
        {
            logger.log( message + value );
        }
    }
}
