/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.management.CompilationMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.util.StringLogger;

class ConfigurationLogging
{
    private static final String SUN_OS_BEAN = "com.sun.management.OperatingSystemMXBean";
    private static final String SUN_UNIX_BEAN = "com.sun.management.UnixOperatingSystemMXBean";
    private static final String IBM_OS_BEAN = "com.ibm.lang.management.OperatingSystemMXBean";

    static void logConfig( Map<Object, Object> params, Class<? extends GraphDatabaseService> graphDb,
            String storeDir, boolean dumpToConsole, StringLogger logger, AutoConfigurator autoConfigurator,
            NeoStoreXaDataSource ds )
    {
        logger.logMessage( "--- CONFIGURATION START ---" );
        logger.logMessage( "Graph Database: " + graphDb.getName() + " " + ds.getStoreId() );
        logger.logMessage( autoConfigurator.getNiceMemoryInformation() );
        logger.logMessage( "Kernel version: " + Version.getKernel() );
        logger.logMessage( "Neo4j component versions:" );
        for ( Version componentVersion : Service.load( Version.class ) )
        {
            logger.logMessage( "  " + componentVersion );
        }
        ds.logStoreVersions();
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        logger.logMessage( "Process id: " + runtime.getName() );
        OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        logger.logMessage( String.format( "Operating System: %s; version: %s; arch: %s; cpus: %s", os.getName(),
                os.getVersion(), os.getArch(), Integer.valueOf( os.getAvailableProcessors() ) ) );
        logBeanProperty( logger, "Max number of file descriptors: ", os, SUN_UNIX_BEAN, "getMaxFileDescriptorCount" );
        logBeanProperty( logger, "Number of open file descriptors: ", os, SUN_UNIX_BEAN, "getOpenFileDescriptorCount" );
        logBeanBytesProperty( logger, "Total Physical memory: ", os, SUN_OS_BEAN, "getTotalPhysicalMemorySize" );
        logBeanBytesProperty( logger, "Free Physical memory: ", os, SUN_OS_BEAN, "getFreePhysicalMemorySize" );
        logBeanBytesProperty( logger, "Committed virtual memory: ", os, SUN_OS_BEAN, "getCommittedVirtualMemorySize" );
        logBeanBytesProperty( logger, "Total swap space: ", os, SUN_OS_BEAN, "getTotalSwapSpaceSize" );
        logBeanBytesProperty( logger, "Free swap space: ", os, SUN_OS_BEAN, "getFreeSwapSpaceSize" );
        logBeanBytesProperty( logger, "Total physical memory: ", os, IBM_OS_BEAN, "getTotalPhysicalMemory" );
        logBeanBytesProperty( logger, "Free physical memory: ", os, IBM_OS_BEAN, "getFreePhysicalMemorySize" );
        logger.logMessage( "Byte order: " + ByteOrder.nativeOrder() );
        logger.logMessage( "VM Name: " + runtime.getVmName() );
        logger.logMessage( "VM Vendor: " + runtime.getVmVendor() );
        logger.logMessage( "VM Version: " + runtime.getVmVersion() );
        CompilationMXBean compiler = ManagementFactory.getCompilationMXBean();
        logger.logMessage( "JIT compiler: " + ( ( compiler == null ) ? "unknown" : compiler.getName() ) );
        Collection<String> classpath;
        if ( runtime.isBootClassPathSupported() )
        {
            classpath = buildClassPath( GraphDbInstance.class.getClassLoader(),
                    new String[] { "bootstrap", "classpath" }, runtime.getBootClassPath(), runtime.getClassPath() );
        }
        else
        {
            classpath = buildClassPath( GraphDbInstance.class.getClassLoader(), new String[] { "classpath" },
                    runtime.getClassPath() );
        }
        logger.logMessage( "Class Path:" );
        for ( String path : classpath )
        {
            logger.logMessage( "  " + path );
        }
        logPath( logger, "Library Path:", runtime.getLibraryPath() );
        for ( GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans() )
        {
            logger.logMessage( "Garbage Collector: " + gc.getName() + ": " + Arrays.toString( gc.getMemoryPoolNames() ) );
        }
        for ( MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans() )
        {
            MemoryUsage usage = pool.getUsage();
            logger.logMessage( String.format( "Memory Pool: %s (%s): committed=%s, used=%s, max=%s, threshold=%s", pool
                    .getName(), pool.getType(), usage == null ? "?" : bytes( usage.getCommitted() ),
                    usage == null ? "?" : bytes( usage.getUsed() ), usage == null ? "?" : bytes( usage.getMax() ), pool
                            .isUsageThresholdSupported() ? bytes( pool.getUsageThreshold() ) : "?" ) );
        }
        logger.logMessage( "VM Arguments: " + runtime.getInputArguments() );
        logger.logMessage( "System properties:" );
        for ( Object property : System.getProperties().keySet() )
        {
            if ( property instanceof String )
            {
                String key = (String) property;
                if ( key.startsWith( "java." ) || key.startsWith( "os." ) || key.endsWith( ".boot.class.path" )
                     || key.equals( "line.separator" ) ) continue;
                logger.logMessage( "  " + key + " = " + System.getProperty( key ) );
            }
        }
        logger.logMessage( "Neo4j Kernel properties:" );
        logConfiguration( params, logger, dumpToConsole );
        logger.logMessage( "Storage files:" );
        logStoreFiles( logger, "  ", new File( storeDir ) );
        ds.logIdUsage();
        logLinuxSchedulers( logger );
        logger.logMessage( "--- CONFIGURATION END ---" );
        logger.flush();
    }

    private static void logConfiguration( Map<Object, Object> params, StringLogger logger, boolean dumpToConsole )
    {
        for ( Object key : params.keySet() )
        {
            if ( key instanceof String )
            {
                Object value = params.get( key );
                String mess = key + "=" + value;
                if ( dumpToConsole )
                {
                    System.out.println( mess );
                }

                logger.logMessage( "  " + mess );
            }
        }
    }

    private static void logLinuxSchedulers( StringLogger logger )
    {
        File sysBlock = new File( "/sys/block" );
        if ( sysBlock.isDirectory() )
        {
            StringBuilder schedulers = new StringBuilder();
            for ( File subdir : sysBlock.listFiles( new java.io.FileFilter()
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
                        BufferedReader reader = new BufferedReader( new FileReader( scheduler ) );
                        try
                        {
                            for ( String line; null != ( line = reader.readLine() ); )
                                schedulers.append( "  " ).append( line ).append( '\n' );
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
            if ( schedulers.length() > 0 ) logger.logMessage( "Schedulers: " + schedulers );
        }
    }

    private static void logBeanBytesProperty( StringLogger logger, String message, Object bean, String type,
            String method )
    {
        Object value = getBeanProperty( bean, type, method, null );
        if ( value instanceof Number ) logger.logMessage( message + bytes( ( (Number) value ).longValue() ) );
    }

    private static void logBeanProperty( StringLogger logger, String message, Object bean, String type, String method )
    {
        Object value = getBeanProperty( bean, type, method, null );
        if ( value != null ) logger.logMessage( message + value );
    }

    private static Object getBeanProperty( Object bean, String type, String method, String defVal )
    {
        try
        {
            return Class.forName( type ).getMethod( method ).invoke( bean );
        }
        catch ( Exception e )
        {
            return defVal;
        }
        catch ( LinkageError e )
        {
            return defVal;
        }
    }

    private static Collection<String> buildClassPath( ClassLoader loader, String[] pathKeys, String... classPaths )
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

    private static String pathValue( Map<String, String> paths, String key, String path )
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


    private static long logStoreFiles( StringLogger logger, String prefix, File dir )
    {
        if ( !dir.isDirectory() ) return 0;
        long total = 0;
        for ( File file : dir.listFiles() )
        {
            long size;
            String filename = file.getName();
            if ( file.isDirectory() )
            {
                logger.logMessage( prefix + filename + ":" );
                size = logStoreFiles( logger, prefix + "  ", file );
                filename = "- Total";
            }
            else
            {
                size = file.length();
            }
            logger.logMessage( prefix + filename + ": " + bytes( size ) );
            total += size;
        }
        return total;
    }

    private static final String[] BYTE_SIZES = { "B", "kB", "MB", "GB" };

    private static String bytes( long bytes )
    {
        double size = bytes;
        for ( String suffix : BYTE_SIZES )
        {
            if ( size < 1024 ) return String.format( "%.2f %s", Double.valueOf( size ), suffix );
            size /= 1024;
        }
        return String.format( "%.2f TB", Double.valueOf( size ) );
    }

    private static void logPath( StringLogger logger, String title, String classpath )
    {
        logger.logMessage( title );
        for ( String path : classpath.split( File.pathSeparator ) )
        {
            logger.logMessage( "  " + canonicalize( path ) );
        }
    }

}
