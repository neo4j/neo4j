/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import java.io.File;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.transaction.TransactionManager;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.impl.core.LockReleaser;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.nioneo.xa.NioNeoDbPersistenceSource;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.TxModule;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;

class GraphDbInstance
{
    private boolean started = false;
    private final boolean create;
    private String storeDir;

    GraphDbInstance( String storeDir, boolean create, Config config )
    {
        this.storeDir = storeDir;
        this.create = create;
        this.config = config;
    }

    private final Config config;

    private NioNeoDbPersistenceSource persistenceSource = null;

    public Config getConfig()
    {
        return config;
    }

    /**
     * Starts Neo4j with default configuration
     * @param graphDb The graph database service.
     *
     * @param storeDir path to directory where Neo4j store is located
     * @param create if true a new Neo4j store will be created if no store exist
     *            at <CODE>storeDir</CODE>
     * @param configuration parameters
     * @throws StartupFailedException if unable to start
     */
    public synchronized Map<Object, Object> start( GraphDatabaseService graphDb,
            KernelExtensionLoader kernelExtensionLoader )
    {
        if ( started )
        {
            throw new IllegalStateException( "Neo4j instance already started" );
        }
        Map<Object, Object> params = config.getParams();
        boolean useMemoryMapped = Boolean.parseBoolean( (String) config.getInputParams().get(
                Config.USE_MEMORY_MAPPED_BUFFERS ) );
        boolean dumpToConsole = Boolean.parseBoolean( (String) config.getInputParams().get(
                Config.DUMP_CONFIGURATION ) );
        storeDir = FileUtils.fixSeparatorsInPath( storeDir );
        StringLogger logger = StringLogger.getLogger( storeDir );
        AutoConfigurator autoConfigurator = new AutoConfigurator( storeDir, useMemoryMapped, dumpToConsole );
        autoConfigurator.configure( subset( config.getInputParams(), Config.USE_MEMORY_MAPPED_BUFFERS ) );
        // params.putAll( config.getInputParams() );

        String separator = System.getProperty( "file.separator" );
        String store = storeDir + separator + NeoStore.DEFAULT_NAME;
        params.put( "store_dir", storeDir );
        params.put( "neo_store", store );
        params.put( "create", String.valueOf( create ) );
        String logicalLog = storeDir + separator + NeoStoreXaDataSource.LOGICAL_LOG_DEFAULT_NAME;
        params.put( "logical_log", logicalLog );
        params.put( LockManager.class, config.getLockManager() );
        params.put( LockReleaser.class, config.getLockReleaser() );

        kernelExtensionLoader.configureKernelExtensions();

        config.getTxModule().registerDataSource( Config.DEFAULT_DATA_SOURCE_NAME,
                Config.NIO_NEO_DB_CLASS, NeoStoreXaDataSource.BRANCH_ID, params );
        // hack for lucene index recovery if in path
        if ( !config.isReadOnly() || config.isBackupSlave() )
        {
            try
            {
                Class clazz = Class.forName( Config.LUCENE_DS_CLASS );
                cleanWriteLocksInLuceneDirectory( storeDir + File.separator + "lucene" );
                byte luceneId[] = UTF8.encode( "162373" );
                registerLuceneDataSource( "lucene", clazz.getName(),
                        config.getTxModule(), storeDir + File.separator + "lucene",
                        config.getLockManager(), luceneId, params );
            }
            catch ( ClassNotFoundException e )
            { // ok index util not on class path
            }
            catch ( NoClassDefFoundError err )
            { // ok index util not on class path
            }

            try
            {
                Class clazz = Class.forName( Config.LUCENE_FULLTEXT_DS_CLASS );
                cleanWriteLocksInLuceneDirectory( storeDir + File.separator + "lucene-fulltext" );
                byte[] luceneId = UTF8.encode( "262374" );
                registerLuceneDataSource( "lucene-fulltext",
                        clazz.getName(), config.getTxModule(),
                        storeDir + File.separator + "lucene-fulltext", config.getLockManager(),
                        luceneId, params );
            }
            catch ( ClassNotFoundException e )
            { // ok index util not on class path
            }
            catch ( NoClassDefFoundError err )
            { // ok index util not on class path
            }
        }
        persistenceSource = new NioNeoDbPersistenceSource();
        config.setPersistenceSource( Config.DEFAULT_DATA_SOURCE_NAME, create );
        config.getIdGeneratorModule().setPersistenceSourceInstance(
                persistenceSource );
        config.getTxModule().init();
        config.getPersistenceModule().init();
        persistenceSource.init();
        config.getIdGeneratorModule().init();
        config.getGraphDbModule().init();

        kernelExtensionLoader.initializeIndexProviders();

        config.getTxModule().start();
        config.getPersistenceModule().start( config.getTxModule().getTxManager(), persistenceSource,
                config.getSyncHookFactory(), config.getLockReleaser() );
        persistenceSource.start( config.getTxModule().getXaDataSourceManager() );
        config.getIdGeneratorModule().start();
        config.getGraphDbModule().start( config.getLockReleaser(),
                config.getPersistenceModule().getPersistenceManager(),
                config.getRelationshipTypeCreator(), params );

        logConfig( params, graphDb.getClass(), storeDir, dumpToConsole, logger, autoConfigurator );
        started = true;
        return Collections.unmodifiableMap( params );
    }

    private static void logConfig( Map<Object, Object> params, Class<? extends GraphDatabaseService> graphDb,
            String storeDir, boolean dumpToConsole, StringLogger logger, AutoConfigurator autoConfigurator )
    {
        logger.logMessage( "--- CONFIGURATION START ---" );
        logger.logMessage( "Graph Database: " + graphDb.getName() );
        logger.logMessage( autoConfigurator.getNiceMemoryInformation() );
        logger.logMessage( "Kernel version: " + Version.getKernel() );
        logger.logMessage( "Neo4j component versions:" );
        for ( Version componentVersion : Service.load( Version.class ) )
        {
            logger.logMessage( "  " + componentVersion );
        }
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        logger.logMessage( String.format( "Operating System: %s; version: %s; arch: %s; cpus: %s", os.getName(),
                os.getVersion(), os.getArch(), Integer.valueOf( os.getAvailableProcessors() ) ) );
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
        logClassPath( logger, "Library Path:", runtime.getLibraryPath() );
        for ( GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans() )
        {
            logger.logMessage( "Garbage Collector: " + gc.getName() + ": " + Arrays.toString( gc.getMemoryPoolNames() ) );
        }
        for ( MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans() )
        {
            MemoryUsage usage = pool.getUsage();
            logger.logMessage( String.format( "Memory Pool: %s (%s): committed=%s, used=%s, max=%s, threshold=%s",
                    pool.getName(), pool.getType(), usage == null ? "?" : bytes( usage.getCommitted() ),
                    usage == null ? "?" : bytes( usage.getUsed() ), usage == null ? "?" : bytes( usage.getMax() ),
                    pool.isUsageThresholdSupported() ? bytes( pool.getUsageThreshold() ) : "?" ) );
        }
        logger.logMessage( "VM Arguments: " + runtime.getInputArguments() );
        logger.logMessage( "System properties:" );
        for ( Object property : System.getProperties().keySet() )
        {
            if ( property instanceof String )
            {
                String key = (String) property;
                if ( key.startsWith( "java." ) || key.startsWith( "os." ) || key.endsWith( ".boot.class.path" )
                     || key.equals( "line.separator" ) )
                    continue;
                logger.logMessage( "  " + key + " = " + System.getProperty( key ) );
            }
        }
        logger.logMessage( "Neo4j Kernel properties:" );
        logConfiguration( params, logger, dumpToConsole );
        logger.logMessage( "Storage files:" );
        logStoreFiles( logger, "  ", new File( storeDir ) );
        logger.logMessage( "--- CONFIGURATION END ---" );
        logger.flush();
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

    private static void logClassLoader( StringLogger logger, ClassLoader loader )
    {
        if ( loader == null ) return;
        if ( loader instanceof URLClassLoader )
        {
            URLClassLoader urlLoader = (URLClassLoader) loader;
            for ( URL url : urlLoader.getURLs() )
            {
                logger.logMessage( "  " + url );
            }
        }
        logClassLoader( logger, loader.getParent() );
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

    private static void logClassPath( StringLogger logger, String title, String classpath )
    {
        logger.logMessage( title );
        for ( String path : classpath.split( File.pathSeparator ) )
        {
            logger.logMessage( "  " + path );
        }
    }

    private static Map<Object, Object> subset( Map<Object, Object> source, String... keys )
    {
        Map<Object, Object> result = new HashMap<Object, Object>();
        for ( String key : keys )
        {
            if ( source.containsKey( key ) )
            {
                result.put( key, source.get( key ) );
            }
        }
        return result;
    }

    private static void logConfiguration( Map<Object, Object> params, StringLogger logger, boolean dumpToConsole )
    {
        for( Object key : params.keySet())
        {
            if (key instanceof String)
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

    private void cleanWriteLocksInLuceneDirectory( String luceneDir )
    {
        File dir = new File( luceneDir );
        if ( !dir.isDirectory() )
        {
            return;
        }
        for ( File file : dir.listFiles() )
        {
            if ( file.isDirectory() )
            {
                cleanWriteLocksInLuceneDirectory( file.getAbsolutePath() );
            }
            else if ( file.getName().equals( "write.lock" ) )
            {
                boolean success = file.delete();
                assert success;
            }
        }
    }

    private XaDataSource registerLuceneDataSource( String name,
            String className, TxModule txModule, String luceneDirectory,
            LockManager lockManager, byte[] resourceId,
            Map<Object,Object> params )
    {
        params.put( "dir", luceneDirectory );
        params.put( LockManager.class, lockManager );
        return txModule.registerDataSource( name, className, resourceId,
                params, true );
    }

    /**
     * Returns true if Neo4j is started.
     *
     * @return True if Neo4j started
     */
    public boolean started()
    {
        return started;
    }

    /**
     * Shut down Neo4j.
     */
    public synchronized void shutdown()
    {
        if ( started )
        {
            config.getGraphDbModule().stop();
            config.getIdGeneratorModule().stop();
            persistenceSource.stop();
            config.getPersistenceModule().stop();
            config.getTxModule().stop();
            config.getGraphDbModule().destroy();
            config.getIdGeneratorModule().destroy();
            persistenceSource.destroy();
            config.getPersistenceModule().destroy();
            config.getTxModule().destroy();
        }
        started = false;
    }

    public Iterable<RelationshipType> getRelationshipTypes()
    {
        return config.getGraphDbModule().getRelationshipTypes();
    }

    public boolean transactionRunning()
    {
        try
        {
            return config.getTxModule().getTxManager().getTransaction() != null;
        }
        catch ( Exception e )
        {
            throw new TransactionFailureException(
                    "Unable to get transaction.", e );
        }
    }

    public TransactionManager getTransactionManager()
    {
        return config.getTxModule().getTxManager();
    }
}
