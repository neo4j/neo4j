/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.onlinebackup;

import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.onlinebackup.impl.AbstractGraphDatabaseResource;
import org.neo4j.onlinebackup.impl.LocalGraphDatabaseResource;
import org.neo4j.onlinebackup.impl.Neo4jResource;
import org.neo4j.onlinebackup.impl.XaDataSourceResource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.*;

/**
 * Online backup implementation for Neo4j.
 */
public class Neo4jBackup implements Backup
{
    private static final String DEFAULT_BACKUP_LOG_LOCATION = "backup.log";
    private static final Level LOG_LEVEL_NORMAL = Level.INFO;
    private static final Level LOG_LEVEL_DEBUG = Level.ALL;
    private static final Level LOG_LEVEL_OFF = Level.OFF;

    private static Logger logger = Logger.getLogger( Neo4jBackup.class.getName() );
    private static ConsoleHandler consoleHandler = new ConsoleHandler();
    private static FileHandler fileHandler = null;
    static
    {
        logger.setUseParentHandlers( false );
        logger.setLevel( LOG_LEVEL_NORMAL );
        consoleHandler.setLevel( LOG_LEVEL_NORMAL );
        logger.addHandler( consoleHandler );
    }

    private final AbstractGraphDatabase onlineGraphDb;
    private final ResourceFetcher destinationResourceFetcher;
    private final List<String> xaNames;

    /**
     * Backup from a running {@link AbstractGraphDatabase} to a destination
     * directory. Only the data source representing the Neo4j database will be
     * used and if it isn't set to keep logical logs an
     * {@link IllegalStateException} will be thrown.
     * 
     * @param source running database as backup source
     * @param destinationDir location of backup destination
     * @return instance for creating backup
     */
    public static Backup neo4jDataSource( AbstractGraphDatabase source,
            String destinationDir )
    {
        return new Neo4jBackup( source, new DestinationDirResourceFetcher( destinationDir ),
                Collections.singletonList( Config.DEFAULT_DATA_SOURCE_NAME ) );
    }

    /**
     * Backup from a running {@link AbstractGraphDatabase} to another running
     * {@link AbstractGraphDatabase}. Only the data source representing the
     * Neo4j database will be used and if it isn't set to keep logical logs an
     * {@link IllegalStateException} will be thrown.
     * 
     * @param source running database as backup source
     * @param destination running database as backup destination
     * @return instance for creating backup
     */
    public static Backup neo4jDataSource( AbstractGraphDatabase source,
            AbstractGraphDatabase destination )
    {
        return new Neo4jBackup( source, new GraphDbResourceFetcher( destination ),
                Collections.singletonList( Config.DEFAULT_DATA_SOURCE_NAME ) );
    }

    /**
     * Backup from a running {@link AbstractGraphDatabase} to a destination
     * directory. All registered XA data sources will be used and all those data
     * sources will have to be set to keep their logical logs, otherwise an
     * {@link IllegalStateException} will be thrown.
     * 
     * @param source running database as backup source
     * @param destinationDir location of backup destination
     * @return instance for creating backup
     */
    public static Backup allDataSources( AbstractGraphDatabase source,
            String destinationDir )
    {
        return new Neo4jBackup( source, new DestinationDirResourceFetcher( destinationDir ),
                allDataSources( source ) );
    }

    /**
     * Backup from a running {@link AbstractGraphDatabase} to another running
     * {@link AbstractGraphDatabase}. All registered XA data sources will be
     * used and all those data sources will have to be set to keep their logical
     * logs, otherwise an {@link IllegalStateException} will be thrown.
     * 
     * @param source running database as backup source
     * @param destination running database as backup destination
     * @return instance for creating backup
     */
    public static Backup allDataSources( AbstractGraphDatabase source,
            AbstractGraphDatabase destination )
    {
        return new Neo4jBackup( source, new GraphDbResourceFetcher( destination ),
                allDataSources( source ) );
    }

    /**
     * Backup from a running {@link AbstractGraphDatabase} to a destination
     * directory. Which XA data sources to include in the backup can here be
     * explicitly specified. This is considered to be more of an "expert-mode".
     * If any of the specified data sources isn't set to keep its logical logs
     * an {@link IllegalStateException} will be thrown.
     * 
     * @param source running database as backup source
     * @param destinationDir location of backup destination
     * @param xaDataSourceNames names of data sources to backup
     * @return instance for creating backup
     */
    public static Backup customDataSources( AbstractGraphDatabase source,
            String destinationDir, String... xaDataSourceNames )
    {
        return new Neo4jBackup( source, new DestinationDirResourceFetcher(
                destinationDir ), new ArrayList<String>(
                Arrays.asList( xaDataSourceNames ) ) );
    }

    /**
     * Backup from a running {@link AbstractGraphDatabase} to another running
     * {@link AbstractGraphDatabase}. Which XA data sources to include in the
     * backup can here be explicitly specified. This is considered to be more of
     * an "expert-mode". If any of the specified data sources isn't set to keep
     * its logical logs an {@link IllegalStateException} will be thrown.
     * 
     * @param source running database as backup source
     * @param destination running database as backup destination
     * @param xaDataSourceNames names of data sources to backup
     * @return instance for creating backup
     */
    public static Backup customDataSources( AbstractGraphDatabase source,
            AbstractGraphDatabase destination, String... xaDataSourceNames )
    {
        return new Neo4jBackup( source, new GraphDbResourceFetcher( destination ),
                new ArrayList<String>( new ArrayList<String>(
                        Arrays.asList( xaDataSourceNames ) ) ) );
    }
    
    private Neo4jBackup( AbstractGraphDatabase source,
            ResourceFetcher destination, List<String> xaDataSources )
    {
        if ( source == null )
        {
            throw new IllegalArgumentException( "The source graph db instance is null." );
        }
        if ( xaDataSources == null )
        {
            throw new IllegalArgumentException( "XA data source name list is null" );
        }
        this.onlineGraphDb = source;
        this.destinationResourceFetcher = destination;
        this.xaNames = xaDataSources;
        assertLogicalLogsAreKept();
    }
    
    private static List<String> allDataSources( AbstractGraphDatabase db )
    {
        List<String> result = new ArrayList<String>();
        for ( XaDataSource dataSource : db.getConfig().getTxModule()
                .getXaDataSourceManager().getAllRegisteredDataSources() )
        {
            result.add( dataSource.getName() );
        }
        return result;
    }

    /**
     * Check if logical logs are kept for a data source.
     * @throws IllegalStateException if logical logs are not kept
     */
    private void assertLogicalLogsAreKept()
    {
        if ( xaNames.size() < 1 )
        {
            throw new IllegalArgumentException( "No XA data source names in list" );
        }

        XaDataSourceManager xaDataSourceManager =
            onlineGraphDb.getConfig().getTxModule().getXaDataSourceManager();

        for ( String xaDataSourceName : xaNames )
        {
            XaDataSource xaDataSource = xaDataSourceManager.getXaDataSource( xaDataSourceName );
            if ( !xaDataSource.isLogicalLogKept() )
            {
                throw new IllegalStateException( "Backup cannot be run, as the data source ["
                        + xaDataSourceName + "," + xaDataSource + "] is not configured to keep logical logs." );
            }
        }
    }

    public void doBackup() throws IOException
    {
        logger.info( "Initializing backup." );
        Neo4jResource srcResource = new AbstractGraphDatabaseResource( onlineGraphDb );
        Neo4jResource dstResource = this.destinationResourceFetcher.fetch();
        if ( xaNames.size() == 1 )
        {
            runSimpleBackup( srcResource, dstResource );
        }
        else
        {
            runMultiBackup( srcResource, dstResource );
        }
        this.destinationResourceFetcher.close( dstResource );
    }

    /**
     * Backup Neo4j data source only.
     * 
     * @param srcResource backup source
     * @param dstResource backup destination
     * @throws IOException
     */
    private void runSimpleBackup( final Neo4jResource srcResource,
            final Neo4jResource dstResource ) throws IOException
    {
        Neo4jBackupTask task = new Neo4jBackupTask(
                srcResource.getDataSource(), dstResource.getDataSource() );
        task.prepare();
        task.run();
        logger.info( "Completed backup of [" + srcResource.getName() + "] data source." );
    }

    /**
     * Backup multiple data sources.
     * 
     * @param srcResource backup source
     * @param dstResource backup destination
     * @throws IOException
     */
    private void runMultiBackup( final Neo4jResource srcResource,
            final Neo4jResource dstResource ) throws IOException
    {
        List<Neo4jBackupTask> tasks = new ArrayList<Neo4jBackupTask>();
        logger.info( "Checking and preparing " + xaNames + " data sources." );
        for ( String xaName : xaNames )
        {
            // check source
            XaDataSourceResource srcDataSource = srcResource.getDataSource( xaName );
            if ( srcDataSource == null )
            {
                String message = "XaDataSource not found in backup source: [" + xaName + "]";
                logger.severe( message );
                throw new RuntimeException( message );
            }
            else
            {
                // check destination
                XaDataSourceResource dstDataSource = dstResource.getDataSource( xaName );
                if ( dstDataSource == null )
                {
                    String message = "XaDataSource not found in backup destination: ["
                                     + xaName + "]";
                    logger.severe( message );
                    throw new RuntimeException( message );
                }
                else
                {
                    Neo4jBackupTask task = new Neo4jBackupTask( srcDataSource, dstDataSource );
                    task.prepare();
                    tasks.add( task );
                }
            }
        }
        if ( tasks.size() == 0 )
        {
            String message = "No data sources to backup were found.";
            logger.severe( message );
            throw new RuntimeException( message );
        }
        else
        {
            for ( Neo4jBackupTask task : tasks )
            {
                task.run();
            }
            logger.info( "Completed backup of " + tasks + " data sources." );
        }
    }

    /**
     * Class to handle backup tasks. It separates preparing and running the
     * backup.
     */
    private class Neo4jBackupTask
    {
        private final XaDataSourceResource src;
        private final XaDataSourceResource dst;
        private long srcVersion = -1;
        private long dstVersion = -1;
        private final String resourceName;

        /**
         * Create a backup task.
         * 
         * @param src wrapped data source for source
         * @param dst wrapped data source for destination
         */
        private Neo4jBackupTask( final XaDataSourceResource src,
                final XaDataSourceResource dst )
        {
            this.src = src;
            this.dst = dst;
            this.resourceName = src.getName();
        }

        /**
         * Rotate log and check versions.
         * 
         * @throws IOException
         */
        public void prepare() throws IOException
        {
            logger.fine( "Checking and preparing data source: [" + resourceName
                         + "]" );
            // check store identities
            if ( src.getCreationTime() != dst.getCreationTime()
                 && src.getIdentifier() != dst.getIdentifier() )
            {
                String message = "Source[" + src.getCreationTime() + ","
                                 + src.getIdentifier()
                                 + "] is not same as destination["
                                 + dst.getCreationTime() + ","
                                 + dst.getIdentifier() + "] for resource ["
                                 + resourceName + "]";
                logger.severe( message );
                throw new IllegalStateException( message );
            }
            // check versions
            srcVersion = src.getVersion();
            dstVersion = dst.getVersion();
            if ( srcVersion < dstVersion )
            {
                String message = "Source srcVersion[" + srcVersion
                                 + "] < destination srcVersion[" + dstVersion
                                 + "] for resource [" + resourceName + "]";
                logger.severe( message );
                throw new IllegalStateException( message );
            }
            // rotate log, check versions
            src.rotateLog();
            srcVersion = src.getVersion();
            if ( srcVersion < dstVersion )
            {
                final String message = "Source srcVersion[" + srcVersion
                                       + "] < destination srcVersion["
                                       + dstVersion
                                       + "] after rotate for resource ["
                                       + resourceName + "]";
                logger.severe( message );
                throw new IllegalStateException( message );
            }
            // check that log entries exist
            for ( long i = dstVersion; i < srcVersion; i++ )
            {
                if ( !src.hasLogicalLog( i ) )
                {
                    String message = "Missing log entry in backup source: ["
                                     + i + "] in resource [" + resourceName
                                     + "]. Can not perform backup.";
                    logger.severe( message );
                    throw new IllegalStateException( message );
                }
            }
            // setup destination as slave
            dst.makeBackupSlave();
        }

        /**
         * Run the backup.
         * 
         * @throws IOException
         */
        public void run() throws IOException
        {
            if ( srcVersion == -1 || dstVersion == -1 )
            {
                final String message = "Backup can not start: source and/or destination "
                                       + "could not be prepared for backup: ["
                                       + resourceName + "]";
                logger.severe( message );
                throw new IllegalStateException( message );
            }
            logger.fine( "Backing up data source: [" + resourceName + "]" );
            for ( long i = dstVersion; i < srcVersion; i++ )
            {
                logger.fine( "Applying logical log [" + i + "] on ["
                             + resourceName + "]" );
                dst.applyLog( src.getLogicalLog( i ) );
            }
            logger.fine( "Source and destination have been synchronized. "
                         + "Backup of data source complete [" + dstVersion
                         + "->" + srcVersion + "] on [" + resourceName + "]." );
        }

        /**
         * Returns the resource name for this task.
         */
        @Override
        public String toString()
        {
            return resourceName;
        }
    }

    public void enableFileLogger() throws SecurityException, IOException
    {
        enableFileLogger( DEFAULT_BACKUP_LOG_LOCATION );
    }

    public void enableFileLogger( String filename ) throws SecurityException,
            IOException
    {
        if ( filename == null )
        {
            throw new IllegalArgumentException( "Given filename is null." );
        }
        disableFileLogger();
        setFileHandler( new FileHandler( filename, true ) );
    }

    public void enableFileLogger( FileHandler handler )
    {
        if ( handler == null )
        {
            throw new IllegalArgumentException( "Given FileHandler is null." );
        }
        disableFileLogger();
        setFileHandler( handler );
    }

    private void setFileHandler( FileHandler handler )
    {
        if ( fileHandler != null )
        {
            throw new IllegalStateException( "File handler already exists." );
        }
        fileHandler = handler;
        fileHandler.setLevel( consoleHandler.getLevel() );
        fileHandler.setFormatter( new SimpleFormatter() );
        logger.addHandler( fileHandler );
    }

    public void disableFileLogger()
    {
        if ( fileHandler != null )
        {
            fileHandler.flush();
            fileHandler.close();
            logger.removeHandler( fileHandler );
            fileHandler = null;
        }
    }

    public void setLogLevelDebug()
    {
        logger.setLevel( LOG_LEVEL_DEBUG );
        consoleHandler.setLevel( LOG_LEVEL_DEBUG );
        if ( fileHandler != null )
        {
            fileHandler.setLevel( LOG_LEVEL_DEBUG );
        }
    }

    public void setLogLevelNormal()
    {
        logger.setLevel( LOG_LEVEL_NORMAL );
        consoleHandler.setLevel( LOG_LEVEL_NORMAL );
        if ( fileHandler != null )
        {
            fileHandler.setLevel( LOG_LEVEL_NORMAL );
        }
    }

    public void setLogLevelOff()
    {
        logger.setLevel( LOG_LEVEL_OFF );
    }
    
    private static abstract class ResourceFetcher
    {
        abstract Neo4jResource fetch();
        abstract void close( Neo4jResource resource );
    }
    
    private static class GraphDbResourceFetcher extends ResourceFetcher
    {
        private final AbstractGraphDatabase db;

        GraphDbResourceFetcher( AbstractGraphDatabase db )
        {
            if ( db == null )
            {
                throw new IllegalArgumentException( "Destination graph database is null" );
            }
            this.db = db;
        }
        
        @Override
        void close( Neo4jResource resource )
        {
            // Do nothing
        }

        @Override
        Neo4jResource fetch()
        {
            return new AbstractGraphDatabaseResource( db );
        }
    }
    
    private static class DestinationDirResourceFetcher extends ResourceFetcher
    {
        private final String destDir;

        DestinationDirResourceFetcher( String destDir )
        {
            if ( destDir == null )
            {
                throw new IllegalArgumentException( "Destination dir is null" );
            }
            this.destDir = destDir;
        }
        
        @Override
        void close( Neo4jResource resource )
        {
            resource.close();
        }

        @Override
        Neo4jResource fetch()
        {
            return LocalGraphDatabaseResource.getInstance( destDir );
        }
    }
}
