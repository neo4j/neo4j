/*
 * Copyright (c) 2002-2009 "Neo Technology," Network Engine for Objects in Lund
 * AB [http://neotechnology.com] This file is part of Neo4j. Neo4j is free
 * software: you can redistribute it and/or modify it under the terms of the GNU
 * Affero General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version. This
 * program is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details. You should have received a copy of the GNU Affero General Public
 * License along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.onlinebackup;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.neo4j.kernel.EmbeddedGraphDatabase;

/**
 * Online backup implementation for neo4j.
 */
public class NeoBackup implements Backup
{
    private final EmbeddedGraphDatabase onlineNeo;
    private String destDir;
    private List<String> xaNames = null;
    private EmbeddedGraphDatabase backupNeo = null;
    private static Logger logger = Logger.getLogger( NeoBackup.class.getName() );
    private static ConsoleHandler consoleHandler = new ConsoleHandler();
    private static FileHandler fileHandler = null;
    private static final Level LOG_LEVEL_NORMAL = Level.INFO;
    private static final Level LOG_LEVEL_DEBUG = Level.ALL;
    private static final Level LOG_LEVEL_OFF = Level.OFF;
    static
    {
        logger.setUseParentHandlers( false );
        logger.setLevel( LOG_LEVEL_NORMAL );
        consoleHandler.setLevel( LOG_LEVEL_NORMAL );
        logger.addHandler( consoleHandler );
    }

    /**
     * Backup from a running EmbeddedNeo to a destination directory.
     * @param sourceNeo
     *            running database as backup source
     * @param destDir
     *            location of backup destination
     */
    public NeoBackup( final EmbeddedGraphDatabase sourceNeo,
        final String destDir )
    {
        if ( sourceNeo == null )
        {
            throw new IllegalArgumentException(
                "The EmbeddedNeo instance is null." );
        }
        if ( destDir == null )
        {
            throw new IllegalArgumentException( "Destination dir is null." );
        }
        if ( !new File( destDir ).exists() )
        {
            throw new RuntimeException(
                "Unable to locate local onlineNeo store in[" + destDir + "]" );
        }
        this.onlineNeo = sourceNeo;
        this.destDir = destDir;
    }

    /**
     * Backup from a running EmbeddedNeo to another running EmbeddedNeo.
     * @param sourceNeo
     *            running database as backup source
     * @param destNeo
     *            running database as backup destination
     */
    public NeoBackup( final EmbeddedGraphDatabase sourceNeo,
        final EmbeddedGraphDatabase destNeo )
    {
        if ( sourceNeo == null )
        {
            throw new IllegalArgumentException(
                "The online EmbeddedNeo instance is null." );
        }
        if ( destNeo == null )
        {
            throw new IllegalArgumentException(
                "The backup destination Neo instance is null." );
        }
        this.onlineNeo = sourceNeo;
        this.backupNeo = destNeo;
    }

    /**
     * Backup from a running EmbeddedNeo to a destination directory including
     * other data sources. NOTE: For now it assumes there is only a
     * LuceneIndexService running besides Neo4j. Common data source names are
     * "nioneodb" and "lucene".
     * @param sourceNeo
     *            running database as backup source
     * @param destDir
     *            location of backup destination
     * @param xaDataSourceNames
     *            names of data sources to backup
     */
    public NeoBackup( final EmbeddedGraphDatabase sourceNeo,
        final String destDir, final List<String> xaDataSourceNames )
    {
        this( sourceNeo, destDir );
        initXaNames( xaDataSourceNames );
    }

    /**
     * Backup from a running EmbeddedNeo to another running EmbeddedNeo
     * including other data sources. Common data source names are "nioneodb" and
     * "lucene".
     * @param sourceNeo
     *            running database as backup source
     * @param destNeo
     *            running database as backup destination
     * @param xaDataSourceNames
     *            names of data sources to backup
     */
    public NeoBackup( final EmbeddedGraphDatabase sourceNeo,
        final EmbeddedGraphDatabase destNeo,
        final List<String> xaDataSourceNames )
    {
        this( sourceNeo, destNeo );
        initXaNames( xaDataSourceNames );
    }

    /**
     * @param xaDataSourceNames
     */
    private final void initXaNames( final List<String> xaDataSourceNames )
    {
        if ( xaDataSourceNames == null )
        {
            throw new IllegalArgumentException( "xaDataSourceNames is null." );
        }
        if ( xaDataSourceNames.size() < 1 )
        {
            throw new IllegalArgumentException(
                "xaDataSourceNames list is empty." );
        }
        this.xaNames = xaDataSourceNames;
    }

    public void doBackup() throws IOException
    {
        logger.info( "Initializing NeoBackup." );
        NeoResource srcResource = new EmbeddedNeoResource( onlineNeo );
        if ( xaNames == null )
        {
            if ( backupNeo == null )
            {
                NeoResource dstResource = LocalNeoResource
                    .getInstance( destDir );
                runSimpleBackup( srcResource, dstResource );
                dstResource.close();
            }
            else
            {
                NeoResource dstResource = new EmbeddedNeoResource( backupNeo );
                runSimpleBackup( srcResource, dstResource );
            }
        }
        else
        {
            if ( backupNeo == null )
            {
                // TODO this is a temporary fix until we can restore services
                NeoResource dstResource = LocalNeoLuceneResource
                    .getInstance( destDir );
                runMultiBackup( srcResource, dstResource );
                dstResource.close();
            }
            else
            {
                NeoResource dstResource = new EmbeddedNeoResource( backupNeo );
                runMultiBackup( srcResource, dstResource );
            }
        }
    }

    /**
     * Backup only Neo data source.
     * @param srcResource
     *            backup source
     * @param dstResource
     *            backup destination
     * @throws IOException
     */
    private void runSimpleBackup( final NeoResource srcResource,
        final NeoResource dstResource ) throws IOException
    {
        NeoBackupTask task = new NeoBackupTask( srcResource.getDataSource(),
            dstResource.getDataSource() );
        task.prepare();
        task.run();
        logger.info( "Completed backup of [" + srcResource.getName()
            + "] data source." );
    }

    /**
     * Backup multiple data sources.
     * @param srcResource
     *            backup source
     * @param dstResource
     *            backup destination
     * @throws IOException
     */
    private void runMultiBackup( final NeoResource srcResource,
        final NeoResource dstResource ) throws IOException
    {
        List<NeoBackupTask> tasks = new ArrayList<NeoBackupTask>();
        logger.info( "Checking and preparing " + xaNames.toString()
            + " data sources." );
        for ( String xaName : xaNames )
        {
            // check source
            XaDataSourceResource srcDataSource = srcResource
                .getDataSource( xaName );
            if ( srcDataSource == null )
            {
                String message = "XaDataSource not found in backup source: ["
                    + xaName + "]";
                logger.severe( message );
                throw new RuntimeException( message );
            }
            else
            {
                // check destination
                XaDataSourceResource dstDataSource = dstResource
                    .getDataSource( xaName );
                if ( dstDataSource == null )
                {
                    String message = "XaDataSource not found in backup destination: ["
                        + xaName + "]";
                    logger.severe( message );
                    throw new RuntimeException( message );
                }
                else
                {
                    NeoBackupTask task = new NeoBackupTask( srcDataSource,
                        dstDataSource );
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
            for ( NeoBackupTask task : tasks )
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
    private class NeoBackupTask
    {
        private final XaDataSourceResource src;
        private final XaDataSourceResource dst;
        private long srcVersion = -1;
        private long dstVersion = -1;
        private final String resourceName;

        /**
         * Create a backup task.
         * @param src
         *            wrapped data source for source
         * @param dst
         *            wrapped data source for destination
         * @param resourceName
         *            name of data source
         */
        private NeoBackupTask( final XaDataSourceResource src,
            final XaDataSourceResource dst )
        {
            this.src = src;
            this.dst = dst;
            this.resourceName = src.getName();
        }

        /**
         * Rotate log and check versions.
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
                    + src.getIdentifier() + "] is not same as destination["
                    + dst.getCreationTime() + "," + dst.getIdentifier()
                    + "] for resource [" + resourceName + "]";
                logger.severe( message );
                throw new RuntimeException( message );
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
                throw new RuntimeException( message );
            }
            // rotate log, check versions
            src.rotateLog();
            srcVersion = src.getVersion();
            if ( srcVersion < dstVersion )
            {
                final String message = "Source srcVersion[" + srcVersion
                    + "] < destination srcVersion[" + dstVersion
                    + "] after rotate for resource [" + resourceName + "]";
                logger.severe( message );
                throw new RuntimeException( message );
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
                    throw new RuntimeException( message );
                }
            }
            // setup destination as slave
            dst.makeBackupSlave();
        }

        /**
         * Run the backup.
         * @throws IOException
         */
        public void run() throws IOException
        {
            if ( srcVersion == -1 || dstVersion == -1 )
            {
                final String message = "Backup can not start: source and/or destination "
                    + "could not be prepared for backup: ["
                    + resourceName
                    + "]";
                logger.severe( message );
                throw new RuntimeException( message );
            }
            logger.fine( "Backing up data source: [" + resourceName + "]" );
            for ( long i = dstVersion; i < srcVersion; i++ )
            {
                logger.fine( "Applying logical log [" + i + "] on ["
                    + resourceName + "]" );
                dst.applyLog( src.getLogicalLog( i ) );
            }
            logger.fine( "Source and destination have been synchronized. "
                + "Backup of data source complete [" + dstVersion + "->"
                + srcVersion + "] on [" + resourceName + "]." );
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
        if ( fileHandler == null )
        {
            // create appending file logger
            fileHandler = new FileHandler( "backup.log", true );
            fileHandler.setLevel( consoleHandler.getLevel() );
            fileHandler.setFormatter( new SimpleFormatter() );
            logger.addHandler( fileHandler );
        }
    }

    public void disableFileLogger()
    {
        if ( fileHandler != null )
        {
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
}