/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.backup;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.NoSuchElementException;

import org.neo4j.com.ComException;
import org.neo4j.consistency.ConsistencyCheckSettings;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.storemigration.LogFiles;
import org.neo4j.kernel.impl.storemigration.StoreFile20;
import org.neo4j.kernel.impl.storemigration.StoreFileType;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByConfigurationException;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.LogbackService;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.logging.SystemOutLogging;

import ch.qos.logback.classic.LoggerContext;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.slf4j.impl.StaticLoggerBinder.getSingleton;

public class BackupTool
{
    private static final String TO = "to";
    private static final String FROM = "from";
    private static final String VERIFY = "verify";
    private static final String CONFIG = "config";
    public static final String DEFAULT_SCHEME = "single";

    static final String MISMATCHED_STORE_ID = "You tried to perform a backup from database %s, " +
            "but the target directory contained a backup from database %s. ";

    static final String WRONG_FROM_ADDRESS_SYNTAX = "Please properly specify a location to backup in the" +
            " form <host>[:<port>]{,<host>[:<port>]}...";

    static final String UNKNOWN_SCHEMA_MESSAGE_PATTERN = "%s was specified as a backup module but it was not found. " +
            "Please make sure that the implementing service is on the classpath.";

    public static void main( String[] args )
    {
        BackupTool tool = new BackupTool( new BackupService( new DefaultFileSystemAbstraction() ), System.out );
        try
        {
            tool.run( args );
        }
        catch ( ToolFailureException e )
        {
            e.haltJVM();
        }
    }

    private final BackupService backupService;
    private final PrintStream systemOut;
    private final FileSystemAbstraction fs;

    BackupTool( BackupService backupService, PrintStream systemOut )
    {
        this.backupService = backupService;
        this.systemOut = systemOut;
        this.fs = new DefaultFileSystemAbstraction();
    }

    void run( String[] args ) throws ToolFailureException
    {
        Args arguments = new Args( args );

        checkArguments( arguments );

        String from = arguments.get( FROM, null );
        String to = arguments.get( TO, null );
        boolean verify = arguments.getBoolean( VERIFY, true, true );
        Config tuningConfiguration = readTuningConfiguration( TO, arguments );

        URI backupURI = resolveBackupUri( from.trim(), arguments, tuningConfiguration );
        if ( backupURI == null || backupURI.getHost() == null )
        {
            throw new ToolFailureException( WRONG_FROM_ADDRESS_SYNTAX );
        }

        try
        {
            String str = backupURI.toASCIIString();
            if ( str.contains( "://" ) )
            {
                str = str.split( "://" )[1];
            }
            systemOut.println( "Performing backup from '" + str + "'" );
            doBackup( backupURI, to, verify, tuningConfiguration );
        }
        catch ( TransactionFailureException e )
        {
            if ( e.getCause() instanceof UpgradeNotAllowedByConfigurationException )
            {
                try
                {
                    systemOut.println( "The database present in the target directory is of an older version. " +
                            "Backing that up in target and performing a full backup from source" );
                    moveExistingDatabase( fs, to );

                }
                catch ( IOException e1 )
                {
                    throw new ToolFailureException( "There was a problem moving the old database out of the way" +
                            " - cannot continue, aborting.", e );
                }

                doBackup( backupURI, to, verify, tuningConfiguration );
            }
            else
            {
                throw new ToolFailureException( "TransactionFailureException " +
                        "from existing backup at '" + from + "'.", e );
            }
        }
    }

    private static void checkArguments( Args arguments ) throws ToolFailureException
    {
        if ( arguments.get( FROM, null ) == null )
        {
            throw new ToolFailureException( "Please specify " + dash( FROM ) + ", examples:\n" +
                    "  " + dash( FROM ) + " 192.168.1.34\n" +
                    "  " + dash( FROM ) + " 192.168.1.34:1234\n" +
                    "  " + dash( FROM ) + " 192.168.1.15:2181,192.168.1.16:2181" );
        }

        if ( arguments.get( TO, null ) == null )
        {
            throw new ToolFailureException( "Specify target location with " + dash( TO )
                    + " <target-directory>" );
        }
    }

    private static Config readTuningConfiguration( String storeDir, Args arguments ) throws ToolFailureException
    {
        Map<String, String> specifiedProperties = stringMap();

        String propertyFilePath = arguments.get( CONFIG, null );
        if ( propertyFilePath != null )
        {
            File propertyFile = new File( propertyFilePath );
            try
            {
                specifiedProperties = MapUtil.load( propertyFile );
            }
            catch ( IOException e )
            {
                throw new ToolFailureException( String.format( "Could not read configuration properties file [%s]",
                        propertyFilePath ), e );
            }
        }
        specifiedProperties.put( GraphDatabaseSettings.store_dir.name(), storeDir );
        return new Config( specifiedProperties, GraphDatabaseSettings.class, ConsistencyCheckSettings.class );
    }

    private static URI resolveBackupUri( String from, Args arguments, Config config ) throws ToolFailureException
    {
        if ( from.contains( "," ) )
        {
            if ( !from.startsWith( "ha://" ) )
            {
                checkNoSchemaIsPresent( from );
                from = "ha://" + from;
            }
            return resolveUriWithProvider( "ha", from, arguments, config );
        }
        if ( !from.startsWith( "single://" ) )
        {
            from = from.replace( "ha://", "" );
            checkNoSchemaIsPresent( from );
            from = "single://" + from;
        }
        return newURI( from );
    }

    private static void checkNoSchemaIsPresent( String address ) throws ToolFailureException
    {
        if ( address.contains( "://" ) )
        {
            throw new ToolFailureException( WRONG_FROM_ADDRESS_SYNTAX );
        }
    }

    private static URI newURI( String uriString ) throws ToolFailureException
    {
        try
        {
            return new URI( uriString );
        }
        catch ( URISyntaxException e )
        {
            throw new ToolFailureException( WRONG_FROM_ADDRESS_SYNTAX );
        }
    }

    private static URI resolveUriWithProvider( String providerName, String from, Args args, Config config )
            throws ToolFailureException
    {
        URI uri = newURI( from );

        BackupExtensionService service;
        try
        {
            service = Service.load( BackupExtensionService.class, providerName );
        }
        catch ( NoSuchElementException e1 )
        {
            throw new ToolFailureException( String.format( UNKNOWN_SCHEMA_MESSAGE_PATTERN, providerName ) );
        }

        try
        {
            return service.resolve( uri, args, newLogging( config ) );
        }
        catch ( Throwable t )
        {
            throw new ToolFailureException( t.getMessage() );
        }
    }

    private static Logging newLogging( Config config )
    {
        Logging logging;
        try
        {
            BackupTool.class.getClassLoader().loadClass( "ch.qos.logback.classic.LoggerContext" );
            LifeSupport life = new LifeSupport();
            LogbackService logbackService = life.add(
                    new LogbackService(
                            config,
                            (LoggerContext) getSingleton().getLoggerFactory(), "neo4j-backup-logback.xml" ) );
            life.start();
            logging = logbackService;
        }
        catch ( Throwable e )
        {
            logging = new SystemOutLogging();
        }
        return logging;
    }

    private void doBackup( URI from, String to, boolean checkConsistency, Config config ) throws ToolFailureException
    {
        try
        {
            backupService.doIncrementalBackupOrFallbackToFull( from.getHost(), extractPort( from ), to,
                    checkConsistency, config );
            systemOut.println( "Done" );
        }
        catch ( MismatchingStoreIdException e )
        {
            systemOut.println( "Backup failed." );
            throw new ToolFailureException( String.format( MISMATCHED_STORE_ID, e.getExpected(), e.getEncountered() ) );
        }
        catch ( ComException e )
        {
            throw new ToolFailureException( "Couldn't connect to '" + from + "'", e );
        }
    }

    private static int extractPort( URI from )
    {
        int port = from.getPort();
        if ( port == -1 )
        {
            port = BackupServer.DEFAULT_PORT;
        }
        return port;
    }

    private static void moveExistingDatabase( FileSystemAbstraction fs, String to ) throws IOException
    {
        File toDir = new File( to );
        File backupDir = new File( toDir, "old-version" );
        if ( !fs.mkdir( backupDir ) )
        {
            throw new IOException( "Trouble making target backup directory "
                    + backupDir.getAbsolutePath() );
        }
        StoreFile20.move( fs, toDir, backupDir, StoreFile20.legacyStoreFiles(), false, false, StoreFileType.values() );
        LogFiles.move( fs, toDir, backupDir );
    }

    static class ToolFailureException extends Exception
    {
        ToolFailureException( String message )
        {
            super( message );
        }

        ToolFailureException( String message, Throwable cause )
        {
            super( message, cause );
        }

        void haltJVM()
        {
            System.out.println( getMessage() );
            if ( getCause() != null )
            {
                getCause().printStackTrace( System.out );
            }
            System.exit( 1 );
        }
    }

    private static String dash( String name )
    {
        return "-" + name;
    }
}
