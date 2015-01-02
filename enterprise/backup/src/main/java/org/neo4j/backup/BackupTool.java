/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import ch.qos.logback.classic.LoggerContext;

import org.neo4j.backup.BackupService.BackupOutcome;
import org.neo4j.com.ComException;
import org.neo4j.consistency.ConsistencyCheckSettings;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.storemigration.LogFiles;
import org.neo4j.kernel.impl.storemigration.StoreFileType;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByConfigurationException;
import org.neo4j.kernel.impl.storemigration.legacystore.v20.StoreFile20;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.LogbackService;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.logging.SystemOutLogging;

import static org.slf4j.impl.StaticLoggerBinder.getSingleton;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class BackupTool
{
    private static final String TO = "to";
    private static final String HOST = "host";
    private static final String PORT = "port";

    @Deprecated // preferred -host and -port separately
    private static final String FROM = "from";

    private static final String VERIFY = "verify";

    private static final String CONFIG = "config";
    public static final String DEFAULT_SCHEME = "single";
    static final String MISMATCHED_STORE_ID = "You tried to perform a backup from database %s, " +
            "but the target directory contained a backup from database %s. ";

    static final String WRONG_FROM_ADDRESS_SYNTAX = "Please properly specify a location to backup in the" +
            " form " + dash( HOST ) + " <host> " + dash( PORT ) + " <port>";

    static final String UNKNOWN_SCHEMA_MESSAGE_PATTERN = "%s was specified as a backup module but it was not found. " +
            "Please make sure that the implementing service is on the classpath.";

    static final String NO_SOURCE_SPECIFIED = "Please specify " + dash( HOST ) + " and optionally " + dash( PORT ) +
            ", examples:\n" +
            "  " + dash( HOST ) + " 192.168.1.34\n" +
            "  " + dash( HOST ) + " 192.168.1.34 " + dash( PORT ) + " 1234";

    public static void main( String[] args )
    {
        BackupTool tool = new BackupTool( new BackupService( new DefaultFileSystemAbstraction() ), System.out );
        try
        {
            BackupOutcome backupOutcome = tool.run( args );

            if ( !backupOutcome.isConsistent() )
            {
                exitFailure( "WARNING: The database is inconsistent." );
            }
        }
        catch ( ToolFailureException e )
        {
            if ( e.getCause() != null )
            {
                e.getCause().printStackTrace( System.out );
            }

            exitFailure( e.getMessage() );
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

    BackupOutcome run( String[] args ) throws ToolFailureException
    {
        Args arguments = new Args( args );

        if ( !arguments.hasNonNull( TO ) )
        {
            throw new ToolFailureException( "Specify target location with " + dash( TO ) + " <target-directory>" );
        }

        if ( arguments.hasNonNull( FROM ) && !arguments.has( HOST ) && !arguments.has( PORT ) )
        {
            return runBackupWithLegacyArgs( arguments );
        }
        else if ( arguments.hasNonNull( HOST ) )
        {
            return runBackup( arguments );
        }
        else
        {
            throw new ToolFailureException( NO_SOURCE_SPECIFIED );
        }
    }

    private BackupOutcome runBackupWithLegacyArgs( Args args ) throws ToolFailureException
    {
        String from = args.get( FROM ).trim();
        String to = args.get( TO ).trim();
        boolean verify = args.getBoolean( VERIFY, true, true );
        Config tuningConfiguration = readTuningConfiguration( TO, args );

        URI backupURI = resolveBackupUri( from, args, tuningConfiguration );

        HostnamePort hostnamePort = newHostnamePort( backupURI );

        return executeBackup( hostnamePort, to, verify, tuningConfiguration );
    }

    private BackupOutcome runBackup( Args args ) throws ToolFailureException
    {
        String host = args.get( HOST ).trim();
        int port = args.getNumber( PORT, BackupServer.DEFAULT_PORT ).intValue();
        String to = args.get( TO ).trim();
        boolean verify = args.getBoolean( VERIFY, true, true );
        Config tuningConfiguration = readTuningConfiguration( TO, args );

        if ( host.contains( ":" ) )
        {
            if ( !host.startsWith( "[" ) )
            {
                host = "[" + host;
            }
            if ( !host.endsWith( "]" ) )
            {
                host += "]";
            }
        }

        URI backupURI = newURI( DEFAULT_SCHEME + "://" + host + ":" + port ); // a bit of validation

        HostnamePort hostnamePort = newHostnamePort( backupURI );

        return executeBackup( hostnamePort, to, verify, tuningConfiguration );
    }

    private BackupOutcome executeBackup( HostnamePort hostnamePort, String to,
                                         boolean verify, Config tuningConfiguration ) throws ToolFailureException
    {
        try
        {
            systemOut.println( "Performing backup from '" + hostnamePort + "'" );
            return doBackup( hostnamePort, to, verify, tuningConfiguration );
        }
        catch ( TransactionFailureException tfe )
        {
            if ( tfe.getCause() instanceof UpgradeNotAllowedByConfigurationException )
            {
                try
                {
                    systemOut.println( "The database present in the target directory is of an older version. " +
                            "Backing that up in target and performing a full backup from source" );
                    moveExistingDatabase( fs, to );

                }
                catch ( IOException e )
                {
                    throw new ToolFailureException( "There was a problem moving the old database out of the way" +
                            " - cannot continue, aborting.", e );
                }

                return doBackup( hostnamePort, to, verify, tuningConfiguration );
            }
            else
            {
                throw new ToolFailureException( "TransactionFailureException " +
                        "from existing backup at '" + hostnamePort + "'.", tfe );
            }
        }
    }

    private BackupOutcome doBackup( HostnamePort hostnamePort, String to,
                                    boolean checkConsistency, Config config ) throws ToolFailureException
    {
        try
        {
            String host = hostnamePort.getHost();
            int port = hostnamePort.getPort();
            BackupOutcome outcome =
                    backupService.doIncrementalBackupOrFallbackToFull( host, port, to, checkConsistency, config );
            systemOut.println( "Done" );
            return outcome;
        }
        catch ( MismatchingStoreIdException e )
        {
            systemOut.println( "Backup failed." );
            throw new ToolFailureException( String.format( MISMATCHED_STORE_ID, e.getExpected(), e.getEncountered() ) );
        }
        catch ( ComException e )
        {
            throw new ToolFailureException( "Couldn't connect to '" + hostnamePort + "'", e );
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
        BackupExtensionService service;
        try
        {
            service = Service.load( BackupExtensionService.class, providerName );
        }
        catch ( NoSuchElementException e )
        {
            throw new ToolFailureException( String.format( UNKNOWN_SCHEMA_MESSAGE_PATTERN, providerName ) );
        }

        try
        {
            return service.resolve( from, args, newLogging( config ) );
        }
        catch ( Throwable t )
        {
            throw new ToolFailureException( t.getMessage() );
        }
    }

    private static HostnamePort newHostnamePort( URI backupURI ) throws ToolFailureException
    {
        if ( backupURI == null || backupURI.getHost() == null )
        {
            throw new ToolFailureException( WRONG_FROM_ADDRESS_SYNTAX );
        }
        String host = backupURI.getHost();
        int port = backupURI.getPort();
        if ( port == -1 )
        {
            port = BackupServer.DEFAULT_PORT;
        }
        return new HostnamePort( host, port );
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

    private static void moveExistingDatabase( FileSystemAbstraction fs, String to ) throws IOException
    {
        File toDir = new File( to );
        File backupDir = new File( toDir, "old-version" );
        if ( !fs.mkdir( backupDir ) )
        {
            throw new IOException( "Trouble making target backup directory " + backupDir.getAbsolutePath() );
        }
        StoreFile20.move( fs, toDir, backupDir, StoreFile20.legacyStoreFiles(), false, false, StoreFileType.values() );
        LogFiles.move( fs, toDir, backupDir );
    }

    private static String dash( String name )
    {
        return "-" + name;
    }

    static void exitFailure( String msg )
    {
        System.out.println( msg );
        System.exit( 1 );
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
    }
}
