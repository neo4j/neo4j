/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.util.Map;
import java.util.NoSuchElementException;

import org.neo4j.backup.impl.BackupClient;
import org.neo4j.backup.impl.BackupOutcome;
import org.neo4j.backup.impl.BackupProtocolService;
import org.neo4j.backup.impl.BackupServer;
import org.neo4j.backup.impl.ConsistencyCheck;
import org.neo4j.com.ComException;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.SimpleLogService;
import org.neo4j.kernel.impl.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.store.UnexpectedStoreVersionException;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class BackupTool
{
    private static final String TO = "to";
    private static final String HOST = "host";
    private static final String PORT = "port";

    private static final String FROM = "from";

    private static final String VERIFY = "verify";
    private static final String CONFIG = "config";

    private static final String CONSISTENCY_CHECKER = "consistency-checker";

    private static final String TIMEOUT = "timeout";
    private static final String FORENSICS = "gather-forensics";
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
        System.err.println("WARNING: neo4j-backup is deprecated and support for it will be removed in a future\n" +
                "version of Neo4j; please use neo4j-admin backup instead.\n");
        BackupTool tool = new BackupTool( new BackupProtocolService(), System.out );
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
            System.out.println( "Backup failed." );
            exitFailure( e.getMessage() );
        }
    }

    private final BackupProtocolService backupProtocolService;
    private final PrintStream systemOut;

    BackupTool( BackupProtocolService backupProtocolService, PrintStream systemOut )
    {
        this.backupProtocolService = backupProtocolService;
        this.systemOut = systemOut;
    }

    BackupOutcome run( String[] args ) throws ToolFailureException
    {
        Args arguments = Args.withFlags( VERIFY ).parse( args );

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
        Path to = Paths.get( args.get( TO ).trim() );
        Config tuningConfiguration = readConfiguration( args );
        boolean forensics = args.getBoolean( FORENSICS, false, true );
        ConsistencyCheck consistencyCheck = parseConsistencyChecker( args );

        long timeout = args.getDuration( TIMEOUT, BackupClient.BIG_READ_TIMEOUT );

        URI backupURI = resolveBackupUri( from, args, tuningConfiguration );

        HostnamePort hostnamePort = newHostnamePort( backupURI );

        return executeBackup( hostnamePort, to, consistencyCheck, tuningConfiguration, timeout, forensics );
    }

    private static ConsistencyCheck parseConsistencyChecker( Args args )
    {
        boolean verify = args.getBoolean( VERIFY, true, true );
        if ( verify )
        {
            String consistencyCheckerName = args.get( CONSISTENCY_CHECKER, ConsistencyCheck.FULL.name(),
                    ConsistencyCheck.FULL.name() );
            return ConsistencyCheck.fromString( consistencyCheckerName );
        }
        return ConsistencyCheck.NONE;
    }

    private BackupOutcome runBackup( Args args ) throws ToolFailureException
    {
        String host = args.get( HOST ).trim();
        int port = args.getNumber( PORT, BackupServer.DEFAULT_PORT ).intValue();
        Path to = Paths.get( args.get( TO ).trim() );
        Config tuningConfiguration = readConfiguration( args );
        boolean forensics = args.getBoolean( FORENSICS, false, true );
        ConsistencyCheck consistencyCheck = parseConsistencyChecker( args );

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

        long timeout = args.getDuration( TIMEOUT, BackupClient.BIG_READ_TIMEOUT );

        URI backupURI = newURI( DEFAULT_SCHEME + "://" + host + ":" + port ); // a bit of validation

        HostnamePort hostnamePort = newHostnamePort( backupURI );

        return executeBackup( hostnamePort, to, consistencyCheck, tuningConfiguration, timeout, forensics );
    }

    BackupOutcome executeBackup( HostnamePort hostnamePort, Path to, ConsistencyCheck consistencyCheck,
                                 Config config, long timeout, boolean forensics ) throws ToolFailureException
    {
        try
        {
            systemOut.println( "Performing backup from '" + hostnamePort + "'" );
            String host = hostnamePort.getHost();
            int port = hostnamePort.getPort();

            BackupOutcome outcome = backupProtocolService.doIncrementalBackupOrFallbackToFull( host, port, to, consistencyCheck, config, timeout, forensics );
            systemOut.println( "Done" );
            return outcome;
        }
        catch ( UnexpectedStoreVersionException | IncrementalBackupNotPossibleException e )
        {
            throw new ToolFailureException( e.getMessage(), e );
        }
        catch ( MismatchingStoreIdException e )
        {
            throw new ToolFailureException( String.format( MISMATCHED_STORE_ID, e.getExpected(), e.getEncountered() ) );
        }
        catch ( ComException e )
        {
            throw new ToolFailureException( "Couldn't connect to '" + hostnamePort + "'", e );
        }
    }

    private static Config readConfiguration( Args arguments ) throws ToolFailureException
    {
        Map<String, String> specifiedConfig = stringMap();

        String configFilePath = arguments.get( CONFIG, null );
        if ( configFilePath != null )
        {
            File configFile = new File( configFilePath );
            try
            {
                specifiedConfig = MapUtil.load( configFile );
            }
            catch ( IOException e )
            {
                throw new ToolFailureException( String.format( "Could not read configuration file [%s]",
                        configFilePath ), e );
            }
        }
        return Config.defaults( specifiedConfig );
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
            return resolveUriWithProvider( "ha", config, from, arguments );
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

    private static URI resolveUriWithProvider( String providerName, Config config, String from, Args args )
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
            ZoneId logTimeZone = config.get( GraphDatabaseSettings.db_timezone ).getZoneId();
            FormattedLogProvider userLogProvider = FormattedLogProvider.withZoneId( logTimeZone ).toOutputStream( System.out );
            return service.resolve( from, args, new SimpleLogService( userLogProvider, NullLogProvider.getInstance() ) );
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
