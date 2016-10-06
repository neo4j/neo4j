/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.consistency;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.Strings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory;
import org.neo4j.kernel.impl.recovery.RecoveryRequiredChecker;
import org.neo4j.kernel.impl.util.Converters;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.server.configuration.ConfigLoader;

import static org.neo4j.dbms.DatabaseManagementSystemSettings.database_path;

public class CheckConsistencyCommand implements AdminCommand
{
    public static class Provider extends AdminCommand.Provider
    {
        public Provider()
        {
            super( "check-consistency" );
        }

        @Override
        public Optional<String> arguments()
        {
            return Optional.of( "--database=<database> [--additional-config=<file>] [--verbose]" );
        }

        @Override
        public String description()
        {
            return "Check the consistency of a database.";
        }

        @Override
        public AdminCommand create( Path homeDir, Path configDir, OutsideWorld outsideWorld )
        {
            return new CheckConsistencyCommand( homeDir, configDir, outsideWorld );
        }
    }

    private final Path homeDir;
    private final Path configDir;
    private final OutsideWorld outsideWorld;
    private final ConsistencyCheckService consistencyCheckService;
    private final FileSystemAbstraction fileSystemAbstraction;

    public CheckConsistencyCommand( Path homeDir, Path configDir, OutsideWorld outsideWorld )
    {
        this( homeDir, configDir, outsideWorld, new ConsistencyCheckService() );
    }

    public CheckConsistencyCommand( Path homeDir, Path configDir, OutsideWorld outsideWorld,
            ConsistencyCheckService consistencyCheckService )
    {
        this.homeDir = homeDir;
        this.configDir = configDir;
        this.outsideWorld = outsideWorld;
        this.consistencyCheckService = consistencyCheckService;
        this.fileSystemAbstraction = new DefaultFileSystemAbstraction();
    }

    @Override
    public void execute( String[] args ) throws IncorrectUsage, CommandFailed
    {
        String database;
        Boolean verbose;
        File additionalConfigFile;

        Args parsedArgs = Args.parse( args );
        try
        {
            database = parsedArgs.interpretOption( "database", Converters.mandatory(), s -> s );
            verbose = parsedArgs.getBoolean( "verbose" );
            additionalConfigFile =
                    parsedArgs.interpretOption( "additional-config", Converters.optional(), Converters.toFile() );
        }
        catch ( IllegalArgumentException e )
        {
            throw new IncorrectUsage( e.getMessage() );
        }

        Config config = loadNeo4jConfig( homeDir, configDir, database, loadAdditionalConfig( additionalConfigFile ) );

        try
        {
            File storeDir = config.get( database_path );
            checkDbState( storeDir, config );
            ConsistencyCheckService.Result consistencyCheckResult = consistencyCheckService
                    .runFullConsistencyCheck( storeDir, config, ProgressMonitorFactory.textual( System.err ),
                            FormattedLogProvider.toOutputStream( System.out ), this.fileSystemAbstraction, verbose );

            if ( !consistencyCheckResult.isSuccessful() )
            {
                throw new CommandFailed( String.format( "Inconsistencies found. See '%s' for details.",
                        consistencyCheckService.chooseReportPath( config, storeDir ).toString() ) );
            }
        }
        catch ( ConsistencyCheckIncompleteException | IOException e )
        {
            throw new CommandFailed( "Consistency checking failed." + e.getMessage(), e );
        }
    }

    private Map<String,String> loadAdditionalConfig( File additionalConfigFile )
    {
        if ( additionalConfigFile == null )
        {
            return new HashMap<>();
        }

        try
        {
            return MapUtil.load( additionalConfigFile );
        }
        catch ( IOException e )
        {
            throw new IllegalArgumentException(
                    String.format( "Could not read configuration file [%s]", additionalConfigFile ), e );
        }
    }

    private void checkDbState( File storeDir, Config additionalConfiguration ) throws CommandFailed
    {
        try ( PageCache pageCache = StandalonePageCacheFactory
                .createPageCache( this.fileSystemAbstraction, additionalConfiguration ) )
        {
            if ( new RecoveryRequiredChecker( this.fileSystemAbstraction, pageCache ).isRecoveryRequiredAt( storeDir ) )
            {
                throw new CommandFailed(
                        Strings.joinAsLines( "Active logical log detected, this might be a source of inconsistencies.",
                                "Please recover database before running the consistency check.",
                                "To perform recovery please start database and perform clean shutdown." ) );
            }
        }
        catch ( IOException e )
        {
            outsideWorld.stdErrLine(
                    "Failure when checking for recovery state: '%s', continuing as normal.%n" + e.getMessage() );
        }
    }

    private static Config loadNeo4jConfig( Path homeDir, Path configDir, String databaseName,
            Map<String,String> additionalConfig )
    {
        ConfigLoader configLoader = new ConfigLoader( settings() );
        Config config = configLoader.loadOfflineConfig( Optional.of( homeDir.toFile() ),
                Optional.of( configDir.resolve( "neo4j.conf" ).toFile() ) );
        additionalConfig.put( DatabaseManagementSystemSettings.active_database.name(), databaseName );
        return config.with( additionalConfig );
    }

    private static List<Class<?>> settings()
    {
        return Arrays.asList( GraphDatabaseSettings.class, DatabaseManagementSystemSettings.class,
                ConsistencyCheckSettings.class );
    }
}
