/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.pushtocloud;

import org.apache.commons.io.FileUtils;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.Converters;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.NormalizedDatabaseName;
import org.neo4j.dbms.archive.Loader;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@Command(
        name = "push-to-cloud",
        description = "Push your local database to a Neo4j Aura instance. The database must be shutdown in order to take a dump to upload. " +
                      "The target location is your Neo4j Aura Bolt URI. You will be asked your Neo4j Cloud username and password during " +
                      "the push-to-cloud operation."
)

public class PushToCloudCommand extends AbstractCommand
{
    private final Copier copier;
    private final DumpCreator dumpCreator;
    private final PushToCloudConsole cons;
    @Option( names = "--database",
            description = "Name of the database to push. Defaults to " + DEFAULT_DATABASE_NAME +
                          "This argument cannot be used together with --dump.",
            converter = Converters.DatabaseNameConverter.class )
    private NormalizedDatabaseName database;
    @Option( names = "--dump",
            description = "'/path/to/my-neo4j-database-dump-file' Path to an existing database dump for upload. " +
                          "This argument cannot be used together with --database." )
    private File dump;
    @Option( names = {"--temp-file-location", "--dump-to"},
            description = "'/path/to/temp-file' Target path for temporary database dump file to be uploaded. " +
                          "Used in combination with the --database argument." )
    private File tmpDumpFile;
    @Option( names = "--bolt-uri", arity = "1", required = true,
            description = "'neo4j://mydatabaseid.databases.neo4j.io' Bolt URI of target database" )
    private String boltURI;
    @Option( names = "--username", defaultValue = "${NEO4J_USERNAME}",
            description = "Optional: Username of the target database to push this database to. Prompt will ask for username if not provided. " +
                          "Alternatively NEO4J_USERNAME environment variable can be used." )
    private String username;

    @Option( names = "--password", defaultValue = "${NEO4J_PASSWORD}",
            description = "Optional: Password of the target database to push this database to. Prompt will ask for password if not provided. " +
                          "Alternatively NEO4J_PASSWORD environment variable can be used." )
    private String password;
    @Option( names = "--overwrite", description = "Optional: Overwrite the data in the target database." )
    private boolean overwrite;

    public PushToCloudCommand( ExecutionContext ctx, Copier copier, DumpCreator dumpCreator, PushToCloudConsole cons )
    {
        super( ctx );
        this.copier = copier;
        this.dumpCreator = dumpCreator;
        this.cons = cons;
    }

    @Override
    public void execute()
    {
        try
        {
            if ( (database == null || isEmpty( database.name() )) && (dump == null || isEmpty( dump.toString() )) )
            {
                database = new NormalizedDatabaseName( DEFAULT_DATABASE_NAME );
            }
            if ( isEmpty( username ) )
            {
                if ( (username = cons.readLine( "%s", "Neo4j aura username (default: neo4j):" )) == null )
                {
                    username = "neo4j";
                }
            }
            char[] pass;
            if ( isEmpty( password ) )
            {
                if ( (pass = cons.readPassword( "Neo4j aura password for %s:", username )).length == 0 )
                {
                    throw new CommandFailedException(
                            "Please supply a password, either by '--password' parameter, 'NEO4J_PASSWORD' environment variable, or prompt" );
                }
            }
            else
            {
                pass = password.toCharArray();
            }

            String consoleURL = buildConsoleURI( boltURI );
            String bearerToken = copier.authenticate( verbose, consoleURL, username, pass, overwrite );

            long size = sourceSize( ctx, dump, database );
            copier.checkSize( verbose, consoleURL, size, bearerToken );

            Path source = initiateSource( ctx, dump, database, tmpDumpFile );
            // only mark dump to delete after processing, if we just created it
            boolean deleteDump = dump == null;

            copier.copy( verbose, consoleURL, boltURI, source, deleteDump, bearerToken );
        }
        catch ( Exception e )
        {
            if ( verbose )
            {
                e.printStackTrace( ctx.out() );
            }
            throw e;
        }
    }

    private String buildConsoleURI( String boltURI ) throws CommandFailedException
    {
        // A boltURI looks something like this:
        //
        //   bolt+routing://mydbid-myenvironment.databases.neo4j.io
        //                  <─┬──><──────┬─────>
        //                    │          └──────── environment
        //                    └─────────────────── database id
        //
        // Constructing a console URI takes elements from the bolt URI and places them inside this URI:
        //
        //   https://console<environment>.neo4j.io/v1/databases/<database id>
        //
        // Examples:
        //
        //   bolt+routing://rogue.databases.neo4j.io  --> https://console.neo4j.io/v1/databases/rogue
        //   bolt+routing://rogue-mattias.databases.neo4j.io  --> https://console-mattias.neo4j.io/v1/databases/rogue

        Pattern pattern = Pattern.compile( "(?:bolt(?:\\+routing)?|neo4j(?:\\+s|\\+ssc)?)://([^-]+)(-(.+))?.databases.neo4j.io$" );
        Matcher matcher = pattern.matcher( boltURI );
        if ( !matcher.matches() )
        {
            throw new CommandFailedException( "Invalid Bolt URI '" + boltURI + "'" );
        }

        String databaseId = matcher.group( 1 );
        String environment = matcher.group( 2 );
        return String.format( "https://console%s.neo4j.io/v1/databases/%s", environment == null ? "" : environment, databaseId );
    }

    private Path initiateSource( ExecutionContext ctx, File dump, NormalizedDatabaseName database, File to ) throws CommandFailedException
    {
        // Either a dump or database name (of a stopped database) can be provided
        if ( dump != null && database != null )
        {
            throw new CommandFailedException( "Provide either a dump or database name, not both" );
        }
        else if ( dump != null )
        {
            Path path = dump.toPath();
            if ( !Files.exists( path ) )
            {
                throw new CommandFailedException( format( "The provided dump '%s' file doesn't exist", path ) );
            }
            return path;
        }
        else
        {

            Path dumpFile = to != null ? to.toPath() : ctx.homeDir().resolve( "dump-of-" + database + "-" + currentTimeMillis() );
            if ( Files.exists( dumpFile ) )
            {
                throw new CommandFailedException( format( "The provided dump-to target '%s' file already exists", dumpFile ) );
            }
            dumpCreator.dumpDatabase( database.name(), dumpFile );
            return dumpFile;
        }
    }

    private long sourceSize( ExecutionContext ctx, File dump, NormalizedDatabaseName database ) throws CommandFailedException
    {
        // Either a dump or database name (of a stopped database) can be provided
        if ( dump != null && database != null )
        {
            throw new CommandFailedException( "Provide either a dump or database name, not both" );
        }
        else if ( dump != null )
        {
            Path path = dump.toPath();
            Loader.DumpMetaData metaData;
            try
            {
                metaData = new Loader( System.out ).getMetaData( path );
            }
            catch ( IOException e )
            {
                throw new CommandFailedException( "Unable to check size of database dump.", e );
            }
            return Long.parseLong( metaData.byteCount );
        }
        else
        {
            File configFile = ctx.confDir().resolve( Config.DEFAULT_CONFIG_FILE_NAME ).toFile();

            DatabaseLayout layout = Neo4jLayout.of( getConfig( configFile ) ).databaseLayout( database.name() );
            long storeFilesSize = FileUtils.sizeOf( layout.databaseDirectory() );
            long txLogSize;
            try
            {
                txLogSize = FileUtils.sizeOf( layout.getTransactionLogsDirectory() );
            }
            catch ( IllegalArgumentException e )
            {
                txLogSize = 0;
            }
            return txLogSize +
                   storeFilesSize;
        }
    }

    private Config getConfig( File configFile )
    {
        if ( !ctx.fs().fileExists( configFile ) )
        {
            throw new CommandFailedException( "Unable to find config file, tried: " + configFile.getAbsolutePath() );
        }
        try
        {
            return Config.newBuilder()
                         .fromFile( configFile )
                         .set( GraphDatabaseSettings.neo4j_home, ctx.homeDir().toAbsolutePath() )
                         .build();
        }
        catch ( Exception e )
        {
            throw new CommandFailedException( "Failed to read config file: " + configFile.getAbsolutePath(), e );
        }
    }

    public interface Copier
    {
        /**
         * Authenticates user by name and password.
         *
         * @param verbose          whether or not to print verbose debug messages/statuses.
         * @param consoleURL       console URI to target.
         * @param username         the username.
         * @param password         the password.
         * @param consentConfirmed user confirmed to overwrite existing database.
         * @return a bearer token to pass into {@link #copy(boolean, String, String, Path, boolean, String)} later on.
         * @throws CommandFailedException on authentication failure or some other unexpected failure.
         */
        String authenticate( boolean verbose, String consoleURL, String username, char[] password, boolean consentConfirmed ) throws CommandFailedException;

        /**
         * Copies the given dump to the console URI.
         *
         * @param verbose                 whether or not to print verbose debug messages/statuses.
         * @param consoleURL              console URI to target.
         * @param boltUri                 bolt URI to target database.
         * @param source                  dump to copy to the target.
         * @param deleteSourceAfterImport delete the dump after successful import
         * @param bearerToken             token from successful {@link #authenticate(boolean, String, String, char[], boolean)} call.
         * @throws CommandFailedException on copy failure or some other unexpected failure.
         */
        void copy( boolean verbose, String consoleURL, String boltUri, Path source, boolean deleteSourceAfterImport, String bearerToken )
                throws CommandFailedException;

        /**
         * @param verbose     whether or not to print verbose debug messages/statuses
         * @param consoleURL  console URI to target.
         * @param sizeInBytes database size in bytes
         * @param bearerToken token from successful {@link #authenticate(boolean, String, String, char[], boolean)} call.
         * @throws CommandFailedException if the database won't fit on the aura instance
         */
        void checkSize( boolean verbose, String consoleURL, long sizeInBytes, String bearerToken ) throws CommandFailedException;
    }

    public interface DumpCreator
    {
        void dumpDatabase( String databaseName, Path targetDumpFile ) throws CommandFailedException;
    }
}
