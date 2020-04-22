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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.arguments.Arguments;
import org.neo4j.commandline.arguments.MandatoryNamedArg;
import org.neo4j.commandline.arguments.OptionalNamedArg;
import org.neo4j.commandline.arguments.common.Database;
import org.neo4j.helpers.Args;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;

public class PushToCloudCommand implements AdminCommand
{
    static final String ARG_DATABASE = "database";
    static final String ARG_BOLT_URI = "bolt-uri";
    static final String ARG_DUMP = "dump";
    static final String ARG_DUMP_TO = "dump-to";
    static final String ARG_VERBOSE = "verbose";
    static final String ARG_OVERWRITE = "overwrite";
    static final String ARG_USERNAME = "username";
    static final String ARG_PASSWORD = "password";
    static final String ENV_USERNAME = "NEO4J_USERNAME";
    static final String ENV_PASSWORD = "NEO4J_PASSWORD";

    static final Arguments arguments = new Arguments()
            // Provide a (potentially running?) database
            .withDatabase()
            // ... or an existing backup/dump of a database
            .withArgument( new OptionalNamedArg( ARG_DUMP, "/path/to/my-neo4j-database-dump-file", null,
                    "Path to an existing database dump for upload. This arugment cannot be used together with --database." ) )
            .withArgument( new OptionalNamedArg( ARG_DUMP_TO, "/path/to/dump-file-to-be-created", null,
                    "Target path for dump file. Used in combination with the --database argument." ) )
            .withArgument( new MandatoryNamedArg( ARG_BOLT_URI, "bolt+routing://mydatabaseid.databases.neo4j.io",
                    "Bolt URI of target database" ) )
            .withArgument( new OptionalNamedArg( ARG_VERBOSE, "true/false", null,
                    "Enable verbose output." ) )
            .withArgument( new OptionalNamedArg( ARG_USERNAME, "neo4j", null,
                    "Optional: Username of the target database to push this database to. Prompt will ask for username if not provided. " +
                            "Alternatively NEO4J_USERNAME environment variable can be used." ) )
            .withArgument( new OptionalNamedArg( ARG_PASSWORD, "mYs3cr3tPa$$w0rd", null,
                    "Optional: Password of the target database to push this database to. Prompt will ask for password if not provided. " +
                            "Alternatively NEO4J_PASSWORD environment variable can be used." ) )
            .withArgument( new OptionalNamedArg( ARG_OVERWRITE, "true/false", "false",
                    "Optional: Overwrite the data in the target database." ) );

    private final Path homeDir;
    private final Path configDir;
    private final OutsideWorld outsideWorld;
    private final Copier copier;
    private final DumpCreator dumpCreator;

    public PushToCloudCommand( Path homeDir, Path configDir, OutsideWorld outsideWorld, Copier copier, DumpCreator dumpCreator )
    {
        this.homeDir = homeDir;
        this.configDir = configDir;
        this.outsideWorld = outsideWorld;
        this.copier = copier;
        this.dumpCreator = dumpCreator;
    }

    @Override
    public void execute( String[] args ) throws IncorrectUsage, CommandFailed
    {
        Args arguments = Args.parse( args );
        boolean verbose = arguments.getBoolean( ARG_VERBOSE );
        try
        {
            String passwordFromArg = arguments.get( ARG_PASSWORD );
            String username = arguments.get( ARG_USERNAME );

            String usernameFromEnv = System.getenv( ENV_USERNAME );
            String passwordFromEnv = System.getenv( ENV_PASSWORD );

            // the strategy here is to have a priority of ways to pass in username/password
            // 1. highest priority is cli argument, e.g. --username
            // 2. priority is the environment variable, e.g. ENV_USERNAME
            // if 1. and 2. is not configured, prompt for input.
            if ( username == null )
            {
                if ( usernameFromEnv != null )
                {
                    username = usernameFromEnv;
                }
                else
                {
                    username = outsideWorld.promptLine( "Neo4j Aura database username (default: neo4j): " );
                }
            }
            // default username to neo4j if user pressed 'enter' during the prompt
            if ( username == null || "".equals( username ) )
            {
                username = "neo4j";
            }

            char[] password;
            if ( passwordFromArg != null )
            {
                password = passwordFromArg.toCharArray();
            }
            else
            {
                if ( passwordFromEnv != null )
                {
                    password = passwordFromEnv.toCharArray();
                }
                else
                {
                    password = outsideWorld.promptPassword( format( "Neo4j Aura database password for %s: ", username ) );
                }
            }

            String boltURI = arguments.get( ARG_BOLT_URI );
            if ( boltURI == null || "".equals( boltURI ) )
            {
                boltURI = outsideWorld.promptLine( "Neo4j Aura database Bolt URI: " );
            }
            if ( boltURI == null || "".equals( boltURI ) )
            {
                throw new IncorrectUsage( "Please provide a Neo4j Aura Bolt URI of the target location to push the database to, " +
                        "using the --bolt-uri argument." );
            }
            String confirmationViaArgument = arguments.get( ARG_OVERWRITE, "false", "true" );

            String consoleURL = buildConsoleURI( boltURI );
            String bearerToken = copier.authenticate( verbose, consoleURL, username, password, "true".equals( confirmationViaArgument ) );

            Path source = initiateSource( arguments );
            // only mark dump to delete after processing, if we just created it
            boolean deleteDump = arguments.get( ARG_DUMP ) == null;

            copier.copy( verbose, consoleURL, boltURI, source, deleteDump, bearerToken );
        }
        catch ( Exception e )
        {
            if ( verbose )
            {
                outsideWorld.printStacktrace( e );
            }
            throw e;
        }
    }

    private String buildConsoleURI( String boltURI ) throws IncorrectUsage
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
            throw new IncorrectUsage( "Invalid Bolt URI '" + boltURI + "'" );
        }

        String databaseId = matcher.group( 1 );
        String environment = matcher.group( 2 );
        return String.format( "https://console%s.neo4j.io/v1/databases/%s", environment == null ? "" : environment, databaseId );
    }

    private Path initiateSource( Args arguments ) throws IncorrectUsage, CommandFailed
    {
        // Either a dump or database name (of a stopped database) can be provided
        String dump = arguments.get( ARG_DUMP );
        String database = arguments.get( ARG_DATABASE );
        if ( dump != null && database != null )
        {
            throw new IncorrectUsage( "Provide either a dump or database name, not both" );
        }
        else if ( dump != null )
        {
            Path path = Paths.get( dump );
            if ( !Files.exists( path ) )
            {
                throw new CommandFailed( format( "The provided dump '%s' file doesn't exist", path ) );
            }
            return path;
        }
        else
        {
            if ( database == null )
            {
                database = new Database().defaultValue();
                outsideWorld.stdOutLine( "Selecting default database '" + database + "'" );
            }

            String to = arguments.get( ARG_DUMP_TO );
            Path dumpFile = to != null ? Paths.get( to ) : homeDir.resolve( "dump-of-" + database + "-" + currentTimeMillis() );
            if ( Files.exists( dumpFile ) )
            {
                throw new CommandFailed( format( "The provided dump-to target '%s' file already exists", dumpFile ) );
            }
            dumpCreator.dumpDatabase( database, dumpFile );
            return dumpFile;
        }
    }

    public interface Copier
    {
        /**
         * Authenticates user by name and password.
         *
         * @param verbose whether or not to print verbose debug messages/statuses.
         * @param consoleURL console URI to target.
         * @param username the username.
         * @param password the password.
         * @param consentConfirmed user confirmed to overwrite existing database.
         * @return a bearer token to pass into {@link #copy(boolean, String, String, Path, boolean, String)} later on.
         * @throws CommandFailed on authentication failure or some other unexpected failure.
         */
        String authenticate( boolean verbose, String consoleURL, String username, char[] password, boolean consentConfirmed ) throws CommandFailed;

        /**
         * Copies the given dump to the console URI.
         *
         * @param verbose whether or not to print verbose debug messages/statuses.
         * @param consoleURL console URI to target.
         * @param boltUri bolt URI to target database.
         * @param source dump to copy to the target.
         * @param deleteSourceAfterImport delete the dump after successful import
         * @param bearerToken token from successful {@link #authenticate(boolean, String, String, char[], boolean)} call.
         * @throws CommandFailed on copy failure or some other unexpected failure.
         */
        void copy( boolean verbose, String consoleURL, String boltUri, Path source, boolean deleteSourceAfterImport, String bearerToken ) throws CommandFailed;
    }

    public interface DumpCreator
    {
        void dumpDatabase( String databaseName, Path targetDumpFile ) throws CommandFailed, IncorrectUsage;
    }
}
