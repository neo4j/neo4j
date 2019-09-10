/*
 * Copyright (c) 2002-2019 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.arguments.Arguments;
import org.neo4j.commandline.arguments.MandatoryNamedArg;
import org.neo4j.commandline.arguments.OptionalNamedArg;
import org.neo4j.helpers.Args;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;

public class PushToCloudCommand implements AdminCommand
{
    static final String ARG_DATABASE = "database";
    static final String ARG_BOLT_URI = "bolt-uri";
    static final String ARG_DUMP = "dump";
    static final String ARG_DUMP_TO = "dump-to";
    static final String ARG_VERBOSE = "v";
    static final String ARG_USERNAME = "username";
    static final String ARG_PASSWORD = "password";

    static final Arguments arguments = new Arguments()
            // Provide a (potentially running?) database
            .withDatabase()
            // ... or an existing backup/dump of a database
            .withArgument( new OptionalNamedArg( ARG_DUMP, "/path/to/my-neo4j-database-dump-file", null,
                    "Existing dump of a database, produced from the dump command" ) )
            .withArgument( new OptionalNamedArg( ARG_DUMP_TO, "/path/to/dump-file-to-be-created", null,
                    "Location to create the dump file if database is given. The database will be dumped to this file instead of a default location" ) )
            .withArgument( new MandatoryNamedArg( ARG_BOLT_URI, "bolt+routing://mydatabaseid.databases.neo4j.io",
                    "Bolt URI pointing out the target location to push the database to" ) )
            .withArgument( new OptionalNamedArg( ARG_VERBOSE, "true/false", null,
                    "Whether or not to be verbose about internal details and errors." ) )
            .withArgument( new OptionalNamedArg( ARG_USERNAME, "neo4j", null,
                    "Username of the target database to push this database to." ) )
            .withArgument( new OptionalNamedArg( ARG_PASSWORD, "true/false", null,
                    "Password of the target database to push this database to." ) );

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
            Path source = initiateSource( arguments );

            String passwordFromArg = arguments.get( ARG_PASSWORD );
            String username = arguments.get( ARG_USERNAME );

            if ( ( username == null && passwordFromArg != null ) || ( username != null && passwordFromArg == null ) )
            {
                throw new IncorrectUsage( "Provide either 'username' and 'password' or none" );
            }

            if ( username == null )
            {
                username = outsideWorld.promptLine("Neo4j cloud database user name: ");
            }
            char[] password;
            if ( passwordFromArg != null )
            {
                password = passwordFromArg.toCharArray();
            }
            else
            {
                password = outsideWorld.promptPassword( "Neo4j cloud database password: " );
            }

            String boltURI = arguments.get( ARG_BOLT_URI );
            String consoleURL = buildConsoleURI( boltURI );
            copier.copy( verbose, consoleURL, source, username, password );
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

        Pattern pattern = Pattern.compile( "bolt\\+routing://([^-]+)(-(.+))?.databases.neo4j.io$" );
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
        else if ( database != null )
        {
            String to = arguments.get( ARG_DUMP_TO );
            Path dumpFile = to != null ? Paths.get( to ) : homeDir.resolve( "dump-of-" + database + "-" + currentTimeMillis() );
            if ( Files.exists( dumpFile ) )
            {
                throw new CommandFailed( format( "The provided dump-to target '%s' file already exists", dumpFile ) );
            }
            dumpCreator.dumpDatabase( database, dumpFile );
            return dumpFile;
        }
        else
        {
            throw new IncorrectUsage( "Provide either a dump or database name" );
        }
    }

    public interface Copier
    {
        void copy( boolean verbose, String consoleURL, Path source, String username, char[] password ) throws CommandFailed;
    }

    public interface DumpCreator
    {
        void dumpDatabase( String databaseName, Path targetDumpFile ) throws CommandFailed, IncorrectUsage;
    }
}
