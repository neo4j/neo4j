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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
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
            description = "Name of the database to push. " +
                          "Defaults to " + DEFAULT_DATABASE_NAME + ". " +
                          "This argument cannot be used together with --dump.",
            converter = Converters.DatabaseNameConverter.class )
    private NormalizedDatabaseName database;
    @Option( names = "--dump",
            description = "'/path/to/my-neo4j-database-dump-file' Path to an existing database dump for upload. " +
                          "This argument cannot be used together with --database." )
    private Path dump;
    @Option( names = {"--temp-file-location", "--dump-to"},
            description = "'/path/to/temp-file' Target path for temporary database dump file to be uploaded. " +
                          "Used in combination with the --database argument." )
    private Path tmpDumpFile;
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

            Size size = sourceSize( ctx, dump, database );
            verbose( "Checking database size %s fits at %s\n", size.humanize(), consoleURL );
            copier.checkSize( verbose, consoleURL, size, bearerToken );

            Source source = initiateSource( ctx, dump, database, tmpDumpFile );
            // only mark dump to delete after processing, if we just created it
            boolean deleteDump = dump == null;

            verbose( "Uploading data of %s to %s\n", size.humanize(), consoleURL );
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

    private void verbose( String format, Object... args )
    {
        if ( verbose )
        {
            ctx.out().printf( format, args );
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

    private Source initiateSource( ExecutionContext ctx, Path dump, NormalizedDatabaseName database, Path to ) throws CommandFailedException
    {
        // Either a dump or database name (of a stopped database) can be provided
        if ( dump != null && database != null )
        {
            throw new CommandFailedException( "Provide either a dump or database name, not both" );
        }
        else if ( dump != null )
        {
            if ( Files.notExists( dump ) )
            {
                throw new CommandFailedException( format( "The provided dump '%s' file doesn't exist", dump ) );
            }
            return new Source( dump, Size.ofDump( dumpSize( dump ) ) );
        }
        else
        {
            Path dumpPath = to != null ? to : ctx.homeDir().resolve( "dump-of-" + database.name() + "-" + currentTimeMillis() );
            if ( Files.exists( dumpPath ) )
            {
                throw new CommandFailedException( format( "The provided dump-to target '%s' file already exists", dumpPath ) );
            }
            Path dumpFile = dumpCreator.dumpDatabase( database.name(), dumpPath );
            return new Source( dumpPath, Size.of( dumpSize( dumpFile ), fullSize( ctx, database ) ) );
        }
    }

    private Size sourceSize( ExecutionContext ctx, Path dump, NormalizedDatabaseName database ) throws CommandFailedException
    {
        // Either a dump or database name (of a stopped database) can be provided
        if ( dump != null && database != null )
        {
            throw new CommandFailedException( "Provide either a dump or database name, not both" );
        }
        else if ( dump != null && database == null )
        {
            return Size.ofDump( dumpSize( dump ) );
        }
        else if ( dump != null && database != null ) // TODO consider actually making this case work
        {
            return Size.of( dumpSize( dump ), fullSize( ctx, database ) );
        }
        else
        {
            return Size.ofFull( fullSize( ctx, database ) );
        }
    }

    private long fullSize( ExecutionContext ctx, NormalizedDatabaseName database )
    {
        Path configFile = ctx.confDir().resolve( Config.DEFAULT_CONFIG_FILE_NAME );

        DatabaseLayout layout = Neo4jLayout.of( getConfig( configFile ) ).databaseLayout( database.name() );
        long storeFilesSize = FileUtils.sizeOf( layout.databaseDirectory().toFile() );
        long txLogSize;
        try
        {
            txLogSize = FileUtils.sizeOf( layout.getTransactionLogsDirectory().toFile() );
        }
        catch ( IllegalArgumentException e )
        {
            txLogSize = 0;
        }
        long size = txLogSize + storeFilesSize;
        verbose( "Determined FullSize=%d bytes from storeFileSize=%d + txLogSize=%d in database '%s'\n", size, storeFilesSize, txLogSize, database.name() );
        return size;
    }

    private long dumpSize( Path dump )
    {
        long sizeInBytes = readSizeFromDumpMetaData( dump );
        verbose( "Determined DumpSize=%d bytes from dump at %s\n", sizeInBytes, dump );
        return sizeInBytes;
    }

    public static long readSizeFromDumpMetaData( Path dump )
    {
        Loader.DumpMetaData metaData;
        try
        {
            metaData = new Loader( System.out ).getMetaData( dump );
        }
        catch ( IOException e )
        {
            throw new CommandFailedException( "Unable to check size of database dump.", e );
        }
        return Long.parseLong( metaData.byteCount );
    }

    private Config getConfig( Path configFile )
    {
        if ( !ctx.fs().fileExists( configFile ) )
        {
            throw new CommandFailedException( "Unable to find config file, tried: " + configFile.toAbsolutePath() );
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
            throw new CommandFailedException( "Failed to read config file: " + configFile.toAbsolutePath(), e );
        }
    }

    public static class Source
    {
        final Path path;
        final Size size;

        public Source( Path path, Size size )
        {
            this.path = path;
            this.size = size;
        }

        @Override
        public int hashCode()
        {
            return path.hashCode() + 31 * size.hashCode();
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( obj instanceof Source )
            {
                Source other = (Source) obj;
                return path.equals( other.path ) && size.equals( other.size );
            }
            else
            {
                return false;
            }
        }
    }

    public static class Size
    {
        public static final long COMPRESSION = 4;
        final Optional<Long> dumpSize;
        final Optional<Long> fullSize;

        private Size( Optional<Long> dumpSize, Optional<Long> fullSize )
        {
            this.dumpSize = dumpSize;
            this.fullSize = fullSize;
        }

        public static Size of( long dumpSize, long fullSize )
        {
            return new Size( Optional.of( dumpSize ), Optional.of( fullSize ) );
        }

        public static Size ofDump( long dumpSize )
        {
            return new Size( Optional.of( dumpSize ), Optional.empty() );
        }

        public static Size ofFull( long fullSize )
        {
            return new Size( Optional.empty(), Optional.of( fullSize ) );
        }

        private void addPart( ArrayList<String> parts, Optional<Long> size, String field, String sep )
        {
            size.ifPresent( aLong -> parts.add( format( "%s%s%d", field, sep, aLong ) ) );
        }

        @Override
        public String toString()
        {
            ArrayList<String> parts = new ArrayList<>();
            addPart( parts, dumpSize, "DumpSize", "=" );
            addPart( parts, fullSize, "FullSize", "=" );
            return String.join( ", ", parts );
        }

        public String toJson( String... extra )
        {
            ArrayList<String> parts = new ArrayList<>( Arrays.asList( extra ) );
            addPart( parts, dumpSize, "\"DumpSize\"", ":" );
            addPart( parts, fullSize, "\"FullSize\"", ":" );
            return "{" + String.join( ", ", parts ) + "}";
        }

        public Object humanize()
        {
            if ( fullSize.isPresent() )
            {
                return format( "%.1f GB", bytesToGibibytes( fullSize.get() ) );
            }
            else if ( dumpSize.isPresent() )
            {
                return format( "%.1f GB", bytesToGibibytes( COMPRESSION * dumpSize.get() ) );
            }
            else
            {
                return "<missing>";
            }
        }

        @Override
        public int hashCode()
        {
            return dumpSize.hashCode() + 31 * fullSize.hashCode();
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( obj instanceof Size )
            {
                Size other = (Size) obj;
                return dumpSize.equals( other.dumpSize ) && fullSize.equals( other.fullSize );
            }
            else
            {
                return false;
            }
        }

        public static double bytesToGibibytes( long sizeInBytes )
        {
            return sizeInBytes / (double) (1024 * 1024 * 1024);
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
         * @return a bearer token to pass into {@link #copy(boolean, String, String, Source, boolean, String)} later on.
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
        void copy( boolean verbose, String consoleURL, String boltUri, Source source, boolean deleteSourceAfterImport, String bearerToken )
                throws CommandFailedException;

        /**
         * @param verbose     whether or not to print verbose debug messages/statuses
         * @param consoleURL  console URI to target.
         * @param sizeInBytes database size in bytes
         * @param bearerToken token from successful {@link #authenticate(boolean, String, String, char[], boolean)} call.
         * @throws CommandFailedException if the database won't fit on the aura instance
         */
        void checkSize( boolean verbose, String consoleURL, Size sizeInBytes, String bearerToken ) throws CommandFailedException;
    }

    public interface DumpCreator
    {
        Path dumpDatabase( String databaseName, Path targetDumpFile ) throws CommandFailedException;
    }
}
