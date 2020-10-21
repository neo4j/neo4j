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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.LongConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

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
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;

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

    private static double ACCEPTABLE_DUMP_CHANGE = 0.1;    // Allow 10% deviation between measured database size, and actually stored dump

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

            Uploader uploader = prepareUploader( dump, database, tmpDumpFile );
            uploader.process( consoleURL, bearerToken );
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

    private Uploader prepareUploader( Path dump, NormalizedDatabaseName database, Path to ) throws CommandFailedException
    {
        // Either a dump or database name (of a stopped database) can be provided
        if ( dump != null && database != null )
        {
            throw new CommandFailedException( "Provide either a dump or database name, not both" );
        }
        else if ( dump != null )
        {
            return makeDumpUploader( dump );
        }
        else
        {
            return makeFullUploader( to );
        }
    }

    public DumpUploader makeDumpUploader( Path dump )
    {
        if ( Files.notExists( dump ) )
        {
            throw new CommandFailedException( format( "The provided dump '%s' file doesn't exist", dump ) );
        }
        return new DumpUploader( new Source( dump, dumpSize( dump ) ) );
    }

    public FullUploader makeFullUploader( Path to )
    {
        Path dumpPath = to != null ? to : ctx.homeDir().resolve( "dump-of-" + database.name() + "-" + currentTimeMillis() );
        if ( Files.exists( dumpPath ) )
        {
            throw new CommandFailedException( format( "The provided dump-to target '%s' file already exists", dumpPath ) );
        }
        return new FullUploader( new Source( dumpPath, fullSize( ctx, database ) ) );
    }

    abstract static class Uploader
    {
        protected final Source source;

        Uploader( Source source )
        {
            this.source = source;
        }

        long size()
        {
            return source.size();
        }

        Path path()
        {
            return source.path();
        }

        abstract void process( String consoleURL, String bearerToken );
    }

    class DumpUploader extends Uploader
    {
        DumpUploader( Source source )
        {
            super( source );
        }

        void process( String consoleURL, String bearerToken )
        {
            // Check size of dump (reading actual database size from dump header)
            verbose( "Checking database size %s fits at %s\n", sizeText( size() ), consoleURL );
            copier.checkSize( verbose, consoleURL, size(), bearerToken );

            // Upload dumpFile
            verbose( "Uploading data of %s to %s\n", sizeText( size() ), consoleURL );
            copier.copy( verbose, consoleURL, boltURI, source, false, bearerToken );
        }
    }

    class FullUploader extends Uploader
    {
        FullUploader( Source source )
        {
            super( source );
        }

        void process( String consoleURL, String bearerToken )
        {
            // Check size of full database
            verbose( "Checking database size %s fits at %s\n", sizeText( size() ), consoleURL );
            copier.checkSize( verbose, consoleURL, size(), bearerToken );

            // Dump database to dumpFile
            Path dumpFile = dumpCreator.dumpDatabase( database.name(), path() );
            long sizeFromDump = dumpSize( dumpFile );
            long sizeFromDatabase = size();
            verbose( "Validating sizes: fromDump=%d, fromDatabase=%d", sizeFromDump, sizeFromDatabase );
            if ( Math.abs( sizeFromDump - sizeFromDatabase ) > ACCEPTABLE_DUMP_CHANGE * sizeFromDatabase )
            {
                ctx.out().printf( "Warning: unexpectedly large difference between size in dump, and original size: %d != %d", sizeFromDump, sizeFromDatabase );
            }
            source.setSize( sizeFromDump );

            // Upload dumpFile
            verbose( "Uploading data of %s to %s\n", sizeText( size() ), consoleURL );
            copier.copy( verbose, consoleURL, boltURI, source, true, bearerToken );
        }
    }

    private long fullSize( ExecutionContext ctx, NormalizedDatabaseName database )
    {
        Path configFile = ctx.confDir().resolve( Config.DEFAULT_CONFIG_FILE_NAME );

        DatabaseLayout layout = Neo4jLayout.of( getConfig( configFile ) ).databaseLayout( database.name() );
        long storeFilesSize = FileUtils.sizeOf( layout.databaseDirectory().toFile() );
        long txLogSize = readTxLogsSize( layout.getTransactionLogsDirectory() );
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

    private long readTxLogsSize( Path txLogs )
    {
        long txLogSize = 0;
        if ( Files.exists(txLogs) )
        {
            if ( Files.isDirectory(txLogs) )
            {
                String[] logs = txLogs.toFile().list( ( dir, name ) -> name.startsWith( TransactionLogFilesHelper.DEFAULT_NAME ) );
                if ( logs != null && logs.length > 0 )
                {
                    TxSizeSetter setSize = new TxSizeSetter( logs.length );
                    Arrays.stream( logs ).mapToLong( name -> new File( txLogs.toFile(), name ).length() ).max().ifPresent( setSize );
                    txLogSize = setSize.txLogSize;
                }
            }
            else
            {
                throw new IllegalArgumentException( "Cannot determine size of transaction logs: " + txLogs + " is not a directory" );
            }
        }
        return txLogSize;
    }

    private static class TxSizeSetter implements LongConsumer
    {
        long txLogSize;
        final long count;

        TxSizeSetter( int count )
        {
            this.count = count;
        }

        public void accept( long size )
        {
            // After uploading a full database, the transaction logs were observed to be truncated to 10 files
            txLogSize = Math.min( 10, count ) * size;
        }
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
        private final Path path;
        private long size;

        public Source( Path path, long size )
        {
            this.path = path;
            this.size = size;
        }

        public Path path()
        {
            return path;
        }

        public long size()
        {
            return size;
        }

        protected void setSize( long newSize )
        {
            this.size = newSize;
        }

        long crc32Sum() throws IOException
        {
            CRC32 crc = new CRC32();
            try ( InputStream inputStream = new BufferedInputStream( Files.newInputStream( path ) ) )
            {
                int cnt;
                while ( (cnt = inputStream.read()) != -1 )
                {
                    crc.update( cnt );
                }
            }
            return crc.getValue();
        }

        @Override
        public int hashCode()
        {
            return path.hashCode() + 31 * (int) size;
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( obj instanceof Source )
            {
                Source other = (Source) obj;
                return path.equals( other.path ) && size == other.size;
            }
            else
            {
                return false;
            }
        }
    }

    public static String sizeText( long size )
    {
        return format( "%.1f GB", bytesToGibibytes( size ) );
    }

    public static double bytesToGibibytes( long sizeInBytes )
    {
        return sizeInBytes / (double) (1024 * 1024 * 1024);
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
         * @param size        database size
         * @param bearerToken token from successful {@link #authenticate(boolean, String, String, char[], boolean)} call.
         * @throws CommandFailedException if the database won't fit on the aura instance
         */
        void checkSize( boolean verbose, String consoleURL, long size, String bearerToken ) throws CommandFailedException;
    }

    public interface DumpCreator
    {
        Path dumpDatabase( String databaseName, Path targetDumpFile ) throws CommandFailedException;
    }
}
