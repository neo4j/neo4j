/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.export;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.neo4j.cli.AbstractAdminCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.Converters;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.dbms.archive.Dumper;
import org.neo4j.dbms.archive.Loader;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.memory.NativeScopedBuffer;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.memory.EmptyMemoryTracker;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(
        name = "upload",
        description = "Push a local database to a Neo4j Aura instance. "
                + "The target location is a Neo4j Aura Bolt URI. If Neo4j Cloud username and password are not provided "
                + "either as a command option or as an environment variable, they will be requested interactively ")
public class UploadCommand extends AbstractAdminCommand {
    private static final long CRC32_BUFFER_SIZE = ByteUnit.mebiBytes(4);
    private static final String DEV_MODE_VAR_NAME = "P2C_DEV_MODE";
    private final Copier copier;
    private final PushToCloudConsole cons;

    @Parameters(
            paramLabel = "<database>",
            description = "Name of the database that should be uploaded. The name is used to select a dump file "
                    + "which is expected to be named <database>.dump.",
            converter = Converters.DatabaseNameConverter.class)
    private NormalizedDatabaseName database;

    @Option(
            names = "--from-path",
            paramLabel = "<path>",
            description =
                    "'/path/to/directory-containing-dump' Path to a directory containing a database dump to upload.",
            required = true)
    private Path dumpDirectory;

    @Option(
            names = "--to-uri",
            paramLabel = "<uri>",
            arity = "1",
            required = true,
            description = "'neo4j://mydatabaseid.databases.neo4j.io' Bolt URI of target database")
    private String boltURI;

    @Option(
            names = "--to-user",
            defaultValue = "${NEO4J_USERNAME}",
            description =
                    "Username of the target database to push this database to. Prompt will ask for username if not provided. "
                            + "Alternatively NEO4J_USERNAME environment variable can be used.")
    private String username;

    @Option(
            names = "--to-password",
            defaultValue = "${NEO4J_PASSWORD}",
            description =
                    "Password of the target database to push this database to. Prompt will ask for password if not provided. "
                            + "Alternatively NEO4J_PASSWORD environment variable can be used.")
    private String password;

    @Option(
            names = "--overwrite-destination",
            arity = "0..1",
            paramLabel = "true|false",
            fallbackValue = "true",
            showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
            description = "Overwrite the data in the target database.")
    private boolean overwrite;

    @Option(
            names = "--to",
            paramLabel = "<destination>",
            description = "The destination for the upload.",
            defaultValue = "aura",
            showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
    private String to;

    public UploadCommand(ExecutionContext ctx, Copier copier, PushToCloudConsole cons) {
        super(ctx);
        this.copier = copier;
        this.cons = cons;
    }

    public static long readSizeFromDumpMetaData(ExecutionContext ctx, Path dump) {
        Loader.DumpMetaData metaData;
        try {
            final var fileSystem = ctx.fs();
            metaData = new Loader(fileSystem, System.out).getMetaData(() -> fileSystem.openAsInputStream(dump));
        } catch (IOException e) {
            throw new CommandFailedException("Unable to check size of database dump.", e);
        }
        return Long.parseLong(metaData.byteCount());
    }

    public long readSizeFromTarMetaData(ExecutionContext ctx, Path tar, String dbName) {
        final var fileSystem = ctx.fs();

        try (TarArchiveInputStream tais = new TarArchiveInputStream(maybeGzipped(tar, fileSystem))) {
            Loader.DumpMetaData metaData;
            TarArchiveEntry entry;
            while ((entry = tais.getNextTarEntry()) != null) {
                if (entry.getName().endsWith(dbName + ".dump")) {

                    metaData = new Loader(fileSystem, System.out).getMetaData(() -> tais);
                    return Long.parseLong(metaData.byteCount());
                }
            }
            throw new CommandFailedException(
                    String.format("TAR file %s does not contain dump for  database %s", tar, dbName));
        } catch (IOException e) {
            throw new CommandFailedException("Unable to check size of tar dump database.", e);
        }
    }

    private InputStream maybeGzipped(Path tar, final FileSystemAbstraction fileSystem) throws IOException {
        try {
            return new GZIPInputStream(fileSystem.openAsInputStream(tar));
        } catch (ZipException e) {
            return fileSystem.openAsInputStream(tar);
        }
    }

    public static String sizeText(long size) {
        return format("%.1f GB", bytesToGibibytes(size));
    }

    public static double bytesToGibibytes(long sizeInBytes) {
        return sizeInBytes / (double) (1024 * 1024 * 1024);
    }

    @Override
    public void execute() {
        try {
            if (!"aura".equals(to)) {
                throw new CommandFailedException(
                        format("'%s' is not a supported destination. Supported destinations are: 'aura'", to));
            }

            if (isBlank(username)) {
                if (isBlank(username = cons.readLine("%s", "Neo4j aura username (default: neo4j):"))) {
                    username = "neo4j";
                }
            }
            char[] pass;
            if (isBlank(password)) {
                if ((pass = cons.readPassword("Neo4j aura password for %s:", username)).length == 0) {
                    throw new CommandFailedException(
                            "Please supply a password, either by '--to-password' parameter, 'NEO4J_PASSWORD' environment variable, or prompt");
                }
            } else {
                pass = password.toCharArray();
            }

            boolean devMode = cons.readDevMode(DEV_MODE_VAR_NAME);

            String consoleURL = buildConsoleURI(boltURI, devMode);
            String bearerToken = copier.authenticate(verbose, consoleURL, username, pass, overwrite);

            Uploader uploader = makeDumpUploader(dumpDirectory, database.name());
            uploader.process(consoleURL, bearerToken);
        } catch (Exception e) {
            throw new CommandFailedException(e.getMessage());
        }
    }

    private void verbose(String format, Object... args) {
        if (verbose) {
            ctx.out().printf(format, args);
        }
    }

    String buildConsoleURI(String boltURI, boolean devMode) throws CommandFailedException {
        UrlMatcher matchers[] = devMode
                ? new UrlMatcher[] {new DevMatcher(), new ProdMatcher(), new PrivMatcher()}
                : new UrlMatcher[] {new ProdMatcher()};

        return Arrays.stream(matchers)
                .filter(m -> m.match(boltURI))
                .findFirst()
                .orElseThrow(
                        () -> new CommandFailedException(
                                "Invalid Bolt URI '" + boltURI
                                        + "'. Please note push-to-cloud does not currently support private link bolt connections. Please raise a Support ticket if you need to use push-to-cloud and you have public traffic disabled."))
                .consoleUrl();
    }

    abstract static class UrlMatcher {
        // A boltURI looks something like this:
        //
        //   bolt+routing://mydbid-myenvironment.databases.neo4j.io
        //                  <─┬──><──────┬─────>
        //                    │          └──────── environment
        //                    └─────────────────── database id
        //
        // When running in a dev environment it can also be of the form
        // bolt+routing://mydbid-myenv.databases.neo4j-myenv.io
        // Constructing a console URI takes elements from the bolt URI and places them inside this URI:
        //
        //   https://console<environment>.neo4j.io/v1/databases/<database id>
        //
        // Examples:
        //
        //   bolt+routing://rogue.databases.neo4j.io  --> https://console.neo4j.io/v1/databases/rogue
        //   bolt+routing://rogue-mattias.databases.neo4j.io  --> https://console-mattias.neo4j.io/v1/databases/rogue
        //   bolt+routing://rogue-myenv.databases.neo4j-myenv.io  -->
        // https://console-myenv.neo4j-myenv.io/v1/databases/rogue
        //
        // When PrivateLink is enabled, the URL scheme is a little different:
        //
        //   bolt+routing://mydbid.myenv-orch-0003.neo4j.io"
        //                  <─┬──> <─┬─>
        //                    │      └──────────── environment
        //                    └─────────────────── database id

        protected Matcher matcher;
        protected String url;

        protected abstract Pattern pattern();

        public abstract String consoleUrl();

        public boolean match(String url) {
            this.url = url;
            matcher = pattern().matcher(url);
            return matcher.matches();
        }
    }

    static class ProdMatcher extends UrlMatcher {
        @Override
        protected Pattern pattern() {
            return Pattern.compile(
                    "(?:bolt(?:\\+routing)?|neo4j(?:\\+s|\\+ssc)?)://([^-]+)(-(.+))?.databases.neo4j.io$");
        }

        @Override
        public String consoleUrl() {
            String databaseId = matcher.group(1);
            String environment = matcher.group(2);

            return String.format(
                    "https://console%s.neo4j.io/v1/databases/%s", environment == null ? "" : environment, databaseId);
        }
    }

    static class DevMatcher extends UrlMatcher {
        @Override
        protected Pattern pattern() {
            return Pattern.compile(
                    "(?:bolt(?:\\+routing)?|neo4j(?:\\+s|\\+ssc)?)://([^-]+)(-(.+))?.databases.neo4j(-(.+))?.io$");
        }

        @Override
        public String consoleUrl() {
            String databaseId = matcher.group(1);
            String environment = matcher.group(2);
            String domain = "";

            if (environment == null) {
                throw new CommandFailedException(
                        "Expected to find an environment running in dev mode in bolt URI: " + url);
            }
            if (matcher.groupCount() == 5) {
                domain = matcher.group(4);
            }

            return String.format("https://console%s.neo4j%s.io/v1/databases/%s", environment, domain, databaseId);
        }
    }

    static class PrivMatcher extends UrlMatcher {
        @Override
        protected Pattern pattern() {
            return Pattern.compile(
                    "(?:bolt(?:\\+routing)?|neo4j(?:\\+s|\\+ssc)?)://([a-zA-Z0-9]+)\\.(\\S+)-orch-(\\d+).neo4j(-\\S+)?.io$");
        }

        @Override
        public String consoleUrl() {
            String databaseId = matcher.group(1);
            String environment = matcher.group(2);
            String domain = "";

            switch (environment) {
                case "production" -> environment = "";
                case "staging", "prestaging" -> environment = "-" + environment;
                default -> {
                    environment = "-" + environment;
                    if (matcher.group(4) == null) {
                        throw new CommandFailedException("Invalid Bolt URI '" + url + "'");
                    }
                    domain = matcher.group(4);
                }
            }

            return String.format("https://console%s.neo4j%s.io/v1/databases/%s", environment, domain, databaseId);
        }
    }

    public DumpUploader makeDumpUploader(Path dump, String database) {
        if (!ctx.fs().isDirectory(dump)) {
            throw new CommandFailedException(format("The provided source directory '%s' doesn't exist", dump));
        }
        Path dumpFile = dump.resolve(database + Dumper.DUMP_EXTENSION);
        if (!ctx.fs().fileExists(dumpFile)) {
            Path tarFile = dump.resolve(database + Dumper.TAR_EXTENSION);
            if (!ctx.fs().fileExists(tarFile)) {
                throw new CommandFailedException(format(
                        "Dump files '%s' or '%s' do not exist", dumpFile.toAbsolutePath(), tarFile.toAbsolutePath()));
            }
            dumpFile = tarFile;
        }
        return new DumpUploader(new Source(ctx.fs(), dumpFile, dumpSize(dumpFile, database)));
    }

    private long dumpSize(Path dump, String database) {
        long sizeInBytes;
        if (dump.getFileName().toString().endsWith(".dump")) {
            sizeInBytes = readSizeFromDumpMetaData(ctx, dump);
        } else {
            sizeInBytes = readSizeFromTarMetaData(ctx, dump, database);
        }
        verbose("Determined DumpSize=%d bytes from dump at %s\n", sizeInBytes, dump);
        return sizeInBytes;
    }

    public interface Copier {
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
        String authenticate(
                boolean verbose, String consoleURL, String username, char[] password, boolean consentConfirmed)
                throws CommandFailedException;

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
        void copy(
                boolean verbose,
                String consoleURL,
                String boltUri,
                Source source,
                boolean deleteSourceAfterImport,
                String bearerToken)
                throws CommandFailedException;

        /**
         * @param verbose     whether or not to print verbose debug messages/statuses
         * @param consoleURL  console URI to target.
         * @param size        database size
         * @param bearerToken token from successful {@link #authenticate(boolean, String, String, char[], boolean)} call.
         * @throws CommandFailedException if the database won't fit on the aura instance
         */
        void checkSize(boolean verbose, String consoleURL, long size, String bearerToken) throws CommandFailedException;
    }

    abstract static class Uploader {
        protected final Source source;

        Uploader(Source source) {
            this.source = source;
        }

        long size() {
            return source.size();
        }

        abstract void process(String consoleURL, String bearerToken);
    }

    public record Source(FileSystemAbstraction fs, Path path, long size) {
        long crc32Sum() throws IOException {
            CRC32 crc = new CRC32();
            try (var channel = fs.read(path);
                    var buffer = new NativeScopedBuffer(
                            CRC32_BUFFER_SIZE, ByteOrder.LITTLE_ENDIAN, EmptyMemoryTracker.INSTANCE)) {
                var byteBuffer = buffer.getBuffer();
                while ((channel.read(byteBuffer)) != -1) {
                    byteBuffer.flip();
                    crc.update(byteBuffer);
                    byteBuffer.clear();
                }
            }
            return crc.getValue();
        }
    }

    class DumpUploader extends Uploader {
        DumpUploader(Source source) {
            super(source);
        }

        @Override
        void process(String consoleURL, String bearerToken) {
            // Check size of dump (reading actual database size from dump header)
            verbose("Checking database size %s fits at %s\n", sizeText(size()), consoleURL);
            copier.checkSize(verbose, consoleURL, size(), bearerToken);

            // Upload dumpFile
            verbose("Uploading data of %s to %s\n", sizeText(size()), consoleURL);
            copier.copy(verbose, consoleURL, boltURI, source, false, bearerToken);
        }
    }
}
