/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.id.indexed;

import static java.util.Comparator.comparing;
import static org.neo4j.internal.helpers.Format.date;
import static org.neo4j.internal.id.indexed.IndexedIdGenerator.NO_MONITOR;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.internal.helpers.Args;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FlushableChannel;
import org.neo4j.io.fs.InputStreamReadableChannel;
import org.neo4j.io.fs.OutputStreamWritableChannel;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;

/**
 * Logs all monitor calls into a {@link FlushableChannel}.
 */
public class LoggingIndexedIdGeneratorMonitor implements IndexedIdGenerator.Monitor, Closeable {
    private static final String ARG_TOFILE = "tofile";
    private static final String ARG_FILTER = "filter";

    private static final IdFilter NO_FILTER = (id, numberOfIds) -> true;
    private static final Type[] TYPES = Type.values();
    static final int HEADER_SIZE = Byte.BYTES + Long.BYTES;

    private final FileSystemAbstraction fs;
    private final Path path;
    private final SystemNanoClock clock;
    private FlushableChannel channel;
    private final AtomicLong position = new AtomicLong();
    private final long rotationThreshold;
    private final long pruneThreshold;

    /**
     * Looks at feature toggle and instantiates a LoggingMonitor if enabled, otherwise a no-op monitor.
     */
    public static IndexedIdGenerator.Monitor defaultIdMonitor(FileSystemAbstraction fs, Path idFile, Config config) {
        if (config.get(GraphDatabaseInternalSettings.id_generator_log_enabled)) {
            return new LoggingIndexedIdGeneratorMonitor(
                    fs,
                    idFile.resolveSibling(idFile.getFileName() + ".log"),
                    Clocks.nanoClock(),
                    config.get(GraphDatabaseInternalSettings.id_generator_log_rotation_threshold),
                    ByteUnit.Byte,
                    config.get(GraphDatabaseInternalSettings.id_generator_log_prune_threshold)
                            .toMillis(),
                    TimeUnit.MILLISECONDS);
        }
        return NO_MONITOR;
    }

    LoggingIndexedIdGeneratorMonitor(
            FileSystemAbstraction fs,
            Path path,
            SystemNanoClock clock,
            long rotationThreshold,
            ByteUnit rotationThresholdUnit,
            long pruneThreshold,
            TimeUnit pruneThresholdUnit) {
        this.fs = fs;
        this.path = path;
        this.clock = clock;
        this.rotationThreshold = rotationThresholdUnit.toBytes(rotationThreshold);
        this.pruneThreshold = pruneThresholdUnit.toMillis(pruneThreshold);
        try {
            if (fs.fileExists(path)) {
                moveAwayFile();
            }
            this.channel = instantiateChannel();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public synchronized void opened(long highestWrittenId, long highId) {
        putTypeAndTwoIds(Type.OPENED, highestWrittenId, highId);
    }

    @Override
    public synchronized void allocatedFromHigh(long allocatedId, int numberOfIds) {
        putTypeAndId(Type.ALLOCATE_HIGH, allocatedId, numberOfIds);
    }

    @Override
    public synchronized void allocatedFromReused(long allocatedId, int numberOfIds) {
        putTypeAndId(Type.ALLOCATE_REUSED, allocatedId, numberOfIds);
    }

    @Override
    public synchronized void cached(long cachedId, int numberOfIds) {
        putTypeAndId(Type.CACHED, cachedId, numberOfIds);
    }

    @Override
    public synchronized void markedAsUsed(long markedId, int numberOfIds) {
        putTypeAndId(Type.MARK_USED, markedId, numberOfIds);
    }

    @Override
    public synchronized void markedAsDeleted(long markedId, int numberOfIds) {
        putTypeAndId(Type.MARK_DELETED, markedId, numberOfIds);
    }

    @Override
    public synchronized void markedAsFree(long markedId, int numberOfIds) {
        putTypeAndId(Type.MARK_FREE, markedId, numberOfIds);
    }

    @Override
    public synchronized void markedAsReserved(long markedId, int numberOfIds) {
        putTypeAndId(Type.MARK_RESERVED, markedId, numberOfIds);
    }

    @Override
    public synchronized void markedAsUnreserved(long markedId, int numberOfIds) {
        putTypeAndId(Type.MARK_UNRESERVED, markedId, numberOfIds);
    }

    @Override
    public synchronized void markSessionDone() {
        flushBuffer();
        checkRotateAndPrune();
    }

    @Override
    public synchronized void normalized(long idRange) {
        putTypeAndId(Type.NORMALIZED, idRange);
    }

    @Override
    public synchronized void bridged(long bridgedId) {
        putTypeAndId(Type.BRIDGED, bridgedId);
    }

    @Override
    public synchronized void checkpoint(long highestWrittenId, long highId) {
        putTypeAndTwoIds(Type.CHECKPOINT, highestWrittenId, highId);

        // Take the opportunity to also flush this log
        flushBuffer();
    }

    @Override
    public synchronized void clearingCache() {
        putTypeOnly(Type.CLEARING_CACHE);
    }

    @Override
    public synchronized void clearedCache() {
        putTypeOnly(Type.CLEARED_CACHE);
    }

    @Override
    public synchronized void skippedIdsAtHighId(long firstSkippedHighId, int numberOfIds) {
        putTypeAndId(Type.SKIPPED_HIGH, firstSkippedHighId, numberOfIds);
    }

    @Override
    public synchronized void skippedIdsAtAllocation(long firstWastedId, int numberOfIds) {
        putTypeAndId(Type.SKIPPED_WASTED, firstWastedId, numberOfIds);
    }

    @Override
    public synchronized void close() {
        putTypeOnly(Type.CLOSED);
        IOUtils.closeAllUnchecked(channel);
    }

    private void flushBuffer() {
        try {
            channel.prepareForFlush().flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void putEntryHeader(Type type) throws IOException {
        channel.put(type.id);
        channel.putLong(clock.millis());
    }

    private void checkRotateAndPrune() {
        if (position.longValue() >= rotationThreshold) {
            try {
                // Rotate
                flushBuffer();
                channel.close();
                moveAwayFile();
                position.set(0);
                channel = instantiateChannel();

                // Prune
                long time = clock.millis();
                long threshold = time - pruneThreshold;
                for (Path file : fs.listFiles(
                        path.getParent(), file -> file.getFileName().toString().startsWith(path.getFileName() + "-"))) {
                    if (millisOf(file) < threshold) {
                        fs.deleteFile(file);
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private void putTypeOnly(Type type) {
        try {
            putEntryHeader(type);
            position.addAndGet(HEADER_SIZE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void putTypeAndId(Type type, long id, int numberOfIds) {
        try {
            putEntryHeader(type);
            channel.putLong(id);
            channel.putInt(numberOfIds);
            position.addAndGet(HEADER_SIZE + Long.BYTES + Integer.BYTES);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void putTypeAndId(Type type, long id) {
        try {
            putEntryHeader(type);
            channel.putLong(id);
            position.addAndGet(HEADER_SIZE + Long.BYTES);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void putTypeAndTwoIds(Type type, long id1, long id2) {
        try {
            putEntryHeader(type);
            channel.putLong(id1);
            channel.putLong(id2);
            position.addAndGet(HEADER_SIZE + Long.BYTES * 2);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void moveAwayFile() throws IOException {
        Path to;
        do {

            to = timestampedFile();
        } while (fs.fileExists(to));
        fs.renameFile(path, to);
    }

    private FlushableChannel instantiateChannel() throws IOException {
        return new OutputStreamWritableChannel(fs.openAsOutputStream(path, false));
    }

    private Path timestampedFile() {
        return path.resolveSibling(path.getFileName() + "-" + clock.millis());
    }

    static long millisOf(Path file) {
        String name = file.getFileName().toString();
        int dashIndex = name.lastIndexOf('-');
        if (dashIndex == -1) {
            return Long.MAX_VALUE;
        }
        return Long.parseLong(name.substring(dashIndex + 1));
    }

    /**
     * Used for dumping contents of a log as text
     */
    public static void main(String[] args) throws IOException {
        Args arguments = Args.withFlags(ARG_TOFILE).parse(args);
        if (arguments.orphans().isEmpty()) {
            System.err.println("Please supply base name of log file");
            return;
        }

        Path path = Path.of(arguments.orphans().get(0));
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        String filterArg = arguments.get(ARG_FILTER, null);
        var filter = filterArg != null ? parseFilter(filterArg) : NO_FILTER;
        PrintStream out = System.out;
        boolean redirectsToFile = arguments.getBoolean(ARG_TOFILE);
        if (redirectsToFile) {
            Path outFile = path.resolveSibling(path.getFileName() + ".txt");
            System.out.println("Redirecting output to " + outFile);
            out = new PrintStream(new BufferedOutputStream(Files.newOutputStream(outFile)));
        }
        dump(fs, path, new Printer(out, filter));
        if (redirectsToFile) {
            out.close();
        }
    }

    static void dump(FileSystemAbstraction fs, Path baseFile, Dumper dumper) throws IOException {
        Path[] files = fs.listFiles(
                baseFile.getParent(),
                file -> file.getFileName()
                                .toString()
                                .startsWith(baseFile.getFileName().toString())
                        && !file.getFileName().toString().endsWith(".txt"));
        Arrays.sort(files, comparing(LoggingIndexedIdGeneratorMonitor::millisOf));
        for (Path file : files) {
            dumpFile(fs, file, dumper);
        }
    }

    private static void dumpFile(FileSystemAbstraction fs, Path path, Dumper dumper) throws IOException {
        dumper.path(path);
        try (var channel = new InputStreamReadableChannel(fs.openAsInputStream(path))) {
            while (true) {
                byte typeByte = channel.get();
                if (typeByte < 0 || typeByte >= TYPES.length) {
                    System.out.println("Unknown type " + typeByte);
                    continue;
                }

                Type type = TYPES[typeByte];
                long time = channel.getLong();
                switch (type) {
                    case CLEARING_CACHE, CLEARED_CACHE, CLOSED -> dumper.type(type, time);
                    case ALLOCATE_HIGH,
                            ALLOCATE_REUSED,
                            CACHED,
                            SKIPPED_HIGH,
                            SKIPPED_WASTED,
                            MARK_USED,
                            MARK_DELETED,
                            MARK_FREE,
                            MARK_RESERVED,
                            MARK_UNRESERVED -> dumper.typeAndId(type, time, channel.getLong(), channel.getInt());
                    case NORMALIZED, BRIDGED -> dumper.typeAndId(type, time, channel.getLong());
                    case OPENED, CHECKPOINT -> dumper.typeAndTwoIds(type, time, channel.getLong(), channel.getLong());
                    default -> System.out.println("Unknown type " + type);
                }
            }
        } catch (EOFException e) {
            // This is OK. we're done with this file
        }
    }

    private static IdFilter parseFilter(String arg) {
        String[] ids = arg.split(",");
        long[] result = new long[ids.length];
        for (int i = 0; i < ids.length; i++) {
            result[i] = Long.parseLong(ids[i]);
        }
        return new Filter(result);
    }

    public static class Filter implements IdFilter {
        private final long[] ids;

        public Filter(long... ids) {
            this.ids = ids;
        }

        @Override
        public boolean test(long id, int numberOfIds) {
            for (long testId : ids) {
                if (testId >= id && testId < id + numberOfIds) {
                    return true;
                }
            }
            return false;
        }
    }

    interface Dumper {
        void path(Path path);

        void type(Type type, long time);

        void typeAndId(Type type, long time, long id);

        void typeAndId(Type type, long time, long id, int numberOfIds);

        void typeAndTwoIds(Type type, long time, long id1, long id2);
    }

    interface IdFilter {
        boolean test(long id, int numberOfIds);
    }

    public static class Printer implements Dumper {
        private final PrintStream out;
        private final IdFilter filter;

        public Printer(PrintStream out, IdFilter filter) {
            this.out = out;
            this.filter = filter;
        }

        @Override
        public void path(Path path) {
            out.printf("=== %s ===%n", path.toAbsolutePath());
        }

        @Override
        public void type(Type type, long time) {
            out.printf("%s %s%n", date(time), type.shortName);
        }

        @Override
        public void typeAndId(Type type, long time, long id) {
            if (filter.test(id, 1)) {
                out.printf("%s %s [%d]%n", date(time), type.shortName, id);
            }
        }

        @Override
        public void typeAndId(Type type, long time, long id, int numberOfIds) {
            if (filter.test(id, numberOfIds)) {
                if (numberOfIds == 1) {
                    out.printf("%s %s [%d]%n", date(time), type.shortName, id);
                } else {
                    out.printf("%s %s [%d-%d]%n", date(time), type.shortName, id, id + numberOfIds - 1);
                }
            }
        }

        @Override
        public void typeAndTwoIds(Type type, long time, long id1, long id2) {
            out.printf("%s %s %d/%d%n", date(time), type.shortName, id1, id2);
        }
    }

    enum Type {
        OPENED("Opened"),
        CLOSED("Closed"),
        ALLOCATE_HIGH("AH"),
        ALLOCATE_REUSED("AR"),
        CACHED("CA"),
        MARK_USED("MI"),
        MARK_DELETED("MD"),
        MARK_FREE("MF"),
        MARK_RESERVED("MR"),
        MARK_UNRESERVED("MX"),
        NORMALIZED("NO"),
        BRIDGED("BR"),
        CHECKPOINT("Checkpoint"),
        CLEARING_CACHE("ClearCacheStart"),
        CLEARED_CACHE("ClearCacheEnd"),
        SKIPPED_HIGH("SH"),
        SKIPPED_WASTED("SW");

        final byte id;
        final String shortName;

        Type(String shortName) {
            this.id = (byte) ordinal();
            this.shortName = shortName;
        }
    }
}
