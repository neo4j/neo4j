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
package org.neo4j.index.internal.gbptree;

import static java.util.Comparator.comparing;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.neo4j.internal.helpers.Format.date;
import static org.neo4j.io.ByteUnit.mebiBytes;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongPredicate;
import org.neo4j.internal.helpers.Args;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FlushableChannel;
import org.neo4j.io.fs.InputStreamReadableChannel;
import org.neo4j.io.fs.OutputStreamWritableChannel;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;

class LoggingStructureWriteLog implements StructureWriteLog {
    private static final int ENTRY_HEADER_SIZE = Byte.BYTES + Long.BYTES * 3;

    private static final Function<Path, Path> PATH_FUNCTION =
            gbptreePath -> gbptreePath.resolveSibling(gbptreePath.getFileName() + ".slog");

    private final FileSystemAbstraction fs;
    private final Path path;
    private final SystemNanoClock clock;
    private FlushableChannel channel;
    private final AtomicLong position = new AtomicLong();
    private final long rotationThreshold;
    private final long pruneThreshold = DAYS.toMillis(1);
    private final AtomicLong nextSessionId = new AtomicLong();

    LoggingStructureWriteLog(FileSystemAbstraction fs, Path path, long rotationThreshold) {
        this.fs = fs;
        this.path = path;
        this.clock = Clocks.nanoClock();
        this.rotationThreshold = rotationThreshold;
        try {
            if (fs.fileExists(path)) {
                moveAwayFile();
            }
            this.channel = instantiateChannel();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static LoggingStructureWriteLog forGBPTree(FileSystemAbstraction fs, Path gbptreeFile) {
        return new LoggingStructureWriteLog(fs, PATH_FUNCTION.apply(gbptreeFile), mebiBytes(50));
    }

    @Override
    public Session newSession() {
        return new SessionImpl(nextSessionId.getAndIncrement());
    }

    @Override
    public synchronized void checkpoint(
            long previousStableGeneration, long newStableGeneration, long newUnstableGeneration) {
        writeEntry(Type.CHECKPOINT, -1, newStableGeneration, previousStableGeneration, newUnstableGeneration);
        checkRotation();
    }

    private void checkRotation() {
        if (position.longValue() >= rotationThreshold) {
            try {
                // Rotate
                channel.prepareForFlush().flush();
                channel.close();
                moveAwayFile();
                position.set(0);
                channel = instantiateChannel();

                // Prune
                var time = clock.millis();
                var threshold = time - pruneThreshold;
                for (var file : fs.listFiles(
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

    @Override
    public synchronized void close() {
        IOUtils.closeAllUnchecked(channel);
    }

    private void moveAwayFile() throws IOException {
        Path to;
        do {
            to = timestampedFile();
        } while (fs.fileExists(to));
        fs.renameFile(path, to);
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

    private FlushableChannel instantiateChannel() throws IOException {
        return new OutputStreamWritableChannel(fs.openAsOutputStream(path, false));
    }

    private void writeHeader(Type type, long sessionId, long generation) throws IOException {
        channel.put(type.type);
        channel.putLong(sessionId);
        channel.putLong(clock.millis());
        channel.putLong(generation);
    }

    private synchronized void writeEntry(Type type, long sessionId, long generation, long... ids) {
        try {
            writeHeader(type, sessionId, generation);
            for (var id : ids) {
                channel.putLong(id);
            }
            position.addAndGet(ENTRY_HEADER_SIZE + (Long.BYTES * ids.length));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private class SessionImpl implements Session {
        private final long sessionId;

        SessionImpl(long sessionId) {
            this.sessionId = sessionId;
        }

        @Override
        public void split(long generation, long parentId, long childId, long createdChildId) {
            writeEntry(Type.SPLIT, sessionId, generation, parentId, childId, createdChildId);
        }

        @Override
        public void merge(long generation, long parentId, long childId, long deletedChildId) {
            writeEntry(Type.MERGE, sessionId, generation, parentId, childId, deletedChildId);
        }

        @Override
        public void createSuccessor(long generation, long parentId, long oldId, long newId) {
            writeEntry(Type.SUCCESSOR, sessionId, generation, parentId, oldId, newId);
        }

        @Override
        public void addToFreelist(long generation, long id) {
            writeEntry(Type.FREELIST, sessionId, generation, id);
        }

        @Override
        public void growTree(long generation, long createdRootId) {
            writeEntry(Type.TREE_GROW, sessionId, generation, createdRootId);
        }

        @Override
        public void shrinkTree(long generation, long deletedRootId) {
            writeEntry(Type.TREE_SHRINK, sessionId, generation, deletedRootId);
        }
    }

    enum Type {
        SPLIT((byte) 0),
        MERGE((byte) 1),
        SUCCESSOR((byte) 2),
        FREELIST((byte) 3),
        TREE_GROW((byte) 4),
        TREE_SHRINK((byte) 5),
        CHECKPOINT((byte) 6);

        private final byte type;

        Type(byte value) {
            this.type = value;
        }
    }

    static final Type[] TYPES = Type.values();

    interface Events {
        void split(long timeMillis, long sessionId, long generation, long parentId, long childId, long createdChildId);

        void merge(long timeMillis, long sessionId, long generation, long parentId, long childId, long deletedChildId);

        void createSuccessor(long timeMillis, long sessionId, long generation, long parentId, long oldId, long newId);

        void addToFreeList(long timeMillis, long sessionId, long generation, long id);

        void checkpoint(
                long timeMillis, long previousStableGeneration, long newStableGeneration, long newUnstableGeneration);

        void growTree(long timeMillis, long sessionId, long generation, long createdRootId);

        void shrinkTree(long timeMillis, long sessionId, long generation, long deletedRootId);
    }

    public static void read(FileSystemAbstraction fs, Path baseFile, Events events) throws IOException {
        var files = fs.listFiles(
                baseFile.getParent(),
                file -> file.getFileName()
                                .toString()
                                .startsWith(baseFile.getFileName().toString())
                        && !file.getFileName().toString().endsWith(".txt"));
        Arrays.sort(files, comparing(LoggingStructureWriteLog::millisOf));
        for (var file : files) {
            readFile(fs, file, events);
        }
    }

    private static void readFile(FileSystemAbstraction fs, Path path, Events events) throws IOException {
        try (var channel = new InputStreamReadableChannel(fs.openAsInputStream(path))) {
            while (true) {
                var typeByte = channel.get();
                if (typeByte < 0 || typeByte >= TYPES.length) {
                    System.out.println("Unknown type " + typeByte);
                    continue;
                }

                var type = TYPES[typeByte];
                var sessionId = channel.getLong();
                var timeMillis = channel.getLong();
                var generation = channel.getLong();
                switch (type) {
                    case SPLIT -> events.split(
                            timeMillis, sessionId, generation, channel.getLong(), channel.getLong(), channel.getLong());
                    case MERGE -> events.merge(
                            timeMillis, sessionId, generation, channel.getLong(), channel.getLong(), channel.getLong());
                    case SUCCESSOR -> events.createSuccessor(
                            timeMillis, sessionId, generation, channel.getLong(), channel.getLong(), channel.getLong());
                    case FREELIST -> events.addToFreeList(timeMillis, sessionId, generation, channel.getLong());
                    case TREE_GROW -> events.growTree(timeMillis, sessionId, generation, channel.getLong());
                    case TREE_SHRINK -> events.shrinkTree(timeMillis, sessionId, generation, channel.getLong());
                    case CHECKPOINT -> events.checkpoint(timeMillis, channel.getLong(), generation, channel.getLong());
                    default -> throw new UnsupportedOperationException(type.toString());
                }
            }
        } catch (EOFException e) {
            // This is OK. we're done with this file
        }
    }

    private static class Dumper implements Events {
        private final LongPredicate idFilter;

        Dumper(LongPredicate idFilter) {
            this.idFilter = idFilter;
        }

        @Override
        public void split(
                long timeMillis, long sessionId, long generation, long parentId, long childId, long createdChildId) {
            if (idFilter.test(parentId) || idFilter.test(createdChildId)) {
                System.out.printf(
                        "%s %d %d SP %d -> %d -> %d%n",
                        date(timeMillis), sessionId, generation, parentId, childId, createdChildId);
            }
        }

        @Override
        public void merge(
                long timeMillis, long sessionId, long generation, long parentId, long childId, long deletedChildId) {
            if (idFilter.test(parentId) || idFilter.test(deletedChildId)) {
                System.out.printf(
                        "%s %d %d ME %d -> %d -X-> %d%n",
                        date(timeMillis), sessionId, generation, parentId, childId, deletedChildId);
            }
        }

        @Override
        public void createSuccessor(
                long timeMillis, long sessionId, long generation, long parentId, long oldId, long newId) {
            if (idFilter.test(oldId) || idFilter.test(newId)) {
                System.out.printf(
                        "%s %d %d SU %d -> %d -> %d%n",
                        date(timeMillis), sessionId, generation, parentId, oldId, newId);
            }
        }

        @Override
        public void addToFreeList(long timeMillis, long sessionId, long generation, long id) {
            if (idFilter.test(id)) {
                System.out.printf("%s %d %d FR %d%n", date(timeMillis), sessionId, generation, id);
            }
        }

        @Override
        public void growTree(long timeMillis, long sessionId, long generation, long createdRootId) {
            if (idFilter.test(createdRootId)) {
                System.out.printf("%s %d %d GT %d%n", date(timeMillis), sessionId, generation, createdRootId);
            }
        }

        @Override
        public void shrinkTree(long timeMillis, long sessionId, long generation, long deletedRootId) {
            if (idFilter.test(deletedRootId)) {
                System.out.printf("%s %d %d ST %d%n", date(timeMillis), sessionId, generation, deletedRootId);
            }
        }

        @Override
        public void checkpoint(
                long timeMillis, long previousStableGeneration, long newStableGeneration, long newUnstableGeneration) {
            System.out.printf(
                    "%s CP %d %d %d%n",
                    date(timeMillis), previousStableGeneration, newStableGeneration, newUnstableGeneration);
        }
    }

    public static void main(String[] args) throws IOException {
        var arguments = Args.parse(args);
        var basePath = Path.of(arguments.orphans().get(0));
        var rawFilter = arguments.get("filter", null);
        LongPredicate idFilter = id -> true;
        if (rawFilter != null) {
            var idToFilterOn = Long.parseLong(rawFilter);
            idFilter = id -> id == idToFilterOn;
        }
        try (var fs = new DefaultFileSystemAbstraction()) {
            read(fs, basePath, new Dumper(idFilter));
        }
    }
}
