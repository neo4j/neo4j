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
package org.neo4j.io.fs;

import static java.lang.String.format;
import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.FileVisitResult.SKIP_SUBTREE;
import static java.nio.file.Files.createDirectory;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.isDirectory;
import static java.nio.file.Files.notExists;
import static java.nio.file.Files.walkFileTree;
import static java.nio.file.StandardCopyOption.COPY_ATTRIBUTES;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Collections.singleton;
import static java.util.Objects.requireNonNull;
import static org.neo4j.function.Predicates.alwaysTrue;
import static org.neo4j.util.Preconditions.checkArgument;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.SystemUtils;
import org.neo4j.io.fs.DefaultFileSystemAbstraction.NativeByteBufferOutputStream;

/**
 * Set of utility methods to work with {@link Path} using the {@link DefaultFileSystemAbstraction default file system}.
 * This class is used by {@link DefaultFileSystemAbstraction} and its methods should not take {@link FileSystemAbstraction} as a parameter.
 * Consider using {@link FileSystemUtils} when a helper method needs to work with different file systems.
 *
 * @see FileSystemUtils
 */
public final class FileUtils {
    private static final int NUMBER_OF_RETRIES = 5;

    private FileUtils() {
        throw new AssertionError();
    }

    /**
     * For the lazy people out there that don't know if they are working on a file or a directory.
     * If the path points to a directory that will be deleted recursively.
     * @param path a file or a directory
     * @throws IOException if an I/O error occurs.
     */
    public static void delete(Path path) throws IOException {
        if (Files.isDirectory(path)) {
            deleteDirectory(path);
        } else {
            deleteFile(path);
        }
    }

    /**
     * Delete a directory recursively.
     * @param path directory to delete.
     * @throws IOException if an I/O error occurs.
     */
    public static void deleteDirectory(Path path) throws IOException {
        deleteDirectory(path, alwaysTrue());
    }

    /**
     * Delete a directory recursively with a filter.
     * @param path directory to traverse.
     * @param removeFilePredicate filter for files to remove.
     * @throws IOException if an I/O error occurs.
     */
    public static void deleteDirectory(Path path, Predicate<Path> removeFilePredicate) throws IOException {
        if (notExists(path)) {
            return;
        }
        if (!isDirectory(path)) {
            throw new NotDirectoryException(path.toString());
        }
        windowsSafeIOOperation(() -> walkFileTree(path, new DeletingFileVisitor(removeFilePredicate)));
    }

    /**
     * Delete a file or an empty directory.
     * @param file to delete.
     * @throws IOException if an I/O error occurs.
     */
    public static void deleteFile(Path file) throws IOException {
        if (notExists(file)) {
            return;
        }
        if (isDirectory(file) && !isDirectoryEmpty(file)) {
            throw new DirectoryNotEmptyException(file.toString());
        }
        windowsSafeIOOperation(() -> Files.delete(file));
    }

    public static long blockSize(Path file) throws IOException {
        requireNonNull(file);
        var path = file;
        while (path != null && !exists(path)) {
            path = path.getParent();
        }
        if (path == null) {
            throw new IOException("Fail to determine block size for file: " + file);
        }
        return Files.getFileStore(path).getBlockSize();
    }

    /**
     * Utility method that moves a file from its current location to the
     * new target location. If rename fails (for example if the target is
     * another disk) a copy/delete will be performed instead.
     *
     * @param toMove The file to move.
     * @param target Target directory to move to.
     * @throws IOException if an IO error occurs.
     */
    public static void moveFile(Path toMove, Path target) throws IOException {
        if (notExists(toMove)) {
            throw new NoSuchFileException(toMove.toString());
        }
        if (exists(target)) {
            throw new FileAlreadyExistsException(target.toString());
        }

        try {
            Files.move(toMove, target);
        } catch (IOException e) {
            if (isDirectory(toMove)) {
                Files.createDirectories(target);
                copyDirectory(toMove, target);
                deleteDirectory(toMove);
            } else {
                copyFile(toMove, target);
                deleteFile(toMove);
            }
        }
    }

    /**
     * Utility method that moves a file from its current location to the
     * provided target directory. If rename fails (for example if the target is
     * another disk) a copy/delete will be performed instead.
     *
     * @param toMove The File object to move.
     * @param targetDirectory the destination directory
     * @return the new file, null iff the move was unsuccessful
     * @throws IOException if an IO error occurs.
     */
    public static Path moveFileToDirectory(Path toMove, Path targetDirectory) throws IOException {
        if (notExists(targetDirectory)) {
            Files.createDirectories(targetDirectory);
        }
        if (!isDirectory(targetDirectory)) {
            throw new NotDirectoryException(targetDirectory.toString());
        }

        Path target = targetDirectory.resolve(toMove.getFileName());
        moveFile(toMove, target);
        return target;
    }

    /**
     * Utility method that copy a file from its current location to the
     * provided target directory.
     *
     * @param file file that needs to be copied.
     * @param targetDirectory the destination directory
     * @throws IOException if an IO error occurs.
     */
    public static void copyFileToDirectory(Path file, Path targetDirectory) throws IOException {
        if (notExists(targetDirectory)) {
            Files.createDirectories(targetDirectory);
        }
        if (!isDirectory(targetDirectory)) {
            throw new NotDirectoryException(targetDirectory.toString());
        }

        Path target = targetDirectory.resolve(file.getFileName());
        copyFile(file, target);
    }

    public static void truncateFile(Path file, long position) throws IOException {
        try (FileChannel channel = FileChannel.open(file, READ, WRITE)) {
            windowsSafeIOOperation(() -> channel.truncate(position));
        }
    }

    /*
     * See http://bugs.java.com/bugdatabase/view_bug.do?bug_id=4715154.
     */
    private static void waitAndThenTriggerGC() {
        try {
            Thread.sleep(500);
        } catch (InterruptedException ignored) {
        } // ok
        System.gc();
    }

    public static String fixSeparatorsInPath(String path) {
        String fileSeparator = System.getProperty("file.separator");
        if ("\\".equals(fileSeparator)) {
            path = path.replace('/', '\\');
        } else if ("/".equals(fileSeparator)) {
            path = path.replace('\\', '/');
        }
        return path;
    }

    public static void copyFile(Path srcFile, Path dstFile) throws IOException {
        copyFile(srcFile, dstFile, StandardCopyOption.REPLACE_EXISTING);
    }

    public static void copyFile(Path srcFile, Path dstFile, CopyOption... copyOptions) throws IOException {
        Files.createDirectories(dstFile.getParent());
        Files.copy(srcFile, dstFile, copyOptions);
    }

    public static void copyDirectory(Path from, Path to) throws IOException {
        copyDirectory(from, to, alwaysTrue());
    }

    public static void copyDirectory(Path from, Path to, Predicate<Path> filter) throws IOException {
        requireNonNull(from);
        requireNonNull(to);
        checkArgument(from.isAbsolute(), "From directory must be absolute");
        checkArgument(to.isAbsolute(), "To directory must be absolute");
        checkArgument(isDirectory(from), "From is not a directory");
        checkArgument(!from.normalize().equals(to.normalize()), "From and to directories are the same");

        if (notExists(to.getParent())) {
            Files.createDirectories(to.getParent());
        }
        walkFileTree(from, new CopyingFileVisitor(from, to, filter, REPLACE_EXISTING, COPY_ATTRIBUTES));
    }

    /**
     * Resolve toDir against fileToMove relativized against fromDir, resulting in a path denoting the location of
     * fileToMove after being moved fromDir toDir.
     * <p>
     * NOTE: This that this does not perform the move, it only calculates the new file name.
     * <p>
     * Throws {@link IllegalArgumentException} is fileToMove is not a sub path to fromDir.
     *
     * @param fromDir Path denoting current parent directory for fileToMove
     * @param toDir Path denoting location for fileToMove after move
     * @param fileToMove Path denoting current location for fileToMove
     * @return {@link Path} denoting new abstract path for file after move.
     */
    public static Path pathToFileAfterMove(Path fromDir, Path toDir, Path fileToMove) {
        // File to move must be true sub path to from dir
        if (!fileToMove.startsWith(fromDir) || fileToMove.equals(fromDir)) {
            throw new IllegalArgumentException("File " + fileToMove + " is not a sub path to dir " + fromDir);
        }

        return toDir.resolve(fromDir.relativize(fileToMove));
    }

    public interface Operation {
        void perform() throws IOException;
    }

    public static void windowsSafeIOOperation(Operation operation) throws IOException {
        IOException storedIoe = null;
        for (int i = 0; i < NUMBER_OF_RETRIES; i++) {
            try {
                operation.perform();
                return;
            } catch (IOException e) {
                storedIoe = e;
                waitAndThenTriggerGC();
            }
        }
        throw requireNonNull(storedIoe);
    }

    /**
     * Canonical file resolution on windows does not resolve links.
     * Real paths on windows can be resolved only using {@link Path#toRealPath(LinkOption...)}, but file should exist in that case.
     * We will try to do as much as possible and will try to use {@link Path#toRealPath(LinkOption...)} when file exist and will fallback to only
     * use {@link Path#normalize()} if file does not exist.
     * see JDK-8003887 for details
     * @param file - file to resolve canonical representation
     * @return canonical file representation.
     */
    public static Path getCanonicalFile(Path file) {
        try {
            return exists(file) ? file.toRealPath().normalize() : file.normalize();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void writeAll(FileChannel channel, ByteBuffer src, long position) throws IOException {
        long filePosition = position;
        long expectedEndPosition = filePosition + src.limit() - src.position();
        int bytesWritten;
        while ((filePosition += bytesWritten = channel.write(src, filePosition)) < expectedEndPosition) {
            if (bytesWritten <= 0) {
                throw new IOException("Unable to write to disk, reported bytes written was " + bytesWritten);
            }
        }
    }

    public static void writeAll(FileChannel channel, ByteBuffer src) throws IOException {
        long bytesToWrite = src.limit() - src.position();
        int bytesWritten;
        while ((bytesToWrite -= bytesWritten = channel.write(src)) > 0) {
            if (bytesWritten <= 0) {
                throw new IOException("Unable to write to disk, reported bytes written was " + bytesWritten);
            }
        }
    }

    /**
     * Get type of file store where provided file is located.
     * @param path file to get file store type for.
     * @return name of file store or "Unknown file store type: " + exception message,
     *         in case if exception occur during file store type retrieval.
     */
    public static String getFileStoreType(Path path) {
        try {
            return Files.getFileStore(path).type();
        } catch (IOException e) {
            return "Unknown file store type: " + e.getMessage();
        }
    }

    public static void tryForceDirectory(Path directory) throws IOException {
        if (notExists(directory)) {
            return;
        } else if (!isDirectory(directory)) {
            throw new NotDirectoryException(
                    format("The path %s must refer to a directory!", directory.toAbsolutePath()));
        }

        if (SystemUtils.IS_OS_WINDOWS) {
            // Windows doesn't allow us to open a FileChannel against a directory for reading, so we can't attempt to
            // "fsync" there
            return;
        }

        // Attempts to fsync the directory, guaranting e.g. file creation/deletion/rename events are durable
        // See http://mail.openjdk.java.net/pipermail/nio-dev/2015-May/003140.html
        // See also https://github.com/apache/lucene-solr/commit/7bea628bf3961a10581833935e4c1b61ad708c5c
        try (FileChannel directoryChannel = FileChannel.open(directory, singleton(READ))) {
            directoryChannel.force(true);
        }
    }

    public static boolean isDirectoryEmpty(Path directory) throws IOException {
        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(directory)) {
            return !dirStream.iterator().hasNext();
        }
    }

    /**
     * List {@link Path}s in a directory the same way as {@link File#listFiles()} where {@link IOException}s are ignored.
     *
     * @param dir path to the directory to list files in.
     * @return an array of paths. The array will be empty if the directory is empty. Returns {@code null} if the directory does not denote an actual
     * directory, or if an I/O error occurs.
     */
    public static Path[] listPaths(Path dir) {
        try {
            try (Stream<Path> list = Files.list(dir)) {
                return list.toArray(Path[]::new);
            }
        } catch (IOException ignored) {
            return null; // Preserve behaviour of File.listFiles()
        }
    }

    /**
     * Wrap the {@link StoreFileChannel} for the provider path as an {@link OutputStream}
     * @param path the path to write to
     * @param storeChannelProvider factory for creating the store channel
     * @param options the options to use when creating the channel
     * @return the output stream
     * @throws IOException if unable to open the channel
     */
    public static OutputStream toBufferedStream(
            Path path, Function<FileChannel, StoreFileChannel> storeChannelProvider, Set<OpenOption> options)
            throws IOException {
        FileChannel channel = FileChannel.open(path, options);
        StoreFileChannel fileChannel = storeChannelProvider.apply(channel);
        fileChannel.tryMakeUninterruptible();
        return new BufferedOutputStream(new NativeByteBufferOutputStream(fileChannel));
    }

    private static class DeletingFileVisitor extends SimpleFileVisitor<Path> {
        private final Predicate<Path> removeFilePredicate;
        private int skippedFiles;

        DeletingFileVisitor(Predicate<Path> removeFilePredicate) {
            this.removeFilePredicate = removeFilePredicate;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            if (removeFilePredicate.test(file)) {
                Files.delete(file);
            } else {
                skippedFiles++;
            }
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException e) throws IOException {
            if (e != null) {
                throw e;
            }
            try {
                if (skippedFiles == 0 || isDirectoryEmpty(dir)) {
                    Files.delete(dir);
                }
                return FileVisitResult.CONTINUE;
            } catch (DirectoryNotEmptyException notEmpty) {
                String reason = notEmptyReason(dir, notEmpty);
                throw new IOException(notEmpty.getMessage() + ": " + reason, notEmpty);
            }
        }

        private static String notEmptyReason(Path dir, DirectoryNotEmptyException notEmpty) {
            try (Stream<Path> list = Files.list(dir)) {
                return list.map(p -> String.valueOf(p.getFileName())).collect(Collectors.joining("', '", "'", "'."));
            } catch (Exception e) {
                notEmpty.addSuppressed(e);
                return "(could not list directory: " + e.getMessage() + ")";
            }
        }
    }

    private static class CopyingFileVisitor extends SimpleFileVisitor<Path> {
        private final Path from;
        private final Path to;
        private final Predicate<Path> filter;
        private final CopyOption[] copyOption;
        private final Set<Path> copiedPathsInDestination = new HashSet<>();

        CopyingFileVisitor(Path from, Path to, Predicate<Path> filter, CopyOption... copyOption) {
            this.from = from.normalize();
            this.to = to.normalize();
            this.filter = filter;
            this.copyOption = copyOption;
        }

        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
            if (!from.equals(dir) && !filter.test(dir)) {
                return SKIP_SUBTREE;
            }

            if (copiedPathsInDestination.contains(dir)) {
                return SKIP_SUBTREE;
            }

            Path target = to.resolve(from.relativize(dir));
            if (!exists(target)) {
                createDirectory(target);
                if (isInDestination(target)) {
                    copiedPathsInDestination.add(target);
                }
            }
            return CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            if (!filter.test(file)) {
                return CONTINUE;
            }
            if (!copiedPathsInDestination.contains(file)) {
                Path target = to.resolve(from.relativize(file));
                Files.copy(file, target, copyOption);
                if (isInDestination(target)) {
                    copiedPathsInDestination.add(target);
                }
            }
            return CONTINUE;
        }

        private boolean isInDestination(Path path) {
            return path.startsWith(to);
        }
    }
}
