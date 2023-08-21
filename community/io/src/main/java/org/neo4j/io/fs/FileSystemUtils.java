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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import org.neo4j.io.memory.NativeScopedBuffer;
import org.neo4j.memory.MemoryTracker;

/**
 * This class consists exclusively of static methods that operate on files, directories, or other types of files.
 * Every method takes {@link FileSystemAbstraction} as a parameter to be able to work across all different file systems.
 *
 * @see FileUtils
 */
public final class FileSystemUtils {
    private FileSystemUtils() {}

    /**
     * Writes provided string into a file denoted by the {@code path} using provided file system.
     *
     * The semantics of how this file is opened for write is the same as underlying file system {@link FileSystemAbstraction#write(Path)} call.
     * Write will happen using intermediate direct byte buffer which will be created and released during this call.
     *
     * @param fs user provided file system.
     * @param path the path to the file to write.
     * @param value string value to write.
     * @param memoryTracker tracker where allocated direct byte buffer will be tracked.
     * @throws IOException on I/O error opening/creating/writing the file.
     */
    public static void writeString(FileSystemAbstraction fs, Path path, String value, MemoryTracker memoryTracker)
            throws IOException {
        writeAllBytes(fs, path, value.getBytes(UTF_8), memoryTracker);
    }

    /**
     * Writes provided byte array into a file denoted by the {@code path} using provided file system.
     *
     * The semantics of how this file is opened for write is the same as underlying file system {@link FileSystemAbstraction#write(Path)} call.
     * Write will happen using intermediate direct byte buffer which will be created and released during this call.
     *
     * @param fs user provided file system.
     * @param path the path to the file to write.
     * @param data byte array to write.
     * @param memoryTracker tracker where allocated direct byte buffer will be tracked.
     * @throws IOException on I/O error opening/creating/writing the file.
     */
    public static void writeAllBytes(FileSystemAbstraction fs, Path path, byte[] data, MemoryTracker memoryTracker)
            throws IOException {
        try (StoreChannel storeChannel = fs.write(path)) {
            try (var scopedBuffer = new NativeScopedBuffer(data.length, ByteOrder.LITTLE_ENDIAN, memoryTracker)) {
                ByteBuffer buffer = scopedBuffer.getBuffer();
                buffer.put(data);
                buffer.flip();
                storeChannel.writeAll(buffer);
            }
        }
    }

    /**
     * Read string from a file denoted by the {@code path} using provided file system.
     *
     * The semantics of how this file is opened for read is the same as underlying file system {@link FileSystemAbstraction#read(Path)} call.
     * Read will happen using intermediate direct byte buffer which will be created and released during this call.
     *
     * @param fs user provided file system.
     * @param path the path to the file to read.
     * @param memoryTracker tracker where allocated direct byte buffer will be tracked.
     * @throws IOException on I/O error opening/creating/writing the file.
     */
    public static String readString(FileSystemAbstraction fs, Path path, MemoryTracker memoryTracker)
            throws IOException {
        if (!fs.fileExists(path)) {
            return null;
        }
        return new String(readAllBytes(fs, path, memoryTracker), UTF_8);
    }

    /**
     * Read all bytes from a file denoted by the {@code path} using the provided file system.
     *
     * The semantics of how this file is opened for read is the same as underlying file system {@link FileSystemAbstraction#read(Path)} call.
     * Read will happen using intermediate direct byte buffer which will be created and released during this call.
     *
     * @param fs user provided file system.
     * @param path the path to the file to read.
     * @param memoryTracker tracker where allocated direct byte buffer will be tracked.
     * @throws IOException on I/O error opening/creating/writing the file.
     */
    public static byte[] readAllBytes(FileSystemAbstraction fs, Path path, MemoryTracker memoryTracker)
            throws IOException {
        long fileSize = fs.getFileSize(path);
        try (StoreChannel reader = fs.read(path);
                var scopedBuffer = new NativeScopedBuffer(fileSize, ByteOrder.LITTLE_ENDIAN, memoryTracker)) {
            ByteBuffer buffer = scopedBuffer.getBuffer();
            reader.readAll(buffer);
            buffer.flip();
            var data = new byte[(int) fileSize];
            buffer.get(data);
            return data;
        }
    }

    /**
     * Read strings from a file denoted by the {@code path} using provided file system.
     *
     * The semantics of how this file is opened for read is the same as underlying file system {@link FileSystemAbstraction#read(Path)} call.
     * Read will happen using intermediate direct byte buffer which will be created and released during this call.
     *
     * @param fs user provided file system.
     * @param path the path to the file to read.
     * @param memoryTracker tracker where allocated direct byte buffer will be tracked.
     * @throws IOException on I/O error opening/creating/writing the file.
     */
    public static List<String> readLines(FileSystemAbstraction fs, Path path, MemoryTracker memoryTracker)
            throws IOException {
        var string = readString(fs, path, memoryTracker);
        return string != null ? string.lines().toList() : null;
    }

    /**
     * Check if directory is empty.
     *
     * @param fs file system to use.
     * @param directory directory to check.
     * @return {@code true} when directory does not exist or exists and is empty, {@code false} otherwise.
     */
    public static boolean isEmptyOrNonExistingDirectory(FileSystemAbstraction fs, Path directory) throws IOException {
        if (fs.isDirectory(directory)) {
            Path[] files = fs.listFiles(directory);
            return ArrayUtils.isEmpty(files);
        }
        return !fs.fileExists(directory);
    }

    /**
     * Calculates the size of a given directory or file given the provided abstract filesystem.
     * Returns zero if file does not exist.
     *
     * @param fs the filesystem abstraction to use.
     * @param file to the file or directory.
     * @return the size, in bytes, of the file or the total size of the content in the directory, including
     * subdirectories.
     */
    public static long size(FileSystemAbstraction fs, Path file) {
        try {
            if (fs.isDirectory(file)) {
                Path[] files = fs.listFiles(file);
                long size = 0L;
                for (Path child : files) {
                    size += size(fs, child);
                }
                return size;
            } else {
                return fs.getFileSize(file);
            }
        } catch (IOException e) {
            // usually it's file that was deleted while evaluating directory size
            return 0;
        }
    }

    public static void deleteFile(FileSystemAbstraction fs, Path fileToDelete) throws IOException {
        if (fs.isDirectory(fileToDelete)) {
            fs.deleteRecursively(fileToDelete);
        } else {
            fs.deleteFile(fileToDelete);
        }
    }

    /**
     * Converts path to a string in a way that allows the string to be read back.
     * This is to address windows paths with backslashes.
     * @param path to convert.
     * @return string representation of path.
     */
    public static String pathToString(Path path) {
        return path.toString().replace("\\", "\\\\");
    }
}
