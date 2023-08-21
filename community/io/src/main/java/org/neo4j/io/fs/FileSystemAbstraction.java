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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.stream.Stream;
import org.neo4j.io.fs.watcher.FileWatcher;

/**
 * Abstraction for all interactions with files.
 */
public interface FileSystemAbstraction extends Closeable {
    /**
     * Used as return value from {@link #getFileDescriptor(StoreChannel)} for a channel where the machine-specific file descriptor
     * for that given file cannot be determined or retrieved.
     */
    int INVALID_FILE_DESCRIPTOR = -1;

    CopyOption[] EMPTY_COPY_OPTIONS = new CopyOption[0];

    /**
     * Create file watcher that provides possibilities to monitor directories on underlying file system for modifications or deletions
     * made from an external process directly to a file.
     *
     * @return specific file system abstract watcher
     * @throws IOException in case exception occur during file watcher creation
     */
    FileWatcher fileWatcher() throws IOException;

    /**
     * Opens a file denoted by the {@code fileName} and returns a {@link StoreChannel} to interact with its contents. This call can alternatively even
     * create the file if it doesn't already exist, as well as opening it in read-only or read-write mode, all depending on
     * the provided {@code options}.
     *
     * @param fileName the path to the file to open.
     * @param options a set of options to apply to this call. Common such options include:
     * <ul>
     *     <li>{@link StandardOpenOption#READ}: open the file for reading its contents</li>
     *     <li>{@link StandardOpenOption#WRITE}: open the file for writing into it}</li>
     *     <li>{@link StandardOpenOption#CREATE}: create the file before opening it, if it doesn't already exist</li>
     *     <li>{@link StandardOpenOption#CREATE_NEW}: create the file and fail if it already exists</li>
     *     <li>{@link StandardOpenOption#TRUNCATE_EXISTING}: truncate the file to 0 bytes if the file already existed</li>
     *     <li>{@link StandardOpenOption#APPEND}: accompanied {@link StandardOpenOption#WRITE} this places the write position
     *     at the end of the file, if it already contains data</li>
     * </ul>
     * @return the {@link StoreChannel} used to interact with the opened file.
     * @throws IOException on I/O error opening/creating the file with the provided set of {@code options}, or if the provided options
     * doesn't match the state of the file, e.g. if the options prohibits the file from existing, but it already exists.
     */
    StoreChannel open(Path fileName, Set<OpenOption> options) throws IOException;

    /**
     * Opens a file denoted by the {@code fileName} and returns an {@link OutputStream} to write binary data to it.
     * The semantics of how this file is opened is the equivalence of:
     * <ul>
     *     <li>if {@code append=false}: {@link StandardOpenOption#CREATE}, {@link StandardOpenOption#TRUNCATE_EXISTING}
     *     and {@link StandardOpenOption#WRITE}</li>
     *     <li>if {@code append=true}: {@link StandardOpenOption#CREATE} and {@link StandardOpenOption#APPEND}</li>
     * </ul>
     *
     * @param fileName the path to the file to open.
     * @param append if {@code false} truncates the file to zero length, otherwise if {@code true} sets the position at the end of the
     * existing file so that written data gets appended at the end of the file.
     * @return an {@link OutputStream} capable of writing binary data to the file denoted by {@code fileName}.
     * @throws IOException on I/O error opening/creating the file.
     */
    OutputStream openAsOutputStream(Path fileName, boolean append) throws IOException;

    /**
     * Opens a file denoted by the {@code fileName} and returns an {@link InputStream} to read from it.
     *
     * @param fileName the path to the file to open.
     * @return an {@link InputStream} capable of reading from the file denoted by {@code fileName}.
     * @throws IOException on I/O error or if the file doesn't exist.
     */
    InputStream openAsInputStream(Path fileName) throws IOException;

    /**
     * Convenience method for calling {@link #open(Path, Set)} with the open options:
     * <ul>
     *     <li>{@link StandardOpenOption#READ}</li>
     *     <li>{@link StandardOpenOption#WRITE}</li>
     *     <li>{@link StandardOpenOption#CREATE}</li>
     * </ul>
     *
     * @param fileName the path to the file to open.
     * @return the {@link StoreChannel} used to interact with the opened file.
     * @throws IOException on I/O error opening/creating the file.
     */
    StoreChannel write(Path fileName) throws IOException;

    /**
     * Convenience method for calling {@link #open(Path, Set)} with the open options:
     * <ul>
     *     <li>{@link StandardOpenOption#READ}</li>
     * </ul>
     *
     * @param fileName the path to the file to open.
     * @return the {@link StoreChannel} used to interact with the opened file.
     * @throws IOException on I/O error opening the file.
     */
    StoreChannel read(Path fileName) throws IOException;

    /**
     * @param file the file to check whether or not it exists in this file system.
     * @return {@code true} if this file exists, otherwise {@code false}.
     */
    boolean fileExists(Path file);

    /**
     * Creates the directory denoted by {@code fileName}. All parent directories must exist for this call to succeed.
     * If the file already exists, be it as a file or directory then a {@link FileAlreadyExistsException} will be thrown.
     *
     * @param fileName the path to the directory to create.
     * @throws FileAlreadyExistsException if this file already exists, either as a file or as a directory.
     * @throws IOException on I/O error creating the directory.
     */
    void mkdir(Path fileName) throws IOException;

    /**
     * Creates the directory denoted by {@code fileName}, including all non-existent parent directories.
     * If the path already exists as a directory then this call will do nothing and not throw exception, however if the path
     * already exists as a file a {@link FileAlreadyExistsException} will be thrown.
     *
     * @param fileName the path to the directory to create, together with its non-existent parent directories.
     * @throws IOException on I/O error creating any of the directories.
     */
    void mkdirs(Path fileName) throws IOException;

    /**
     * Returns the size, in bytes, of the file denoted by {@code fileName}.
     * If the provided {@code fileName} is a directory then {@code 0} is returned.
     *
     * @param fileName the file to get the file size of.
     * @return the file size, in bytes.
     * @throws NoSuchFileException if the file doesn't exist.
     * @throws IOException on I/O error.
     */
    long getFileSize(Path fileName) throws IOException;

    /**
     * Returns the block size of the file denoted by {@code file}, or rather the block size of files on the drive that
     * the given file is on. Block size is the smallest chunk of bytes that the underlying file storage reads/writes bytes from/to.
     *
     * @param file the file to get the block size for.
     * @return the block size for the given {@code file}.
     * @throws IOException on I/O error getting the block size.
     */
    long getBlockSize(Path file) throws IOException;

    /**
     * Deletes the file/directory denoted by {@code path}, if it exists. If the {@code path} denotes a directory then the directory, and all its
     * files and sub-directories recursively will also be deleted.
     *
     * @param path the path (file or directory) to delete.
     * @throws IOException on I/O error or if one or more file/directory couldn't be deleted. One scenario is that the file may
     * be in use by some other process (mostly a Windows-specific issue) or security/permission issues.
     */
    default void delete(Path path) throws IOException {
        if (isDirectory(path)) {
            deleteRecursively(path);
        } else {
            deleteFile(path);
        }
    }

    /**
     * Deletes the file/directory denoted by {@code fileName}, if it exists.
     * If {@code fileName} denotes a directory the directory must be empty, otherwise a {@link DirectoryNotEmptyException} is thrown.
     *
     * @param fileName file or empty directory to delete.
     * @throws DirectoryNotEmptyException if {@code fileName} denotes a directory and the directory isn't empty.
     * @throws IOException on I/O error or if the file couldn't be deleted.
     */
    void deleteFile(Path fileName) throws IOException;

    /**
     * Deletes the directory denoted by {@code fileName}, if it exists.
     * If {@code fileName} denotes a file a {@link NotDirectoryException} is thrown.
     * This operation is not atomic in that if mid-operation a file or directory can't be deleted or an {@link IOException} is thrown,
     * some files and directories may have been deleted and some may not.
     *
     * @param directory directory to delete.
     * @throws NotDirectoryException if {@code fileName} denotes a file.
     * @throws IOException on I/O error or if some file or directory couldn't be deleted.
     */
    void deleteRecursively(Path directory) throws IOException;

    /**
     * Renames/moves a file or directory. Renaming a file or directory to/from the same file storage is typically an atomic operation,
     * but if {@code to} and {@code from} are different file storages it's not and that's where the {@code copyOptions} argument comes in,
     * to control how that is handled.
     *
     * @param from the file/directory to rename/move.
     * @param to the new name/location of the file/directory.
     * @param copyOptions controls how a file/directory is moved.
     * @throws FileAlreadyExistsException if the {@code to} file/directory already exist.
     * @throws IOException on I/O error.
     */
    void renameFile(Path from, Path to, CopyOption... copyOptions) throws IOException;

    /**
     * Lists files in the given {@code directory}.
     *
     * @param directory the directory to list files for. Both files and directories contained in the {@code directory} are returned, non-recursively.
     * @return a list of files and directories contained in the provided {@code directory}
     * @throws NotDirectoryException if the provided {@code directory} isn't a directory.
     * @throws NoSuchFileException if the provided {@code directory} doesn't exist.
     * @throws IOException on I/O error.
     */
    Path[] listFiles(Path directory) throws IOException;

    /**
     * Lists files that passes the provided {@code filter} in the given {@code directory}.
     *
     * @param directory the directory to list files for. Both files and directories contained in the {@code directory} are returned, non-recursively.
     * @param filter the filter to use in the listing.
     * @return a list of files and directories contained in the provided {@code directory}
     * @throws NotDirectoryException if the provided {@code directory} isn't a directory.
     * @throws NoSuchFileException if the provided {@code directory} doesn't exist.
     * @throws IOException on I/O error.
     */
    Path[] listFiles(Path directory, DirectoryStream.Filter<Path> filter) throws IOException;

    /**
     * @param file the file to check whether or not it's a directory.
     * @return {@code true} if the {@code file} exists and denotes a directory, otherwise {@code false}.
     */
    boolean isDirectory(Path file);

    /**
     * Moves a file to another directory. If the {@code toDirectory} or any of its parent doesn't exist they will be created before moving the file.
     *
     * @param file the file to move.
     * @param toDirectory path to directory in which to move the {@code file}.
     * @throws NotDirectoryException if {@code toDirectory} isn't a directory.
     * @throws IOException on I/O error.
     */
    void moveToDirectory(Path file, Path toDirectory) throws IOException;

    /**
     * Copies a file to another directory. If the {@code toDirectory} or any of its parent doesn't exist they will be created before moving the file.
     *
     * @param file the file to copy.
     * @param toDirectory path to directory in which to copy the {@code file}.
     * @throws NotDirectoryException if {@code toDirectory} isn't a directory.
     * @throws IOException on I/O error.
     */
    void copyToDirectory(Path file, Path toDirectory) throws IOException;

    /**
     * Copies a file. If the target file {@code to} exists and is a file it will be overwritten.
     * If the target file exists and is an empty directory it will be deleted first.
     * If the target file exists and is a non-empty directory a {@link DirectoryNotEmptyException} is thrown.
     *
     * @param from file to copy.
     * @param to target location to copy file to, overwritten if exists.
     * @throws DirectoryNotEmptyException if target file {@code to} is a non-empty directory.
     * @throws IOException on I/O error.
     */
    default void copyFile(Path from, Path to) throws IOException {
        copyFile(from, to, StandardCopyOption.REPLACE_EXISTING);
    }

    /**
     * Copies a file. If the target file {@code to} exists and is a file it will be overwritten.
     * If the target file exists and is an empty directory it will be deleted first.
     * If the target file exists and is a non-empty directory a {@link DirectoryNotEmptyException} is thrown.
     *
     * @param from file to copy.
     * @param to target location to copy file to, overwritten if exists.
     * @param copyOptions controls how the file is copied.
     * @throws DirectoryNotEmptyException if target file {@code to} is a non-empty directory.
     * @throws IOException on I/O error.
     */
    void copyFile(Path from, Path to, CopyOption... copyOptions) throws IOException;

    /**
     * Copies all sub-directories and files recursively of the directory denoted by {@code fromDirectory} into the directory denoted by {@code toDirectory}.
     * Example:
     * <pre>
     *     copyRecursively( Path.of( "/path/of/from" ), Path.of( "/path/of/to" );
     *     Where contents are:
     *
     *        /path/of/from/
     *            ├─ folder_1/
     *            │    ├─ file_A
     *            │    └─ file_B
     *            └─ file_C
     *
     *     Will have the end result of:
     *
     *        /path/of/to/
     *            ├─ folder_1/
     *            │    ├─ file_A
     *            │    └─ file_B
     *            └─ file_C
     *
     * </pre>
     *
     * @param fromDirectory directory which contents to copy recursively.
     * @param toDirectory directory to copy the files and directories into.
     * @throws IllegalArgumentException if {@code fromDirectory} isn't a directory, or if {@code fromDirectory} and {@code toDirectory} are the same.
     * @throws NoSuchFileException if {@code toDirectory} is a file.
     * @throws IOException on I/O error.
     */
    void copyRecursively(Path fromDirectory, Path toDirectory) throws IOException;

    /**
     * Truncates a file down to the given {@code size} in bytes, removing all remaining bytes from the file.
     * If the file is smaller than the given {@code size} this operation will do nothing.
     *
     * @param file the file to truncate.
     * @param size the size to truncate to, i.e. how many bytes to keep of the file.
     * @throws NoSuchFileException if the file doesn't exist.
     * @throws IOException on I/O error or if the {@code file} is a directory.
     */
    void truncate(Path file, long size) throws IOException;

    /**
     * Returns the timestamp (in milliseconds) of when the given file or directory denoted by {@code file} was last modified.
     * The timestamp is read from the metadata for the file from file storage.
     *
     * @param file the file to get last modified time for.
     * @return the timestamp (in milliseconds) of when the given {@code file} was last modified.
     * @throws NoSuchFileException if the file doesn't exist.
     * @throws IOException on I/O error.
     */
    long lastModifiedTime(Path file) throws IOException;

    /**
     * Deletes the file/directory denoted by {@code fileName}. If the file/directory doesn't exist a {@link NoSuchFileException} is thrown.
     * If {@code fileName} denotes a directory the directory must be empty, otherwise a {@link DirectoryNotEmptyException} is thrown.
     *
     * @param file file or empty directory to delete.
     * @throws DirectoryNotEmptyException if {@code fileName} denotes a directory and the directory isn't empty.
     * @throws NoSuchFileException if {@code fileName} doesn't exist.
     * @throws IOException on I/O error or if the file couldn't be deleted.
     */
    void deleteFileOrThrow(Path file) throws IOException;

    /**
     * Return a stream of {@link FileHandle file handles} for every file in the given directory, and its
     * subdirectories.
     * <p>
     * Alternatively, if the {@link Path} given as an argument refers to a file instead of a directory, then a stream
     * will be returned with a file handle for just that file.
     * <p>
     * The stream is based on a snapshot of the file tree, so changes made to the tree using the returned file handles
     * will not be reflected in the stream.
     * <p>
     * If a file handle ends up leaving a directory empty through a rename or a delete, then the empty directory will
     * automatically be deleted as well. Likewise, if a file is moved to a path where not all of the directories in
     * the path exists, then those missing directories will be created prior to the file rename.
     *
     * @param path The base directory to start streaming files from, or the specific individual file to stream.
     * @param includeDirectories {@code true} to include directories in the {@link Stream}, {@code false} otherwise.
     * @return A {@link Stream} of all files in the tree.
     * @throws NoSuchFileException If the given base directory or file does not exist.
     * @throws IOException  If an I/O error occurs, possibly with the canonicalization of the paths.
     */
    default Stream<FileHandle> streamFilesRecursive(Path path, boolean includeDirectories) throws IOException {
        return StreamFilesRecursive.streamFilesRecursive(this, path, includeDirectories);
    }

    /**
     * Convenience method for calling {@link #streamFilesRecursive(Path,boolean)}, with an {@code includeDirectories}
     * of {@code false}; thus no directories are returned within the {@link Stream}, only files.
     * @see #streamFilesRecursive(Path, boolean)
     */
    default Stream<FileHandle> streamFilesRecursive(Path directory) throws IOException {
        return streamFilesRecursive(directory, false);
    }

    /**
     * Get underlying store channel file descriptor.
     *
     * @param channel channel to get file descriptor from.
     * @return {@link #INVALID_FILE_DESCRIPTOR} when the file description can't be retrieved from the provided channel.
     */
    int getFileDescriptor(StoreChannel channel);

    /**
     * @return {@code true} if the underlying filesystem is persistent, {@code false} otherwise.
     */
    boolean isPersistent();

    /**
     * Creates an empty file in the default temporary-file directory, using the given prefix and suffix to generate its name.
     * @param prefix the prefix string to be used in generating the file's name.
     * @param suffix the suffix string to be used in generating the file's name
     * @return the path to the newly created file that did not exist before this method was invoked.
     * @throws IOException if an I/O error occurs or the temporary-file directory does not exist.
     * @see #createTempFile(Path, String, String)
     */
    Path createTempFile(String prefix, String suffix) throws IOException;

    /**
     * Creates an empty file in the specified directory, using the given prefix and suffix to generate its name.
     * @param prefix the prefix string to be used in generating the file's name.
     * @param suffix the suffix string to be used in generating the file's name
     * @return the path to the newly created file that did not exist before this method was invoked.
     * @throws IOException if an I/O error occurs or the specified directory does not exist.
     */
    Path createTempFile(Path dir, String prefix, String suffix) throws IOException;

    /**
     * Creates a new directory in the default temporary-file directory, using the given prefix to generate its name.
     * @param prefix the prefix string to be used in generating the directory's name.
     * @return the path to the newly created directory that did not exist before this method was invoked.
     * @throws IOException if an I/O error occurs or the temporary-file directory does not exist.
     * @see #createTempDirectory(Path, String)
     */
    Path createTempDirectory(String prefix) throws IOException;

    /**
     * Creates a new directory in the specified directory, using the given prefix to generate its name.
     * @param prefix the prefix string to be used in generating the directory's name.
     * @return the path to the newly created directory that did not exist before this method was invoked.
     * @throws IOException if an I/O error occurs or the specified directory does not exist.
     */
    Path createTempDirectory(Path dir, String prefix) throws IOException;
}
