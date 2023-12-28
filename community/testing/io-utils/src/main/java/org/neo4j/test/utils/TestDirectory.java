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
package org.neo4j.test.utils;

import static java.lang.String.format;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.apache.commons.codec.digest.DigestUtils;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileHandle;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.util.VisibleForTesting;

/**
 * This class represents a temporary directory which ensures that the test's working directory is cleaned up. The clean-up
 * only happens if the test passes, to help diagnose test failures.  For example:
 * <pre>
 *  {@literal @}TestDirectoryExtension
 *   class SomeTest
 *   {
 *    {@literal @}Inject
 *     private TestDirectory dir;
 *
 *    {@literal @}Test
 *     void shouldDoSomething()
 *     {
 *       File storeDir = dir.homeDir();
 *       // do stuff with home dir
 *     }
 *   }
 * </pre>
 */
public class TestDirectory {
    /**
     * {@link TestDirectory#now}, {@link TestDirectory#DATE_TIME_FORMATTER} and hash(test_folder_name) are used to
     * construct uniquely naming test directories.
     * By getting a new value here, every time the JVM is started, we the same
     * tests will get different directory names when executed many times in
     * different JVMs.
     * This way, the test results for many runs of the same tests are kept
     * around, so they can easily be compared with each other. This is useful
     * when you need to investigate a flaky test, for instance.
     */
    private static final LocalDateTime now = LocalDateTime.now(Clock.systemUTC());

    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("dd_HH-mm-ss-SSS").withZone(ZoneOffset.UTC);
    public static final String REGISTER_FILE_NAME = ".register";

    private final FileSystemAbstraction fileSystem;
    private Path testClassBaseFolder;
    private Class<?> owningTest;
    private boolean keepDirectoryAfterSuccessfulTest;
    private Path directory;
    private int additionalRefs;
    // Only really used in "per class" and nested scenarios
    private boolean anyFailure;

    private TestDirectory(FileSystemAbstraction fileSystem) {
        this.fileSystem = fileSystem;
    }

    private TestDirectory(FileSystemAbstraction fileSystem, Class<?> owningTest) {
        this.fileSystem = fileSystem;
        this.owningTest = owningTest;
    }

    public static TestDirectory testDirectory() {
        return new TestDirectory(new DefaultFileSystemAbstraction());
    }

    public static TestDirectory testDirectory(FileSystemAbstraction fs) {
        return new TestDirectory(fs);
    }

    public static TestDirectory testDirectory(Class<?> owningTest) {
        return new TestDirectory(new DefaultFileSystemAbstraction(), owningTest);
    }

    public static TestDirectory testDirectory(Class<?> owningTest, FileSystemAbstraction fs) {
        return new TestDirectory(fs, owningTest);
    }

    /**
     * Keep the store directory, even after a successful test.
     * It's just a useful debug mechanism to have for analyzing store after a test.
     * by default directories aren't kept.
     */
    public TestDirectory keepDirectoryAfterSuccessfulTest() {
        keepDirectoryAfterSuccessfulTest = true;
        return this;
    }

    public Path absolutePath() {
        return homePath().toAbsolutePath();
    }

    public Path homePath() {
        if (!isInitialised()) {
            throw new IllegalStateException("Not initialized");
        }
        return directory;
    }

    public Path homePath(String homeDirName) {
        return directory(homeDirName);
    }

    public boolean isInitialised() {
        return directory != null;
    }

    public Path cleanDirectory(String name) throws IOException {
        return clean(fileSystem, directory(name));
    }

    public Path directory(String name) {
        Path dir = homePath().resolve(name);
        createDirectory(dir);
        return dir;
    }

    public Path cleanDirectory(String name, String... path) throws IOException {
        return clean(fileSystem, directory(name, path));
    }

    public Path directory(String name, String... path) {
        Path dir = homePath();
        for (String s : path) {
            dir = dir.resolve(s);
        }
        dir = dir.resolve(name);
        createDirectory(dir);
        return dir;
    }

    public Path file(String name) {
        return homePath().resolve(name);
    }

    public Path file(String name, String... path) {
        Path dir = homePath();
        for (String s : path) {
            dir = dir.resolve(s);
        }
        return dir.resolve(name);
    }

    public Path createFile(String name) {
        Path file = file(name);
        ensureFileExists(file);
        return file;
    }

    public void cleanup() throws IOException {
        clean(fileSystem, testClassBaseFolder);
    }

    @Override
    public String toString() {
        String testDirectoryName = isInitialised() ? directory.toString() : "<uninitialized>";
        return format("%s[\"%s\"]", getClass().getSimpleName(), testDirectoryName);
    }

    private Path cleanBaseDirectory(String name) throws IOException {
        return clean(fileSystem, ensureBase().resolve(name));
    }

    public void complete(boolean success) throws IOException {
        anyFailure |= !success;
        if (isInitialised()) {
            if (additionalRefs > 0) {
                additionalRefs--;
                return;
            }

            Path directory = this.directory;
            this.directory = null;
            if (!anyFailure && !keepDirectoryAfterSuccessfulTest) {
                fileSystem.deleteRecursively(directory);
            } else if (!fileSystem.isPersistent()) {
                // We want to keep the files, make sure they actually exist on disk, not only in memory (like in
                // EphemeralFileSystem)
                for (FileHandle fh : fileSystem.streamFilesRecursive(directory).toArray(FileHandle[]::new)) {
                    Path path = fh.getPath();
                    Files.createDirectories(path.getParent());
                    try (InputStream inputStream = fileSystem.openAsInputStream(path)) {
                        Files.copy(inputStream, path, REPLACE_EXISTING);
                    }
                }
            }
        }
    }

    public void close() throws IOException {
        if (isInitialised()) {
            return;
        }
        try {
            if (testClassBaseFolder != null && Files.exists(testClassBaseFolder)) {
                try {
                    var files = fileSystem.listFiles(testClassBaseFolder);
                    if (files != null
                            && files.length == 1
                            && files[0].getFileName().toString().equals(REGISTER_FILE_NAME)) {
                        fileSystem.deleteRecursively(testClassBaseFolder);
                    }
                } catch (IOException e) {
                    // Couldn't list files, move on
                }
            }
        } finally {
            fileSystem.close();
        }
    }

    public void prepareDirectory(Class<?> testClass, String test) throws IOException {
        if (isInitialised()) {
            additionalRefs++;
            return;
        }

        if (owningTest == null) {
            owningTest = testClass;
        }
        if (test == null) {
            test = "static";
        }
        directory = prepareDirectoryForTest(test);
    }

    public Path prepareDirectoryForTest(String test) throws IOException {
        String dir = getDateTime() + "_" + DigestUtils.md5Hex(test).substring(0, 5);
        evaluateClassBaseTestFolder();
        register(test, dir);
        return cleanBaseDirectory(dir);
    }

    private String getDateTime() {
        return DATE_TIME_FORMATTER.format(now);
    }

    @VisibleForTesting
    public FileSystemAbstraction getFileSystem() {
        return fileSystem;
    }

    private void ensureFileExists(Path file) {
        try {
            if (!fileSystem.fileExists(file)) {
                fileSystem.write(file).close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create file: " + file, e);
        }
    }

    private void createDirectory(Path directory) {
        try {
            fileSystem.mkdirs(directory);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create directory: " + directory, e);
        }
    }

    private static Path clean(FileSystemAbstraction fs, Path dir) throws IOException {
        if (fs.fileExists(dir)) {
            fs.deleteRecursively(dir);
        }
        fs.mkdirs(dir);
        return dir;
    }

    private void evaluateClassBaseTestFolder() {
        if (owningTest == null) {
            throw new IllegalStateException(" Test owning class is not defined");
        }
        testClassBaseFolder = testDataDirectoryOf(owningTest);
    }

    private static Path testDataDirectoryOf(Class<?> owningTest) {
        Path testData = locateTarget(owningTest).resolve("test data");
        return testData.resolve(shorten(owningTest.getName())).toAbsolutePath();
    }

    private static String shorten(String owningTestName) {
        int targetPartLength = 5;
        String[] parts = owningTestName.split("\\.");
        for (int i = 0; i < parts.length - 1; i++) {
            String part = parts[i];
            if (part.length() > targetPartLength) {
                parts[i] = part.substring(0, targetPartLength - 1) + "~";
            }
        }
        return String.join(".", parts);
    }

    private void register(String test, String dir) {
        try (PrintStream printStream = new PrintStream(
                fileSystem.openAsOutputStream(ensureBase().resolve(REGISTER_FILE_NAME), true),
                false,
                StandardCharsets.UTF_8)) {
            printStream.print(format("%s = %s%n", dir, test));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Path ensureBase() {
        if (testClassBaseFolder == null) {
            evaluateClassBaseTestFolder();
        }
        if (fileSystem.fileExists(testClassBaseFolder) && !fileSystem.isDirectory(testClassBaseFolder)) {
            throw new IllegalStateException(testClassBaseFolder + " exists and is not a directory!");
        }

        try {
            fileSystem.mkdirs(testClassBaseFolder);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return testClassBaseFolder;
    }

    private static Path locateTarget(Class<?> owningTest) {
        try {
            Path codeSource = Path.of(owningTest
                    .getProtectionDomain()
                    .getCodeSource()
                    .getLocation()
                    .toURI());
            if (Files.isDirectory(codeSource)) {
                // code loaded from a directory
                return codeSource.getParent();
            }
        } catch (URISyntaxException e) {
            // ignored
        }
        return Path.of("target");
    }
}
