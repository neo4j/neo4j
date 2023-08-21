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
package org.neo4j.io.fs.watcher;

import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.TRUE;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.Watchable;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.neo4j.io.fs.watcher.resource.WatchedResource;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class DefaultFileSystemWatcherTest {
    @Inject
    TestDirectory testDirectory;

    private WatchService watchServiceMock = mock(WatchService.class);

    @Test
    void fileWatchRegistrationIsIllegal() {
        DefaultFileSystemWatcher watcher = createWatcher();

        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> watcher.watch(Path.of("notADirectory")));
        assertThat(exception.getMessage()).contains("Only directories can be registered to be monitored.");
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void shouldHandleMultipleSubscribersToSameFile() throws Exception {
        try (OtherThreadExecutor executor = new OtherThreadExecutor("watcher");
                DefaultFileSystemWatcher watcher =
                        new DefaultFileSystemWatcher(FileSystems.getDefault().newWatchService())) {
            // Given
            AssertableFileEventListener listener = new AssertableFileEventListener();
            watcher.addFileWatchEventListener(listener);

            Path foo = testDirectory.createFile("foo");
            Path bar = testDirectory.createFile("bar");

            executor.executeDontWait(() -> {
                try {
                    watcher.startWatching();
                } catch (Exception ignored) { // Expected
                }
                return null;
            });

            WatchedResource firstWatch = watcher.watch(testDirectory.homePath());
            firstWatch.close();
            assertThat(firstWatch.getWatchKey().isValid()).isFalse();

            WatchedResource secondWatch = watcher.watch(testDirectory.homePath());
            WatchedResource thirdWatch = watcher.watch(testDirectory.homePath());
            executor.waitUntilWaiting(location -> location.isAt(DefaultFileSystemWatcher.class, "startWatching"));

            // When
            testDirectory.getFileSystem().delete(foo);
            // Then
            assertEventually("Foo deleted", () -> listener.containsDeleted("foo"), TRUE, 30, SECONDS);

            // When
            secondWatch.close(); // Closing the first should still allow the second to observe deletions
            assertThat(thirdWatch.getWatchKey().isValid()).isTrue();
            testDirectory.getFileSystem().delete(bar);
            assertEventually("Bar deleted", () -> listener.containsDeleted("bar"), TRUE, 30, SECONDS);

            thirdWatch.close();
            assertThat(thirdWatch.getWatchKey().isValid()).isFalse();
        }
    }

    @Test
    void registerMultipleDirectoriesForMonitoring() throws Exception {
        try (DefaultFileSystemWatcher watcher =
                new DefaultFileSystemWatcher(FileSystems.getDefault().newWatchService())) {
            Path directory1 = testDirectory.directory("test1");
            Path directory2 = testDirectory.directory("test2");
            WatchedResource watchedResource1 = watcher.watch(directory1);
            WatchedResource watchedResource2 = watcher.watch(directory2);
            assertNotSame(watchedResource1, watchedResource2);
        }
    }

    @Test
    void notifyListenersOnDeletion() throws InterruptedException {
        TestFileSystemWatcher watcher = createWatcher();
        AssertableFileEventListener listener1 = new AssertableFileEventListener();
        AssertableFileEventListener listener2 = new AssertableFileEventListener();

        watcher.addFileWatchEventListener(listener1);
        watcher.addFileWatchEventListener(listener2);

        TestWatchEvent<Path> watchEvent = new TestWatchEvent<>(ENTRY_DELETE, Paths.get("file1"));
        TestWatchEvent<Path> watchEvent2 = new TestWatchEvent<>(ENTRY_DELETE, Paths.get("file2"));
        TestWatchKey watchKey = new TestWatchKey(asList(watchEvent, watchEvent2));

        prepareWatcher(watchKey);

        watch(watcher);

        listener1.assertDeleted("file1");
        listener1.assertDeleted("file2");
        listener2.assertDeleted("file1");
        listener2.assertDeleted("file2");
    }

    @Test
    void notifyListenersOnModification() throws InterruptedException {
        TestFileSystemWatcher watcher = createWatcher();
        AssertableFileEventListener listener1 = new AssertableFileEventListener();
        AssertableFileEventListener listener2 = new AssertableFileEventListener();

        watcher.addFileWatchEventListener(listener1);
        watcher.addFileWatchEventListener(listener2);

        TestWatchEvent<Path> watchEvent = new TestWatchEvent<>(ENTRY_MODIFY, Paths.get("a"));
        TestWatchEvent<Path> watchEvent2 = new TestWatchEvent<>(ENTRY_MODIFY, Paths.get("b"));
        TestWatchEvent<Path> watchEvent3 = new TestWatchEvent<>(ENTRY_MODIFY, Paths.get("c"));
        TestWatchKey watchKey = new TestWatchKey(asList(watchEvent, watchEvent2, watchEvent3));

        prepareWatcher(watchKey);

        watch(watcher);

        listener1.assertModified("a");
        listener1.assertModified("b");
        listener1.assertModified("c");
        listener2.assertModified("a");
        listener2.assertModified("b");
        listener2.assertModified("c");
    }

    @Test
    void stopWatchingAndCloseEverythingOnClosed() throws IOException {
        TestFileSystemWatcher watcher = createWatcher();
        watcher.close();

        verify(watchServiceMock).close();
        assertTrue(watcher.isClosed());
    }

    @Test
    void skipEmptyEvent() throws InterruptedException {
        TestFileSystemWatcher watcher = createWatcher();

        AssertableFileEventListener listener = new AssertableFileEventListener();
        watcher.addFileWatchEventListener(listener);

        TestWatchEvent<String> event = new TestWatchEvent(ENTRY_MODIFY, null);
        TestWatchKey watchKey = new TestWatchKey(asList(event));

        prepareWatcher(watchKey);

        watch(watcher);

        listener.assertNoEvents();
    }

    private void prepareWatcher(TestWatchKey watchKey) throws InterruptedException {
        when(watchServiceMock.take()).thenReturn(watchKey).thenThrow(InterruptedException.class);
    }

    private static void watch(TestFileSystemWatcher watcher) {
        try {
            watcher.startWatching();
        } catch (InterruptedException ignored) {
            // expected
        }
    }

    private TestFileSystemWatcher createWatcher() {
        return new TestFileSystemWatcher(watchServiceMock);
    }

    private static class TestFileSystemWatcher extends DefaultFileSystemWatcher {

        private boolean closed;

        TestFileSystemWatcher(WatchService watchService) {
            super(watchService);
        }

        @Override
        public void close() throws IOException {
            super.close();
            closed = true;
        }

        public boolean isClosed() {
            return closed;
        }
    }

    private static class TestWatchKey implements WatchKey {
        private List<WatchEvent<?>> events;
        private boolean canceled;

        TestWatchKey(List<WatchEvent<?>> events) {
            this.events = events;
        }

        @Override
        public boolean isValid() {
            return false;
        }

        @Override
        public List<WatchEvent<?>> pollEvents() {
            return events;
        }

        @Override
        public boolean reset() {
            return false;
        }

        @Override
        public void cancel() {
            canceled = true;
        }

        @Override
        public Watchable watchable() {
            return null;
        }
    }

    private static class TestWatchEvent<T> implements WatchEvent {

        private Kind<T> eventKind;
        private T fileName;

        TestWatchEvent(Kind<T> eventKind, T fileName) {
            this.eventKind = eventKind;
            this.fileName = fileName;
        }

        @Override
        public Kind kind() {
            return eventKind;
        }

        @Override
        public int count() {
            return 0;
        }

        @Override
        public T context() {
            return fileName;
        }
    }

    private static class AssertableFileEventListener implements FileWatchEventListener {
        private final List<String> deletedFileNames = new ArrayList<>();
        private final List<String> modifiedFileNames = new ArrayList<>();

        @Override
        public synchronized void fileDeleted(WatchKey key, String fileName) {
            deletedFileNames.add(fileName);
        }

        @Override
        public synchronized void fileModified(WatchKey key, String fileName) {
            modifiedFileNames.add(fileName);
        }

        synchronized void assertNoEvents() {
            assertThat(deletedFileNames)
                    .as("Should not have any deletion events")
                    .isEmpty();
            assertThat(modifiedFileNames)
                    .as("Should not have any modification events")
                    .isEmpty();
        }

        synchronized void assertDeleted(String fileName) {
            assertThat(deletedFileNames)
                    .as("Was expected to find notification about deletion.")
                    .contains(fileName);
        }

        synchronized boolean containsDeleted(String fileName) {
            return deletedFileNames.contains(fileName);
        }

        synchronized void assertModified(String fileName) {
            assertThat(modifiedFileNames)
                    .as("Was expected to find notification about modification.")
                    .contains(fileName);
        }
    }
}
