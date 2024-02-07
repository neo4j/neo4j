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
package org.neo4j.dbms.database;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.eclipse.collections.impl.factory.Sets.immutable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.io.pagecache.IOController.DISABLED;
import static org.neo4j.io.pagecache.PageCache.PAGE_SIZE;
import static org.neo4j.io.pagecache.impl.muninn.EvictionBouncer.ALWAYS_ALLOW;
import static org.neo4j.io.pagecache.impl.muninn.VersionStorage.EMPTY_STORAGE;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.neo4j.configuration.Config;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.DatabaseFlushEvent;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.io.pagecache.tracing.FileMappedListener;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.utils.TestDirectory;

@Neo4jLayoutExtension
class DatabasePageCacheTest {
    private static final String DATABASE_NAME = "test database";

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private Neo4jLayout neo4jLayout;

    private DatabasePageCache databasePageCache;
    private PageCache globalPageCache;
    private PagedFileAnswer pagedFileMapper;

    @BeforeEach
    void setUp() throws IOException {
        globalPageCache = mock(PageCache.class);
        pagedFileMapper = new PagedFileAnswer();
        when(globalPageCache.map(any(Path.class), eq(PAGE_SIZE), any(), any(), any(), any(), any()))
                .then(pagedFileMapper);
        databasePageCache = createPageCache();
    }

    @AfterEach
    void tearDown() {
        if (databasePageCache != null) {
            databasePageCache.close();
        }
    }

    @Test
    void mapDatabaseFile() throws IOException {
        Path mapFile = testDirectory.createFile("mapFile");
        PagedFile pagedFile = databasePageCache.map(mapFile, PAGE_SIZE, DATABASE_NAME, immutable.empty());

        assertNotNull(pagedFile);
        verify(globalPageCache)
                .map(mapFile, PAGE_SIZE, DATABASE_NAME, immutable.empty(), DISABLED, ALWAYS_ALLOW, EMPTY_STORAGE);
    }

    @Test
    void listExistingDatabaseMappings() throws IOException {
        Path mapFile1 = testDirectory.createFile("mapFile1");
        Path mapFile2 = testDirectory.createFile("mapFile2");
        PagedFile pagedFile = databasePageCache.map(mapFile1, PAGE_SIZE, DATABASE_NAME);
        PagedFile pagedFile2 = databasePageCache.map(mapFile2, PAGE_SIZE, DATABASE_NAME);

        List<PagedFile> pagedFiles = databasePageCache.listExistingMappings();
        assertThat(pagedFiles).hasSize(2);
        assertThat(pagedFiles).contains(pagedFile);
        assertThat(pagedFiles).contains(pagedFile2);
    }

    @Test
    void doNotIncludeNotDatabaseFilesInMappingsList() throws IOException {
        try (DatabasePageCache anotherDatabaseCache = createPageCache()) {
            Path mapFile1 = testDirectory.createFile("mapFile1");
            Path mapFile2 = testDirectory.createFile("mapFile2");
            Path mapFile3 = testDirectory.createFile("mapFile3");
            Path mapFile4 = testDirectory.createFile("mapFile4");
            PagedFile pagedFile = databasePageCache.map(mapFile1, PAGE_SIZE, DATABASE_NAME);
            PagedFile pagedFile2 = databasePageCache.map(mapFile2, PAGE_SIZE, DATABASE_NAME);
            PagedFile pagedFile3 = anotherDatabaseCache.map(mapFile3, PAGE_SIZE, DATABASE_NAME);
            PagedFile pagedFile4 = anotherDatabaseCache.map(mapFile4, PAGE_SIZE, DATABASE_NAME);

            List<PagedFile> pagedFiles = databasePageCache.listExistingMappings();
            assertThat(pagedFiles).hasSize(2);
            assertThat(pagedFiles).contains(pagedFile, pagedFile2);

            List<PagedFile> anotherPagedFiles = anotherDatabaseCache.listExistingMappings();
            assertThat(anotherPagedFiles).hasSize(2);
            assertThat(anotherPagedFiles).contains(pagedFile3, pagedFile4);
        }
    }

    @Test
    void existingMappingRestrictedToDatabaseMappedFiles() throws IOException {
        try (DatabasePageCache anotherDatabaseCache = createPageCache()) {
            Path mapFile1 = testDirectory.createFile("mapFile1");
            Path mapFile2 = testDirectory.createFile("mapFile2");
            Path mapFile3 = testDirectory.createFile("mapFile3");
            Path mapFile4 = testDirectory.createFile("mapFile4");
            databasePageCache.map(mapFile1, PAGE_SIZE, DATABASE_NAME);
            databasePageCache.map(mapFile2, PAGE_SIZE, DATABASE_NAME);
            anotherDatabaseCache.map(mapFile3, PAGE_SIZE, DATABASE_NAME);
            anotherDatabaseCache.map(mapFile4, PAGE_SIZE, DATABASE_NAME);

            assertTrue(databasePageCache.getExistingMapping(mapFile1).isPresent());
            assertTrue(databasePageCache.getExistingMapping(mapFile2).isPresent());
            assertFalse(databasePageCache.getExistingMapping(mapFile3).isPresent());
            assertFalse(databasePageCache.getExistingMapping(mapFile4).isPresent());

            assertFalse(anotherDatabaseCache.getExistingMapping(mapFile1).isPresent());
            assertFalse(anotherDatabaseCache.getExistingMapping(mapFile2).isPresent());
            assertTrue(anotherDatabaseCache.getExistingMapping(mapFile3).isPresent());
            assertTrue(anotherDatabaseCache.getExistingMapping(mapFile4).isPresent());
        }
    }

    @Test
    void throwOnMultipleCloseAttempts() {
        databasePageCache.close();
        assertThrows(IllegalStateException.class, () -> databasePageCache.close());
        databasePageCache = null;
    }

    @Test
    void flushOnlyAffectsDatabaseRelatedFiles() throws IOException {
        try (DatabasePageCache anotherDatabaseCache = createPageCache()) {
            Path mapFile1 = testDirectory.createFile("mapFile1");
            Path mapFile2 = testDirectory.createFile("mapFile2");
            Path mapFile3 = testDirectory.createFile("mapFile3");
            Path mapFile4 = testDirectory.createFile("mapFile4");
            databasePageCache.map(mapFile1, PAGE_SIZE, DATABASE_NAME);
            databasePageCache.map(mapFile2, PAGE_SIZE, DATABASE_NAME);
            anotherDatabaseCache.map(mapFile3, PAGE_SIZE, DATABASE_NAME);
            anotherDatabaseCache.map(mapFile4, PAGE_SIZE, DATABASE_NAME);

            databasePageCache.flushAndForce(DatabaseFlushEvent.NULL);

            List<PagedFile> pagedFiles = pagedFileMapper.getPagedFiles();
            PagedFile originalPagedFile1 = findPagedFile(pagedFiles, mapFile1);
            PagedFile originalPagedFile2 = findPagedFile(pagedFiles, mapFile2);
            PagedFile originalPagedFile3 = findPagedFile(pagedFiles, mapFile3);
            PagedFile originalPagedFile4 = findPagedFile(pagedFiles, mapFile4);

            verify(originalPagedFile1).flushAndForce(FileFlushEvent.NULL);
            verify(originalPagedFile2).flushAndForce(FileFlushEvent.NULL);
            verify(originalPagedFile3, never()).flushAndForce(FileFlushEvent.NULL);
            verify(originalPagedFile4, never()).flushAndForce(FileFlushEvent.NULL);
        }
    }

    @Test
    void flushWithLimiterOnlyAffectsDatabaseRelatedFiles() throws IOException {
        try (DatabasePageCache anotherDatabaseCache = createPageCache()) {
            Path mapFile1 = testDirectory.createFile("mapFile1");
            Path mapFile2 = testDirectory.createFile("mapFile2");
            Path mapFile3 = testDirectory.createFile("mapFile3");
            Path mapFile4 = testDirectory.createFile("mapFile4");
            databasePageCache.map(mapFile1, PAGE_SIZE, DATABASE_NAME);
            databasePageCache.map(mapFile2, PAGE_SIZE, DATABASE_NAME);
            anotherDatabaseCache.map(mapFile3, PAGE_SIZE, DATABASE_NAME);
            anotherDatabaseCache.map(mapFile4, PAGE_SIZE, DATABASE_NAME);

            databasePageCache.flushAndForce(DatabaseFlushEvent.NULL);

            List<PagedFile> pagedFiles = pagedFileMapper.getPagedFiles();
            PagedFile originalPagedFile1 = findPagedFile(pagedFiles, mapFile1);
            PagedFile originalPagedFile2 = findPagedFile(pagedFiles, mapFile2);
            PagedFile originalPagedFile3 = findPagedFile(pagedFiles, mapFile3);
            PagedFile originalPagedFile4 = findPagedFile(pagedFiles, mapFile4);

            verify(originalPagedFile1).flushAndForce(FileFlushEvent.NULL);
            verify(originalPagedFile2).flushAndForce(FileFlushEvent.NULL);
            verify(originalPagedFile3, never()).flushAndForce(FileFlushEvent.NULL);
            verify(originalPagedFile4, never()).flushAndForce(FileFlushEvent.NULL);
        }
    }

    @Test
    void closingFileCloseCacheMapping() throws IOException {
        Path mapFile1 = testDirectory.createFile("mapFile1");
        Path mapFile2 = testDirectory.createFile("mapFile2");
        PagedFile pagedFile1 = databasePageCache.map(mapFile1, PAGE_SIZE, DATABASE_NAME);
        PagedFile pagedFile2 = databasePageCache.map(mapFile2, PAGE_SIZE, DATABASE_NAME);

        assertEquals(2, databasePageCache.listExistingMappings().size());

        pagedFile1.close();

        assertEquals(1, databasePageCache.listExistingMappings().size());

        pagedFile2.close();

        assertTrue(databasePageCache.listExistingMappings().isEmpty());
    }

    @Test
    void fileMapperListenerNotification() throws IOException {
        Path mapFile1 = testDirectory.createFile("mapFile1");
        Path mapFile2 = testDirectory.createFile("mapFile2");
        Path mapFile3 = testDirectory.createFile("mapFile3");
        Path mapFile4 = testDirectory.createFile("mapFile4");

        var mappedFile1 = databasePageCache.map(mapFile1, PAGE_SIZE, DATABASE_NAME);

        var mapListener1 = new TestFileMappedListener();
        var mapListener2 = new TestFileMappedListener();
        databasePageCache.registerFileMappedListener(mapListener1);
        databasePageCache.registerFileMappedListener(mapListener2);

        var mappedFile2 = databasePageCache.map(mapFile2, PAGE_SIZE, DATABASE_NAME);
        var mappedFile4 = databasePageCache.map(mapFile4, PAGE_SIZE, DATABASE_NAME);

        assertThat(mapListener1.getMappedHistory()).containsExactly(mappedFile2, mappedFile4);
        assertThat(mapListener2.getMappedHistory()).containsExactly(mappedFile2, mappedFile4);
        assertThat(mapListener1.getUnmappedHistory()).isEmpty();
        assertThat(mapListener2.getUnmappedHistory()).isEmpty();

        mappedFile2.close();

        assertThat(mapListener1.getMappedHistory()).containsExactly(mappedFile2, mappedFile4);
        assertThat(mapListener2.getMappedHistory()).containsExactly(mappedFile2, mappedFile4);
        assertThat(mapListener1.getUnmappedHistory()).containsExactly(mappedFile2);
        assertThat(mapListener2.getUnmappedHistory()).contains(mappedFile2);

        mappedFile1.close();

        assertThat(mapListener1.getMappedHistory()).containsExactly(mappedFile2, mappedFile4);
        assertThat(mapListener2.getMappedHistory()).containsExactly(mappedFile2, mappedFile4);
        assertThat(mapListener1.getUnmappedHistory()).containsExactly(mappedFile2, mappedFile1);
        assertThat(mapListener2.getUnmappedHistory()).contains(mappedFile2, mappedFile1);
    }

    @Test
    void shouldFlushUnflushedFilesUsingFlushGuard() throws IOException {
        // Given
        Path mapFile1 = testDirectory.createFile("mapFile1");
        Path mapFile2 = testDirectory.createFile("mapFile2");

        PagedFile pf1 = databasePageCache.map(mapFile1, PAGE_SIZE, DATABASE_NAME);
        PagedFile pf2 = databasePageCache.map(mapFile2, PAGE_SIZE, DATABASE_NAME);
        List<PagedFile> pagedFiles = pagedFileMapper.getPagedFiles();
        PagedFile originalPagedFile1 = findPagedFile(pagedFiles, mapFile1);
        PagedFile originalPagedFile2 = findPagedFile(pagedFiles, mapFile2);

        // When
        DatabasePageCache.FlushGuard flushGuard = databasePageCache.flushGuard(DatabaseFlushEvent.NULL);
        pf1.flushAndForce(FileFlushEvent.NULL);
        // Then
        verify(originalPagedFile1).flushAndForce(FileFlushEvent.NULL);
        verify(originalPagedFile2, never()).flushAndForce(FileFlushEvent.NULL);
        // When (close)
        flushGuard.flushUnflushed();
        // Then
        verify(originalPagedFile2).flushAndForce(FileFlushEvent.NULL);
    }

    @Test
    void shouldNotFlushOtherFilesOnExceptionDuringGuardFlush() throws IOException {
        // Given
        Path mapFile1 = testDirectory.createFile("mapFile1");
        Path mapFile2 = testDirectory.createFile("mapFile2");

        databasePageCache.map(mapFile1, PAGE_SIZE, DATABASE_NAME);
        databasePageCache.map(mapFile2, PAGE_SIZE, DATABASE_NAME);
        List<PagedFile> pagedFiles = pagedFileMapper.getPagedFiles();
        PagedFile originalPagedFile1 = findPagedFileByMappingOrder(0, pagedFiles);
        PagedFile originalPagedFile2 = findPagedFileByMappingOrder(1, pagedFiles);

        doThrow(new IOException("test")).when(originalPagedFile1).flushAndForce(FileFlushEvent.NULL);

        // When
        DatabasePageCache.FlushGuard flushGuard = databasePageCache.flushGuard(DatabaseFlushEvent.NULL);
        // Then
        assertThatThrownBy(flushGuard::flushUnflushed)
                .isInstanceOf(IOException.class)
                .hasMessage("test");
        verify(originalPagedFile2, never()).flushAndForce(FileFlushEvent.NULL); // This depends on mapped order
    }

    @Test
    void shouldNotFlushFilesOnExceptionDuringGuard() throws IOException {
        // Given
        Path mapFile1 = testDirectory.createFile("mapFile1");
        Path mapFile2 = testDirectory.createFile("mapFile2");

        PagedFile pf = databasePageCache.map(mapFile1, PAGE_SIZE, DATABASE_NAME);
        databasePageCache.map(mapFile2, PAGE_SIZE, DATABASE_NAME);
        List<PagedFile> pagedFiles = pagedFileMapper.getPagedFiles();
        PagedFile originalPagedFile1 = findPagedFile(pagedFiles, mapFile1);
        PagedFile originalPagedFile2 = findPagedFile(pagedFiles, mapFile2);

        doThrow(new IOException("test")).when(originalPagedFile1).flushAndForce(FileFlushEvent.NULL);

        // When
        assertThatThrownBy(() -> flushFileUnderGuard(pf))
                .isInstanceOf(IOException.class)
                .hasMessage("test");
        // Then
        verify(originalPagedFile2, never()).flushAndForce(FileFlushEvent.NULL); // This depends on mapped order
    }

    @Test
    void shouldTriggerMappedListenerEventOnFirstFileMapOnly() throws IOException {
        // Given
        var mapFile = testDirectory.createFile("mapFile");
        var listener = new TestFileMappedListener();
        databasePageCache.registerFileMappedListener(listener);
        var firstMapping = databasePageCache.map(mapFile, PAGE_SIZE, DATABASE_NAME);
        assertThat(listener.mappedHistory).isEqualTo(List.of(firstMapping));

        // When
        databasePageCache.map(mapFile, PAGE_SIZE, DATABASE_NAME);

        // Then
        assertThat(listener.mappedHistory).isEqualTo(List.of(firstMapping));
    }

    @Test
    void shouldTriggerUnmappedListenerEventOnLastFileUnmapOnly() throws IOException {
        // Given
        var mapFile = testDirectory.createFile("mapFile");
        var listener = new TestFileMappedListener();
        databasePageCache.registerFileMappedListener(listener);
        var firstMapping = databasePageCache.map(mapFile, PAGE_SIZE, DATABASE_NAME);
        var secondMapping = databasePageCache.map(mapFile, PAGE_SIZE, DATABASE_NAME);
        firstMapping.close();
        assertThat(listener.mappedHistory).isEqualTo(List.of(firstMapping));
        assertThat(listener.unmappedHistory).isEmpty();

        // When
        secondMapping.close();

        // Then
        assertThat(listener.unmappedHistory).isEqualTo(List.of(firstMapping));
    }

    @Test
    void shouldCloseMultipleMappingsOnSameFileOnPageCacheClose() throws IOException {
        // Given
        var mapFile = testDirectory.createFile("mapFile");
        databasePageCache.map(mapFile, PAGE_SIZE, DATABASE_NAME);
        databasePageCache.map(mapFile, PAGE_SIZE, DATABASE_NAME);
        assertThat(pagedFileMapper.pagedFiles.size()).isEqualTo(1);

        // When
        databasePageCache.close();
        databasePageCache = null;

        // Then
        for (var pagedFile : pagedFileMapper.pagedFiles) {
            verify(pagedFile, times(2)).close();
        }
    }

    private PagedFile findPagedFileByMappingOrder(int index, List<PagedFile> pagedFiles) {
        var dbPagedFile = databasePageCache.listExistingMappings().get(index);
        return findPagedFile(pagedFiles, dbPagedFile.path());
    }

    private void flushFileUnderGuard(PagedFile file) throws IOException {
        DatabasePageCache.FlushGuard flushGuard = databasePageCache.flushGuard(DatabaseFlushEvent.NULL);
        file.flushAndForce(FileFlushEvent.NULL);
        flushGuard.flushUnflushed();
    }

    private DatabasePageCache createPageCache() {
        return new DatabasePageCache(globalPageCache, DISABLED, EMPTY_STORAGE, Config.defaults());
    }

    private static PagedFile findPagedFile(List<PagedFile> pagedFiles, Path mapFile) {
        return pagedFiles.stream()
                .filter(pagedFile -> pagedFile.path().equals(mapFile))
                .findFirst()
                .orElseThrow(() ->
                        new IllegalStateException(format("Mapped paged file '%s' not found", mapFile.getFileName())));
    }

    private static class PagedFileAnswer implements Answer<PagedFile> {
        private final List<PagedFile> pagedFiles = new ArrayList<>();

        @Override
        public PagedFile answer(InvocationOnMock invocation) {
            // This behaviour makes this mocked page cache behave like MuninnPageCache regarding mapping
            // a file multiple times
            Path path = invocation.getArgument(0);
            for (PagedFile pagedFile : pagedFiles) {
                if (pagedFile.path().equals(path)) {
                    return pagedFile;
                }
            }

            PagedFile pagedFile = mock(PagedFile.class);
            when(pagedFile.path()).thenReturn(path);
            pagedFiles.add(pagedFile);
            return pagedFile;
        }

        List<PagedFile> getPagedFiles() {
            return pagedFiles;
        }
    }

    private static class TestFileMappedListener implements FileMappedListener {
        private final List<PagedFile> mappedHistory = new CopyOnWriteArrayList<>();
        private final List<PagedFile> unmappedHistory = new CopyOnWriteArrayList<>();

        @Override
        public void fileMapped(PagedFile pagedFile) {
            mappedHistory.add(pagedFile);
        }

        @Override
        public void fileUnmapped(PagedFile pagedFile) {
            unmappedHistory.add(pagedFile);
        }

        public List<PagedFile> getMappedHistory() {
            return mappedHistory;
        }

        public List<PagedFile> getUnmappedHistory() {
            return unmappedHistory;
        }
    }
}
