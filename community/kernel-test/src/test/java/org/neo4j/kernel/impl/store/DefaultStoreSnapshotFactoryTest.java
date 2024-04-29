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
package org.neo4j.kernel.impl.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.collection.Dependencies.dependenciesOf;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.LatestCheckpointInfo;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.logging.internal.DatabaseLogProvider;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.storageengine.api.StoreResource;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class DefaultStoreSnapshotFactoryTest {
    @Inject
    public TestDirectory testDirectory;

    private DefaultStoreSnapshotFactory defaultStoreSnapshotFactory;
    private DatabaseLayout databaseLayout;
    private StoreFileListing.Builder fileListingBuilder;
    private StoreCopyCheckPointMutex storeCopyCheckPointMutex;

    @BeforeEach
    void setUp() {
        var database = mock(Database.class);
        fileListingBuilder = mock(StoreFileListing.Builder.class, CALLS_REAL_METHODS);
        databaseLayout = DatabaseLayout.ofFlat(testDirectory.directory("neo4j", "data", "databases"));
        when(database.getDatabaseLayout()).thenReturn(databaseLayout);
        var availabilityGuard = mock(DatabaseAvailabilityGuard.class);
        when(availabilityGuard.isAvailable()).thenReturn(true);
        var storeFileListing = mock(StoreFileListing.class);
        when(storeFileListing.builder()).thenReturn(fileListingBuilder);
        when(database.getStoreFileListing()).thenReturn(storeFileListing);
        when(database.getInternalLogProvider()).thenReturn(DatabaseLogProvider.nullDatabaseLogProvider());
        var checkPointer = mock(CheckPointer.class);
        when(checkPointer.latestCheckPointInfo()).thenReturn(LatestCheckpointInfo.UNKNOWN_CHECKPOINT_INFO);
        when(database.getDependencyResolver()).thenReturn(dependenciesOf(checkPointer));
        when(database.getDatabaseAvailabilityGuard()).thenReturn(availabilityGuard);
        storeCopyCheckPointMutex = new StoreCopyCheckPointMutex();
        when(database.getStoreCopyCheckPointMutex()).thenReturn(storeCopyCheckPointMutex);
        defaultStoreSnapshotFactory = new DefaultStoreSnapshotFactory(database, testDirectory.getFileSystem());
    }

    @Test
    void shouldHandleEmptyListOfFilesForEachType() throws Exception {
        setExpectedFiles(new StoreFileMetadata[0]);
        var prepareStoreCopyFiles =
                defaultStoreSnapshotFactory.createStoreSnapshot().get();
        var files = prepareStoreCopyFiles.recoverableFiles();
        var atomicFilesSnapshotLength =
                prepareStoreCopyFiles.unrecoverableFiles().count();
        assertEquals(0, files.length);
        assertEquals(0, atomicFilesSnapshotLength);
    }

    private void setExpectedFiles(StoreFileMetadata[] expectedFiles) throws IOException {
        doAnswer(invocation -> Iterators.asResourceIterator(Iterators.iterator(expectedFiles)))
                .when(fileListingBuilder)
                .build();
    }

    @Test
    void shouldReturnExpectedListOfFileNamesForEachType() throws Exception {
        // given
        var expectedFiles = new StoreFileMetadata[] {
            new StoreFileMetadata(databaseLayout.file("a"), 1), new StoreFileMetadata(databaseLayout.file("b"), 2)
        };
        setExpectedFiles(expectedFiles);

        // when
        var prepareStoreCopyFiles =
                defaultStoreSnapshotFactory.createStoreSnapshot().get();
        var files = prepareStoreCopyFiles.recoverableFiles();
        var atomicFilesSnapshot = prepareStoreCopyFiles.unrecoverableFiles().toArray(StoreResource[]::new);

        // then
        var expectedFilesConverted =
                Arrays.stream(expectedFiles).map(StoreFileMetadata::path).toArray(Path[]::new);
        var expectedAtomicFilesConverted = Arrays.stream(expectedFiles)
                .map(f ->
                        new StoreResource(f.path(), getRelativePath(f), f.recordSize(), testDirectory.getFileSystem()))
                .toArray(StoreResource[]::new);
        assertArrayEquals(expectedFilesConverted, files);
        assertEquals(expectedAtomicFilesConverted.length, atomicFilesSnapshot.length);
        for (var i = 0; i < expectedAtomicFilesConverted.length; i++) {
            StoreResource expected = expectedAtomicFilesConverted[i];
            StoreResource actual = atomicFilesSnapshot[i];
            var relativePath = databaseLayout.databaseDirectory().relativize(actual.path());
            assertEquals(expected.relativePath(), relativePath.toString());
            assertEquals(expected.recordSize(), actual.recordSize());
        }
    }

    @Test
    void shouldReleaseMutexAndResourcesOnSnapshotFailure() throws IOException {
        // given
        doThrow(RuntimeException.class).when(fileListingBuilder).build();

        // when
        assertThatThrownBy(
                        () -> defaultStoreSnapshotFactory.createStoreSnapshot().get())
                .isInstanceOf(RuntimeException.class);

        // then it should now be possible to acquire the checkpoint mutex
        var checkpointMutex = storeCopyCheckPointMutex.tryCheckPoint();
        assertThat(checkpointMutex).isNotNull();
        checkpointMutex.close();
    }

    private String getRelativePath(StoreFileMetadata f) {
        return databaseLayout.databaseDirectory().relativize(f.path()).toString();
    }
}
