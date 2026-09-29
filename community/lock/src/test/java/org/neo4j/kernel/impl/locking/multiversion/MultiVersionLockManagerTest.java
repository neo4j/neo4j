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
package org.neo4j.kernel.impl.locking.multiversion;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceType;

class MultiVersionLockManagerTest {
    private final LockManager lockManager = mock(LockManager.class);
    private final LockManager.Client deletageLockClient = mock(LockManager.Client.class);
    private final MultiVersionLockManager versionLockManager = new MultiVersionLockManager(lockManager);

    @BeforeEach
    void setUp() {
        when(lockManager.newClient()).thenReturn(deletageLockClient);
    }

    @Test
    void tryAcquireAndReleaseSharedPageLocks() {
        var lockClient = versionLockManager.newClient();

        var resourceTypes = ResourceType.values();
        for (int i = 0; i < resourceTypes.length; i++) {
            lockClient.trySharedLock(resourceTypes[i], i);
        }

        verify(deletageLockClient).trySharedLock(eq(ResourceType.PAGE), anyLong());
        verifyNoMoreInteractions(deletageLockClient);

        for (int i = 0; i < resourceTypes.length; i++) {
            lockClient.releaseShared(resourceTypes[i], i);
        }

        verify(deletageLockClient).releaseShared(eq(ResourceType.PAGE), anyLong());
        verifyNoMoreInteractions(deletageLockClient);
    }

    @Test
    void tryAcquireAndReleaseExclusivePageLocks() {
        var lockClient = versionLockManager.newClient();

        var resourceTypes = ResourceType.values();
        for (int i = 0; i < resourceTypes.length; i++) {
            lockClient.tryExclusiveLock(resourceTypes[i], i);
        }

        verify(deletageLockClient).tryExclusiveLock(eq(ResourceType.PAGE), anyLong());
        verify(deletageLockClient).tryExclusiveLock(eq(ResourceType.DENSE_VALIDATION), anyLong());
        verifyNoMoreInteractions(deletageLockClient);

        for (int i = 0; i < resourceTypes.length; i++) {
            lockClient.releaseExclusive(resourceTypes[i], i);
        }

        verify(deletageLockClient).releaseExclusive(eq(ResourceType.PAGE), anyLong());
        verify(deletageLockClient).releaseExclusive(eq(ResourceType.LABEL), anyLong());
        verify(deletageLockClient).releaseExclusive(eq(ResourceType.RELATIONSHIP_TYPE), anyLong());
        verify(deletageLockClient).releaseExclusive(eq(ResourceType.INDEX_ENTRY), anyLong());
        verify(deletageLockClient).releaseExclusive(eq(ResourceType.SCHEMA_NAME), anyLong());
        verify(deletageLockClient).releaseExclusive(eq(ResourceType.DENSE_VALIDATION), anyLong());
        verify(deletageLockClient).releaseExclusive(eq(ResourceType.ONTOLOGY), anyLong());
        verifyNoMoreInteractions(deletageLockClient);
    }

    @Test
    void acquireAndReleaseExclusivePageLocks() {
        var lockClient = versionLockManager.newClient();

        var resourceTypes = ResourceType.values();
        for (int i = 0; i < resourceTypes.length; i++) {
            lockClient.acquireExclusive(LockTracer.NONE, resourceTypes[i], i);
        }

        verify(deletageLockClient).acquireExclusive(eq(LockTracer.NONE), eq(ResourceType.PAGE), anyLong());
        verify(deletageLockClient).acquireExclusive(eq(LockTracer.NONE), eq(ResourceType.LABEL), anyLong());
        verify(deletageLockClient).acquireExclusive(eq(LockTracer.NONE), eq(ResourceType.RELATIONSHIP_TYPE), anyLong());
        verify(deletageLockClient).acquireExclusive(eq(LockTracer.NONE), eq(ResourceType.INDEX_ENTRY), anyLong());
        verify(deletageLockClient).acquireExclusive(eq(LockTracer.NONE), eq(ResourceType.SCHEMA_NAME), anyLong());
        verify(deletageLockClient).acquireExclusive(eq(LockTracer.NONE), eq(ResourceType.DENSE_VALIDATION), anyLong());
        verify(deletageLockClient).acquireExclusive(eq(LockTracer.NONE), eq(ResourceType.ONTOLOGY), anyLong());
        verifyNoMoreInteractions(deletageLockClient);

        for (int i = 0; i < resourceTypes.length; i++) {
            lockClient.releaseExclusive(resourceTypes[i], i);
        }

        verify(deletageLockClient).releaseExclusive(eq(ResourceType.PAGE), anyLong());
        verify(deletageLockClient).releaseExclusive(eq(ResourceType.LABEL), anyLong());
        verify(deletageLockClient).releaseExclusive(eq(ResourceType.RELATIONSHIP_TYPE), anyLong());
        verify(deletageLockClient).releaseExclusive(eq(ResourceType.INDEX_ENTRY), anyLong());
        verify(deletageLockClient).releaseExclusive(eq(ResourceType.SCHEMA_NAME), anyLong());
        verify(deletageLockClient).releaseExclusive(eq(ResourceType.DENSE_VALIDATION), anyLong());
        verify(deletageLockClient).releaseExclusive(eq(ResourceType.ONTOLOGY), anyLong());
        verifyNoMoreInteractions(deletageLockClient);
    }

    @Test
    void acquireAndReleaseSharedPageLocks() {
        var lockClient = versionLockManager.newClient();

        var resourceTypes = ResourceType.values();
        for (int i = 0; i < resourceTypes.length; i++) {
            lockClient.acquireShared(LockTracer.NONE, resourceTypes[i], i);
        }

        verify(deletageLockClient).acquireShared(eq(LockTracer.NONE), eq(ResourceType.PAGE), anyLong());
        verifyNoMoreInteractions(deletageLockClient);

        for (int i = 0; i < resourceTypes.length; i++) {
            lockClient.releaseShared(resourceTypes[i], i);
        }

        verify(deletageLockClient).releaseShared(eq(ResourceType.PAGE), anyLong());
        verifyNoMoreInteractions(deletageLockClient);
    }
}
