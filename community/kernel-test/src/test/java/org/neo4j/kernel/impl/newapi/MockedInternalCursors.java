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
package org.neo4j.kernel.impl.newapi;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;

class MockedInternalCursors {
    static InternalCursorFactory mockedInternalCursors() {
        var internalCursors = mock(InternalCursorFactory.class);
        when(internalCursors.allocateNodeCursor()).thenAnswer(i -> mock(DefaultNodeCursor.class));
        when(internalCursors.allocatePropertyCursor()).thenAnswer(i -> mock(DefaultPropertyCursor.class));
        when(internalCursors.allocateStorageNodeCursor()).thenAnswer(i -> mock(StorageNodeCursor.class));
        when(internalCursors.allocateStoragePropertyCursor()).thenAnswer(i -> mock(StoragePropertyCursor.class));
        when(internalCursors.allocateStorageRelationshipTraversalCursor())
                .thenAnswer(i -> mock(StorageRelationshipTraversalCursor.class));
        when(internalCursors.allocateFullAccessNodeCursor()).thenAnswer(i -> mock(FullAccessNodeCursor.class));
        when(internalCursors.allocateFullAccessRelationshipScanCursor())
                .thenAnswer(i -> mock(FullAccessRelationshipScanCursor.class));
        return internalCursors;
    }
}
