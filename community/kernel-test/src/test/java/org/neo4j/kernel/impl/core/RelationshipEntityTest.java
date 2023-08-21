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
package org.neo4j.kernel.impl.core;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;

class RelationshipEntityTest {
    @Test
    void shouldUseCursorForReadingPropertiesIfPresentAndCorrectlyLocated() {
        // given
        InternalTransaction internalTransaction = mockedInternalTransaction();

        long id = 1;
        RelationshipTraversalCursor relationshipTraversalCursor = mock(RelationshipTraversalCursor.class);
        when(relationshipTraversalCursor.isClosed()).thenReturn(false);
        when(relationshipTraversalCursor.relationshipReference()).thenReturn(id);
        when(relationshipTraversalCursor.sourceNodeReference()).thenReturn(2L);
        when(relationshipTraversalCursor.type()).thenReturn(3);
        when(relationshipTraversalCursor.targetNodeReference()).thenReturn(4L);
        RelationshipEntity relationship = new RelationshipEntity(internalTransaction, relationshipTraversalCursor);
        // when
        relationship.getAllProperties(mock(PropertyCursor.class));

        // then
        verify(relationshipTraversalCursor).properties(any(), any());
        verify(internalTransaction.kernelTransaction(), never()).ambientRelationshipCursor();
    }

    @Test
    void shouldNotUseCursorForReadingPropertiesIfPresentButNotCorrectlyLocated() {
        // given
        InternalTransaction internalTransaction = mockedInternalTransaction();

        long id = 1;
        RelationshipTraversalCursor relationshipTraversalCursor = mock(RelationshipTraversalCursor.class);
        when(relationshipTraversalCursor.isClosed()).thenReturn(false);
        when(relationshipTraversalCursor.relationshipReference()).thenReturn(id);
        when(relationshipTraversalCursor.sourceNodeReference()).thenReturn(2L);
        when(relationshipTraversalCursor.type()).thenReturn(3);
        when(relationshipTraversalCursor.targetNodeReference()).thenReturn(4L);
        RelationshipEntity relationship = new RelationshipEntity(internalTransaction, relationshipTraversalCursor);

        // when
        when(relationshipTraversalCursor.relationshipReference()).thenReturn(id + 1);
        relationship.getAllProperties(mock(PropertyCursor.class));

        // then
        verify(relationshipTraversalCursor, never()).properties(any());
        verify(internalTransaction.kernelTransaction()).ambientRelationshipCursor();
    }

    @Test
    void shouldNotUseCursorForReadingPropertiesIfClosed() {
        // given
        InternalTransaction internalTransaction = mockedInternalTransaction();

        long id = 1;
        RelationshipTraversalCursor relationshipTraversalCursor = mock(RelationshipTraversalCursor.class);
        when(relationshipTraversalCursor.isClosed()).thenReturn(true);
        when(relationshipTraversalCursor.relationshipReference()).thenReturn(id);
        when(relationshipTraversalCursor.sourceNodeReference()).thenReturn(2L);
        when(relationshipTraversalCursor.type()).thenReturn(3);
        when(relationshipTraversalCursor.targetNodeReference()).thenReturn(4L);
        RelationshipEntity relationship = new RelationshipEntity(internalTransaction, relationshipTraversalCursor);
        // when
        relationship.getAllProperties(mock(PropertyCursor.class));

        // then
        verify(relationshipTraversalCursor, never()).properties(any());
        verify(internalTransaction.kernelTransaction()).ambientRelationshipCursor();
    }

    @Test
    void shouldNotUseCursorForReadingPropertiesIfNotPresent() {
        // given
        InternalTransaction internalTransaction = mockedInternalTransaction();

        long id = 1;
        RelationshipEntity relationship = new RelationshipEntity(internalTransaction, id, 2, 3, 4);

        // when
        relationship.getAllProperties(mock(PropertyCursor.class));

        // then
        verify(internalTransaction.kernelTransaction()).ambientRelationshipCursor();
    }

    private InternalTransaction mockedInternalTransaction() {
        InternalTransaction internalTransaction = mock(InternalTransaction.class);
        KernelTransaction kernelTransaction = mock(KernelTransaction.class);
        Read dataRead = mock(Read.class);
        RelationshipScanCursor ambientRelationshipScanCursor = mock(RelationshipScanCursor.class);
        when(ambientRelationshipScanCursor.next()).thenReturn(true);
        when(kernelTransaction.dataRead()).thenReturn(dataRead);
        when(kernelTransaction.tokenRead()).thenReturn(mock(TokenRead.class));
        when(kernelTransaction.ambientRelationshipCursor()).thenReturn(ambientRelationshipScanCursor);
        when(internalTransaction.kernelTransaction()).thenReturn(kernelTransaction);
        return internalTransaction;
    }
}
