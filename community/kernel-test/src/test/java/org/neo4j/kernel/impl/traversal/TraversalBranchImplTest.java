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
package org.neo4j.kernel.impl.traversal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.traversal.Evaluation.INCLUDE_AND_CONTINUE;
import static org.neo4j.internal.helpers.collection.Iterables.resourceIterable;
import static org.neo4j.internal.helpers.collection.Iterators.resourceIterator;

import java.util.Collections;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.graphdb.traversal.TraversalContext;

class TraversalBranchImplTest {
    @SuppressWarnings("unchecked")
    @Test
    void shouldExpandOnFirstAccess() {
        // GIVEN
        final var parent = mock(TraversalBranch.class);
        final var source = mock(Node.class);
        final var branch = new TraversalBranchImpl(parent, source);

        final var expander = mock(PathExpander.class);

        final var closed = new MutableBoolean();
        final var iterable = resourceIterable(() -> resourceIterator(Collections.emptyIterator(), closed::setTrue));

        when(expander.expand(eq(branch), any(BranchState.class))).thenReturn(iterable);
        final var context = mock(TraversalContext.class);
        when(context.evaluate(eq(branch), isNull())).thenReturn(INCLUDE_AND_CONTINUE);

        // WHEN initializing
        branch.initialize(expander, context);

        // THEN the branch should not be expanded
        verifyNoInteractions(source);

        // and WHEN actually traversing from it
        branch.next(expander, context);

        // THEN we should expand it
        verify(expander).expand(any(Path.class), any(BranchState.class));
        // and the resources closed when the expansion is reset
        assertThat(closed.getValue()).isTrue();
    }
}
