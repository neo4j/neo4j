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
package org.neo4j.kernel.impl.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.enrichment.ApplyEnrichmentStrategy;
import org.neo4j.storageengine.api.txstate.TransactionStateBehaviour;

class KernelTransactionsStateBehaviourTest {
    @Test
    void shouldForwardSupportsOntologyFromStorageEngine() {
        var storageEngine = mock(StorageEngine.class);
        var engineBehaviour = mock(TransactionStateBehaviour.class);
        when(storageEngine.transactionStateBehaviour()).thenReturn(engineBehaviour);
        var behaviour = new KernelTransactionsStateBehaviour(storageEngine, ApplyEnrichmentStrategy.NO_ENRICHMENT);

        when(engineBehaviour.supportsOntology()).thenReturn(true);
        assertThat(behaviour.supportsOntology()).isTrue();

        when(engineBehaviour.supportsOntology()).thenReturn(false);
        assertThat(behaviour.supportsOntology()).isFalse();
    }
}
