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
package org.neo4j.kernel.impl.api.index;

import static org.apache.commons.lang3.ArrayUtils.contains;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.kernel.impl.api.index.TestIndexProviderDescriptor.PROVIDER_DESCRIPTOR;

import org.junit.jupiter.api.Test;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.test.Race;

class IndexMapReferenceTest {
    @Test
    void shouldSynchronizeModifications() throws Throwable {
        // given
        IndexMapReference ref = new IndexMapReference();
        IndexProxy[] existing = mockedIndexProxies(5, 0);
        ref.modify(indexMap -> {
            for (IndexProxy indexProxy : existing) {
                indexMap.putIndexProxy(indexProxy);
            }
            return indexMap;
        });

        // when
        Race race = new Race();
        for (int i = 0; i < existing.length; i++) {
            race.addContestant(removeIndexProxy(ref, i), 1);
        }
        IndexProxy[] created = mockedIndexProxies(3, existing.length);
        for (int i = 0; i < existing.length; i++) {
            race.addContestant(putIndexProxy(ref, created[i]), 1);
        }
        race.go();

        // then
        var indexProxies = Iterables.asList(ref.getAllIndexProxies());
        assertFalse(indexProxies.stream().anyMatch(indexProxy -> contains(existing, indexProxy)));
        assertTrue(indexProxies.stream().allMatch(indexProxy -> contains(created, indexProxy)));
    }

    private static Runnable putIndexProxy(IndexMapReference ref, IndexProxy proxy) {
        return () -> ref.modify(indexMap -> {
            indexMap.putIndexProxy(proxy);
            return indexMap;
        });
    }

    private static Runnable removeIndexProxy(IndexMapReference ref, long indexId) {
        return () -> ref.modify(indexMap -> {
            indexMap.removeIndexProxy(indexId);
            return indexMap;
        });
    }

    private static IndexProxy[] mockedIndexProxies(int base, int count) {
        IndexProxy[] existing = new IndexProxy[count];
        for (int i = 0; i < count; i++) {
            existing[i] = mock(IndexProxy.class);
            when(existing[i].getDescriptor())
                    .thenReturn(forSchema(forLabel(base + i, 1), PROVIDER_DESCRIPTOR)
                            .withName("index_" + i)
                            .materialise(i));
        }
        return existing;
    }
}
