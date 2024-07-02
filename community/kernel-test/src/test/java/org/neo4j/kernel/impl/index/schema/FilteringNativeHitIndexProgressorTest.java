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
package org.neo4j.kernel.impl.index.schema;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unorderedValues;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;

@ExtendWith(RandomExtension.class)
class FilteringNativeHitIndexProgressorTest {
    @Inject
    private RandomSupport random;

    @Test
    void shouldFilterResults() {
        // given
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            // duplicates are fine
            keys.add(random.nextString());
        }

        Seeker<RangeKey, NullValue> cursor = new ResultCursor(keys.iterator());
        NodeValueIterator valueClient = new NodeValueIterator() {
            @Override
            public boolean needsValues() {
                return true;
            }
        };
        PropertyIndexQuery[] predicates = new PropertyIndexQuery[] {mock(PropertyIndexQuery.class)};
        Predicate<String> filter = string -> string.contains("a");
        when(predicates[0].acceptsValue(any(Value.class)))
                .then(invocation -> filter.test(((TextValue) invocation.getArgument(0)).stringValue()));
        try (FilteringNativeHitIndexProgressor<RangeKey> progressor =
                new FilteringNativeHitIndexProgressor<>(cursor, valueClient, predicates)) {
            valueClient.initializeQuery(
                    TestIndexDescriptorFactory.forLabel(0, 0), progressor, false, false, unorderedValues(), predicates);
            List<Long> result = new ArrayList<>();

            // when
            while (valueClient.hasNext()) {
                result.add(valueClient.next());
            }

            // then
            for (int i = 0; i < keys.size(); i++) {
                if (filter.test(keys.get(i))) {
                    assertTrue(result.remove((long) i));
                }
            }
            assertTrue(result.isEmpty());
        }
    }
}
