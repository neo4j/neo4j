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
package org.neo4j.cypher.operations;

import static org.neo4j.cypher.operations.CypherCoercions.asStorableValueOrNull;
import static org.neo4j.values.SequenceValue.IterationPreference.RANDOM_ACCESS;
import static org.neo4j.values.storable.Values.NO_VALUE;

import java.util.HashSet;
import java.util.Set;
import org.eclipse.collections.api.factory.primitive.LongSets;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.neo4j.exceptions.CypherTypeException;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.EntityFilterPredicate;
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.storable.LongArray;
import org.neo4j.values.storable.NumberArray;
import org.neo4j.values.storable.Value;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.UnorderedLongSetListValue;

public final class PropertyIndexQueries {
    private PropertyIndexQueries() {
        throw new UnsupportedOperationException("Don't instantiate!");
    }

    public static EntityFilterPredicate matchEntitySet(AnyValue value) {
        return switch (value) {
            case UnorderedLongSetListValue set -> PropertyIndexQuery.entityFilter(set.primitiveLongSet());
            case LongArray a -> PropertyIndexQuery.entityFilter(LongSets.mutable.of(a.asObject()));
            case NumberArray na -> {
                MutableLongSet set = LongSets.mutable.withInitialCapacity(na.intSize());
                for (int i = 0; i < na.intSize(); i++) {
                    set.add(na.value(i).longValue());
                }
                yield PropertyIndexQuery.entityFilter(set);
            }
            case ListValue list -> {
                // we don't given the set an initial size since in theory
                // calling list.intSize can be O(N).
                MutableLongSet set = LongSets.mutable.empty();
                for (AnyValue anyValue : list) {
                    set.add(CypherFunctions.asLong(anyValue));
                }
                yield PropertyIndexQuery.entityFilter(set);
            }
            default ->
                throw CypherTypeException.expectedCollection(
                        String.valueOf(value), value.prettyPrint(), CypherTypeValueMapper.valueType(value));
        };
    }

    public static PropertyIndexQuery inSetQuery(int property, AnyValue seekValues) {
        return switch (seekValues) {
            case SequenceValue seq
            when seq.iterationPreference() == RANDOM_ACCESS -> {
                Set<Value> seen = new HashSet<>();
                for (int i = 0; i < seq.intSize(); i++) {
                    var value = asStorableValueOrNull(seq.value(i));
                    if (value != null && value != NO_VALUE) {
                        seen.add(value);
                    }
                }
                yield PropertyIndexQuery.inSet(property, seen.toArray(Value[]::new));
            }
            case SequenceValue seq -> {
                var iterator = seq.iterator();
                Set<Value> seen = new HashSet<>();
                while (iterator.hasNext()) {
                    var value = asStorableValueOrNull(iterator.next());
                    if (value != null && value != NO_VALUE) {
                        seen.add(value);
                    }
                }
                yield PropertyIndexQuery.inSet(property, seen.toArray(Value[]::new));
            }
            default ->
                throw CypherTypeException.expectedList(
                        String.valueOf(seekValues),
                        seekValues.prettyPrint(),
                        CypherTypeValueMapper.valueType(seekValues));
        };
    }
}
