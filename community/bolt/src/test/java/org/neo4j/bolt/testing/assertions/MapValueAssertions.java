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
package org.neo4j.bolt.testing.assertions;

import static org.neo4j.values.storable.NoValue.NO_VALUE;

import java.util.function.Consumer;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;

public final class MapValueAssertions extends AbstractAssert<MapValueAssertions, MapValue> {

    private MapValueAssertions(MapValue value) {
        super(value, MapValueAssertions.class);
    }

    public static MapValueAssertions assertThat(MapValue value) {
        return new MapValueAssertions(value);
    }

    public static InstanceOfAssertFactory<MapValue, MapValueAssertions> mapValue() {
        return new InstanceOfAssertFactory<>(MapValue.class, MapValueAssertions::new);
    }

    public MapValueAssertions isEmpty() {
        this.isNotNull();

        if (!this.actual.isEmpty()) {
            failWithMessage("Expected map value to be empty");
        }

        return this;
    }

    public MapValueAssertions isNotEmpty() {
        this.isNotNull();

        if (this.actual.isEmpty()) {
            failWithMessage("Expected map value to contain at least one element");
        }

        return this;
    }

    public MapValueAssertions hasSize(int expected) {
        this.isNotNull();

        if (this.actual.size() != expected) {
            failWithActualExpectedAndMessage(
                    this.actual.size(),
                    expected,
                    "Expected map value to be of size <%d> but was <%d>",
                    expected,
                    this.actual.size());
        }

        return this;
    }

    public MapValueAssertions containsKey(String expected) {
        this.isNotNull();

        if (!this.actual.containsKey(expected)) {
            failWithMessage("Expected map value to contain entry with key <\"%s\">", expected);
        }

        return this;
    }

    public MapValueAssertions doesNotContainKey(String unexpected) {
        this.isNotNull();

        if (this.actual.containsKey(unexpected)) {
            failWithMessage("Expected map value to not contain entry with key <\"%s\">", unexpected);
        }

        return this;
    }

    public MapValueAssertions containsEntry(String key, Consumer<AnyValue> assertions) {
        this.isNotNull();

        var entry = this.actual.get(key);

        if (entry == NO_VALUE) {
            failWithMessage("Expected map value to contain entry with key <\"%s\">", key);
        }

        Assertions.assertThat(entry).satisfies(assertions);

        return this;
    }

    public MapValueAssertions containsEntry(String key, AnyValue expected) {
        return this.containsEntry(key, actual -> Assertions.assertThat(actual).isEqualTo(expected));
    }

    public AnyValueAssertions extractingEntry(String key) {
        this.isNotNull();

        return new AnyValueAssertions(this.actual.get(key));
    }
}
