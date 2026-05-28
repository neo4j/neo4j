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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.assertj.core.api.Assertions;
import org.neo4j.bolt.testing.messages.BoltWire;

public abstract class AbstractMetadataAssertionBuilder<SELF extends AbstractMetadataAssertionBuilder> {

    private final Map<String, List<Assertion>> assertions;
    private final boolean lenient;

    protected AbstractMetadataAssertionBuilder() {
        this(new HashMap<>(), false);
    }

    protected AbstractMetadataAssertionBuilder(Map<String, List<Assertion>> assertions, boolean lenient) {
        this.assertions = assertions;
        this.lenient = lenient;
    }

    protected static void prettyPrint(StringBuilder builder, Object value) {
        prettyPrint(builder, value, 0);
    }

    private static void prettyPrint(StringBuilder builder, Object value, int level) {
        switch (value) {
            case String str ->
                builder.append("\"").append(str.replace("\"", "\\\"")).append("\"");
            case Collection<?> collection -> prettyPrint(builder, collection, level + 1);
            case Map<?, ?> map -> prettyPrint(builder, (Map<Object, Object>) map, level + 1);
            case null -> builder.append("null");
            default -> builder.append(value);
        }
    }

    private static void prettyPrint(StringBuilder builder, Collection<?> collection, int level) {
        builder.append("[");
        {
            var i = 0;
            for (var element : collection) {
                if (i++ != 0) {
                    builder.append(',');
                }
                builder.append('\n');
                builder.append("  ".repeat(level + 1));

                prettyPrint(builder, element, level + 1);
            }
        }
        builder.append('\n');
        builder.append("  ".repeat(level));
        builder.append(']');
    }

    private static void prettyPrint(StringBuilder builder, Map<?, ?> map, int level) {
        builder.append("{");
        {
            var i = 0;
            for (var entry : map.entrySet()) {
                if (i++ != 0) {
                    builder.append(',');
                }
                builder.append('\n');
                builder.append("  ".repeat(level + 1));

                prettyPrint(builder, entry.getKey(), level + 1);
                builder.append(": ");
                prettyPrint(builder, entry.getValue(), level + 1);
            }
        }
        builder.append('\n');
        builder.append("  ".repeat(Math.max(0, level - 1)));
        builder.append('}');
    }

    protected List<String> matchingKeys(BoltWire wire) {
        return this.assertions.entrySet().stream()
                .filter(entry -> entry.getValue().stream().anyMatch(assertion -> assertion.applies(wire)))
                .map(Entry::getKey)
                .toList();
    }

    protected List<String> ignoredKeys(BoltWire wire) {
        return this.assertions.entrySet().stream()
                .filter(entry -> entry.getValue().stream().noneMatch(assertion -> assertion.applies(wire)))
                .map(Entry::getKey)
                .toList();
    }

    public void evaluate(BoltWire wire, Map<String, Object> meta) {
        var applicableAssertions = this.assertions.entrySet().stream()
                .map(entry -> {
                    var assertions = entry.getValue().stream()
                            .filter(assertion -> assertion.applies(wire))
                            .toList();

                    return Map.entry(entry.getKey(), assertions);
                })
                .filter(entry -> !entry.getValue().isEmpty())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (applicableAssertions.isEmpty()) {
            var msg =
                    "No assertions apply to this wire implementation - Check your test configuration and ensure that at least one assertion is present"
                            + "\n"
                            + "Received metadata map:\n"
                            + "    "
                            + meta.toString();

            throw new IllegalStateException(msg);
        }

        if (!this.lenient) {

            var unexpected = meta.entrySet().stream()
                    .filter(entry -> !applicableAssertions.containsKey(entry.getKey()))
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

            if (!unexpected.isEmpty()) {
                var message = new StringBuilder("One or more keys were not expected:\n");
                message.append("  [\"")
                        .append(String.join("\", \"", unexpected.keySet()))
                        .append("\"]\n\n");
                message.append("Unexpected portion:\n  ");
                prettyPrint(message, unexpected, 1);
                message.append("\n\n").append("Within component:\n  ");
                prettyPrint(message, meta, 1);

                throw new AssertionError(message.toString());
            }
        }

        for (var entry : applicableAssertions.entrySet()) {
            var key = entry.getKey();
            var assertions = entry.getValue();

            try {
                Assertions.assertThat(meta).hasEntrySatisfying(key, value -> {
                    for (var assertion : assertions) {
                        assertion.accept(wire, value);
                    }
                });
            } catch (AssertionError e) {
                throw new AssertionError("Assertion failed for key \"" + key + "\": " + e.getMessage(), e);
            }
        }
    }

    public Consumer<Map<String, Object>> wrapWithConsumer(BoltWire wire) {
        return meta -> {
            this.evaluate(wire, meta);
        };
    }

    protected abstract SELF create(Map<String, List<Assertion>> assertions, boolean lenient);

    protected SELF registerAssertion(String key, Assertion assertion) {
        var copy = new HashMap<>(this.assertions);

        var assertions = copy.computeIfAbsent(key, k -> new ArrayList<>());
        assertions.add(assertion);

        return this.create(copy, this.lenient);
    }

    protected SELF registerAssertion(String key, Consumer<Object> assertion) {
        return this.registerAssertion(key, (wire, meta) -> assertion.accept(meta));
    }

    public SELF assertLeniently() {
        return this.create(this.assertions, true);
    }

    @FunctionalInterface
    protected interface Assertion {

        default boolean applies(BoltWire wire) {
            return true;
        }

        void accept(BoltWire wire, Object meta);

        static Assertion wrap(Consumer<Object> assertion) {
            return (wire, meta) -> assertion.accept(meta);
        }

        static Assertion wrap(BiConsumer<BoltWire, Object> assertion) {
            return (wire, meta) -> assertion.accept(wire, meta);
        }

        default Assertion withCondition(Predicate<BoltWire> condition) {
            var assertion = this;

            return new Assertion() {
                @Override
                public void accept(BoltWire wire, Object meta) {
                    assertion.accept(wire, meta);
                }

                @Override
                public boolean applies(BoltWire wire) {
                    return assertion.applies(wire) && condition.test(wire);
                }
            };
        }
    }
}
