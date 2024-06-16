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
package org.neo4j.shell.parameter;

import static org.neo4j.cypherdsl.core.Cypher.duration;
import static org.neo4j.cypherdsl.core.Cypher.literalOf;
import static org.neo4j.cypherdsl.core.Cypher.point;
import static org.neo4j.cypherdsl.core.Cypher.sortedMapOf;

import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.neo4j.cypherdsl.core.Cypher;
import org.neo4j.cypherdsl.core.Expression;
import org.neo4j.cypherdsl.core.renderer.Configuration;
import org.neo4j.cypherdsl.core.renderer.GeneralizedRenderer;
import org.neo4j.cypherdsl.core.renderer.Renderer;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.IsoDuration;
import org.neo4j.driver.types.Point;
import org.neo4j.values.storable.DurationValue;

public interface ParameterPrettyRenderer {
    String pretty(Map<String, Value> value);

    static ParameterPrettyRenderer create() {
        return new ParameterPrettyRendererImpl();
    }
}

class ParameterPrettyRendererImpl implements ParameterPrettyRenderer {
    private static final Configuration CYPHER_DSL_PRETTY_CONF = Configuration.newConfig()
            .withPrettyPrint(true)
            .alwaysEscapeNames(false)
            .withIndentStyle(Configuration.IndentStyle.SPACE)
            .build();
    private final GeneralizedRenderer renderer =
            Renderer.getRenderer(CYPHER_DSL_PRETTY_CONF, GeneralizedRenderer.class);

    @Override
    public String pretty(Map<String, Value> value) {
        return renderer.render(asCypherDslAst(value));
    }

    private org.neo4j.cypherdsl.core.Expression asCypherDslAst(Object input) {
        if (input instanceof Value driverValue) {
            final var object = driverValue.asObject();
            if (object == null || object.getClass() != input.getClass()) {
                return asCypherDslAst(object);
            }
        }

        if (input instanceof Map<?, ?> map) {
            return sortedMapOf(map.entrySet().stream()
                    .flatMap(e -> Stream.of(e.getKey(), asCypherDslAst(e.getValue())))
                    .toArray());
        } else if (input instanceof Iterable<?> iterable) {
            return Cypher.listOf(StreamSupport.stream(iterable.spliterator(), false)
                    .map(this::asCypherDslAst)
                    .toList());
        } else if (input instanceof IsoDuration d) {
            // Note, neo4j storable values has prettier rendering, so we use that
            final var neo4jDuration = DurationValue.duration(d.months(), d.days(), d.seconds(), d.nanoseconds());
            return duration(neo4jDuration.prettyPrint());
        } else if (input instanceof Point p) {
            if (Double.isNaN(p.z())) {
                return point(literalMap("srid", p.srid(), "x", p.x(), "y", p.y()));
            } else {
                return point(literalMap("srid", p.srid(), "x", p.x(), "y", p.y(), "z", p.z()));
            }
        } else {
            return literalOf(input);
        }
    }

    private Expression literalMap(Object... entries) {
        Object[] expressions = new Object[entries.length];
        for (var i = 0; i < entries.length; i += 2) {
            expressions[i] = entries[i];
            expressions[i + 1] = Cypher.literalOf(entries[i + 1]);
        }
        return Cypher.mapOf(expressions);
    }
}
