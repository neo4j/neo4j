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
package org.neo4j.server.queryapi.response.format;

import java.util.Arrays;
import java.util.List;
import org.neo4j.server.queryapi.types.CypherTypes;

public enum View {
    PLAIN_JSON,
    TYPED_JSON(true, CypherTypes.Unsupported, CypherTypes.Vector, CypherTypes.UUID),
    TYPED_JSON_V1x1(true, CypherTypes.UUID),
    TYPED_JSON_V1x2(true);

    private final List<CypherTypes> unsupportedTypes;
    private final boolean typed;

    View(CypherTypes... unsupportedTypes) {
        this(false, unsupportedTypes);
    }

    View(boolean typed, CypherTypes... unsupportedTypes) {
        this.unsupportedTypes = Arrays.asList(unsupportedTypes);
        this.typed = typed;
    }

    public boolean supports(CypherTypes type) {
        return !unsupportedTypes.contains(type);
    }

    public boolean isTyped() {
        return typed;
    }

    public static String labels(View view) {
        if (view.equals(PLAIN_JSON)) {
            return Fieldnames.LABELS;
        } else {
            return Fieldnames._LABELS;
        }
    }

    public static String elementId(View view) {
        if (view.equals(PLAIN_JSON)) {
            return Fieldnames.ELEMENT_ID;
        } else {
            return Fieldnames._ELEMENT_ID;
        }
    }

    public static String properties(View view) {
        if (view.equals(PLAIN_JSON)) {
            return Fieldnames.PROPERTIES;
        } else {
            return Fieldnames._PROPERTIES;
        }
    }

    public static String startNodeElementId(View view) {
        if (view.equals(PLAIN_JSON)) {
            return Fieldnames.START_NODE_ELEMENT_ID;
        } else {
            return Fieldnames._START_NODE_ELEMENT_ID;
        }
    }

    public static String endNodeElementId(View view) {
        if (view.equals(PLAIN_JSON)) {
            return Fieldnames.END_NODE_ELEMENT_ID;
        } else {
            return Fieldnames._END_NODE_ELEMENT_ID;
        }
    }

    public static String type(View view) {
        if (view.equals(PLAIN_JSON)) {
            return Fieldnames.RELATIONSHIP_TYPE;
        } else {
            return Fieldnames._RELATIONSHIP_TYPE;
        }
    }
}
