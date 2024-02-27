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
package org.neo4j.kernel.api.impl.schema.vector;

class VectorDocumentStructures {
    static VectorDocumentStructure documentStructureFor(VectorIndexVersion version) {
        return switch (version) {
            case UNKNOWN -> null;
            case V1_0 -> V1;
            case V2_0 -> V2;
        };
    }

    private static final VectorDocumentStructure V1 = new VectorDocumentStructure() {
        static final String VECTOR_VALUE_KEY = "vector";

        @Override
        String vectorValueKeyFor(int dimensions) {
            return VECTOR_VALUE_KEY;
        }
    };

    private static final VectorDocumentStructure V2 = new VectorDocumentStructure() {
        static final String VECTOR_VALUE_KEY_SUFFIX = "d-vector";

        @Override
        String vectorValueKeyFor(int dimensions) {
            return dimensions + VECTOR_VALUE_KEY_SUFFIX;
        }
    };
}
