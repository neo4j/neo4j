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

import org.neo4j.values.storable.ValueGroup;

class VectorDocumentStructures {
    static VectorDocumentStructure documentStructureFor(VectorIndexVersion version) {
        return switch (version) {
            case UNKNOWN -> null;
            case V1_0 -> V1;
            case V2_0 -> V2;
            case V3_0, V2026_06, V2026_07 -> V3;
        };
    }

    private static final VectorDocumentStructure V1 = new VectorDocumentStructure() {
        static final String VECTOR_VALUE_KEY = "vector";

        @Override
        public String vectorValueKeyFor(int dimensions) {
            return VECTOR_VALUE_KEY;
        }

        @Override
        public String booleanValueKeyFor(int propertyIndex) {
            throw new UnsupportedOperationException("V1 does not support single stage filtering");
        }

        @Override
        public String integralValueKeyFor(int propertyIndex) {
            throw new UnsupportedOperationException("V1 does not support single stage filtering");
        }

        @Override
        public String floatingValueKeyFor(int propertyIndex) {
            throw new UnsupportedOperationException("V1 does not support single stage filtering");
        }

        @Override
        public String textValueKeyFor(int propertyIndex) {
            throw new UnsupportedOperationException("V1 does not support single stage filtering");
        }

        @Override
        public String temporalValueKeyFor(int propertyIndex, ValueGroup group) {
            throw new UnsupportedOperationException("V1 does not support single stage filtering");
        }

        @Override
        public String zoneOffsetValueKeyFor(int propertyIndex, ValueGroup group) {
            throw new UnsupportedOperationException("V1 does not support single stage filtering");
        }

        @Override
        public String zoneIdValueKeyFor(int propertyIndex, ValueGroup group) {
            throw new UnsupportedOperationException("V1 does not support single stage filtering");
        }

        @Override
        public String durationNanosValueKeyFor(int propertyIndex) {
            throw new UnsupportedOperationException("V1 does not support single stage filtering");
        }

        @Override
        public String durationSecondsValueKeyFor(int propertyIndex) {
            throw new UnsupportedOperationException("V1 does not support single stage filtering");
        }

        @Override
        public String durationDaysValueKeyFor(int propertyIndex) {
            throw new UnsupportedOperationException("V1 does not support single stage filtering");
        }

        @Override
        public String durationMonthsValueKeyFor(int propertyIndex) {
            throw new UnsupportedOperationException("V1 does not support single stage filtering");
        }
    };

    private static final VectorDocumentStructure V2 = new VectorDocumentStructure() {
        static final String VECTOR_VALUE_KEY_SUFFIX = "d-vector";

        @Override
        public String vectorValueKeyFor(int dimensions) {
            return dimensions + VECTOR_VALUE_KEY_SUFFIX;
        }

        @Override
        public String booleanValueKeyFor(int propertyIndex) {
            throw new UnsupportedOperationException("V2 does not support single stage filtering");
        }

        @Override
        public String integralValueKeyFor(int propertyIndex) {
            throw new UnsupportedOperationException("V2 does not support single stage filtering");
        }

        @Override
        public String floatingValueKeyFor(int propertyIndex) {
            throw new UnsupportedOperationException("V2 does not support single stage filtering");
        }

        @Override
        public String textValueKeyFor(int propertyIndex) {
            throw new UnsupportedOperationException("V2 does not support single stage filtering");
        }

        @Override
        public String temporalValueKeyFor(int propertyIndex, ValueGroup group) {
            throw new UnsupportedOperationException("V2 does not support single stage filtering");
        }

        @Override
        public String zoneOffsetValueKeyFor(int propertyIndex, ValueGroup group) {
            throw new UnsupportedOperationException("V2 does not support single stage filtering");
        }

        @Override
        public String zoneIdValueKeyFor(int propertyIndex, ValueGroup group) {
            throw new UnsupportedOperationException("V2 does not support single stage filtering");
        }

        @Override
        public String durationNanosValueKeyFor(int propertyIndex) {
            throw new UnsupportedOperationException("V2 does not support single stage filtering");
        }

        @Override
        public String durationSecondsValueKeyFor(int propertyIndex) {
            throw new UnsupportedOperationException("V2 does not support single stage filtering");
        }

        @Override
        public String durationDaysValueKeyFor(int propertyIndex) {
            throw new UnsupportedOperationException("V2 does not support single stage filtering");
        }

        @Override
        public String durationMonthsValueKeyFor(int propertyIndex) {
            throw new UnsupportedOperationException("V2 does not support single stage filtering");
        }
    };

    private static final VectorDocumentStructure V3 = new VectorDocumentStructure() {
        static final String VECTOR_VALUE_KEY_SUFFIX = "d-vector";

        @Override
        public String vectorValueKeyFor(int dimensions) {
            return dimensions + VECTOR_VALUE_KEY_SUFFIX;
        }

        @Override
        public String booleanValueKeyFor(int propertyIndex) {
            return "boolean-" + propertyIndex;
        }

        @Override
        public String integralValueKeyFor(int propertyIndex) {
            return "integral-" + propertyIndex;
        }

        @Override
        public String floatingValueKeyFor(int propertyIndex) {
            return "floating-" + propertyIndex;
        }

        @Override
        public String textValueKeyFor(int propertyIndex) {
            return "text-" + propertyIndex;
        }

        @Override
        public String temporalValueKeyFor(int propertyIndex, ValueGroup group) {
            return "temporal-" + group.name() + "-" + propertyIndex;
        }

        @Override
        public String zoneOffsetValueKeyFor(int propertyIndex, ValueGroup group) {
            return "zoneoffset-" + group.name() + "-" + propertyIndex;
        }

        @Override
        public String zoneIdValueKeyFor(int propertyIndex, ValueGroup group) {
            return "zoneid-" + group.name() + "-" + propertyIndex;
        }

        @Override
        public String durationNanosValueKeyFor(int propertyIndex) {
            return "nanos-" + propertyIndex;
        }

        @Override
        public String durationSecondsValueKeyFor(int propertyIndex) {
            return "seconds-" + propertyIndex;
        }

        @Override
        public String durationDaysValueKeyFor(int propertyIndex) {
            return "days-" + propertyIndex;
        }

        @Override
        public String durationMonthsValueKeyFor(int propertyIndex) {
            return "months-" + propertyIndex;
        }
    };
}
