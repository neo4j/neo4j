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
package org.neo4j.kernel.api.impl.schema;

import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexQuery;
import org.neo4j.internal.schema.IndexQuery.IndexQueryType;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.ValueCategory;

public abstract class TextIndexCapability implements IndexCapability {
    private static final double COST_MULTIPLIER_TRIGRAM_GOOD = COST_MULTIPLIER_STANDARD - 0.1;
    private static final double COST_MULTIPLIER_TRIGRAM_BAD = COST_MULTIPLIER_STANDARD + 0.1;
    private static final double COST_MULTIPLIER_TEXT_GOOD = COST_MULTIPLIER_STANDARD - 0.05;
    private static final double COST_MULTIPLIER_TEXT_BAD = COST_MULTIPLIER_STANDARD + 0.05;

    private TextIndexCapability() {}

    public static TextIndexCapability trigram() {
        return new Trigram();
    }

    public static TextIndexCapability text() {
        return new Text();
    }

    protected abstract double costMultiplierBad();

    protected abstract double costMultiplierGood();

    @Override
    public boolean supportsOrdering() {
        return false;
    }

    @Override
    public boolean supportsReturningValues() {
        return false;
    }

    @Override
    public boolean areValueCategoriesAccepted(ValueCategory... valueCategories) {
        Preconditions.requireNonEmpty(valueCategories);
        Preconditions.requireNoNullElements(valueCategories);
        return valueCategories.length == 1 && valueCategories[0] == ValueCategory.TEXT;
    }

    @Override
    public boolean isQuerySupported(IndexQueryType queryType, ValueCategory valueCategory) {

        if (queryType == IndexQueryType.ALL_ENTRIES) {
            return true;
        }

        if (!areValueCategoriesAccepted(valueCategory)) {
            return false;
        }

        return isIndexQueryTypeSupported(queryType);
    }

    private static boolean isIndexQueryTypeSupported(IndexQueryType indexQueryType) {
        return switch (indexQueryType) {
            case EXACT, STRING_PREFIX, STRING_CONTAINS, STRING_SUFFIX -> true;
            default -> false;
        };
    }

    @Override
    public double getCostMultiplier(IndexQueryType... queryTypes) {
        Preconditions.checkState(queryTypes.length == 1, "Does not support composite queries");
        var queryType = queryTypes[0];
        return switch (queryType) {
            case STRING_SUFFIX, STRING_CONTAINS -> costMultiplierGood();
            case EXACT, RANGE, STRING_PREFIX -> costMultiplierBad();
            case ALL_ENTRIES -> COST_MULTIPLIER_STANDARD;
            default -> throw new IllegalStateException("Unexpected value: " + queryType);
        };
    }

    @Override
    public boolean supportPartitionedScan(IndexQuery... queries) {
        Preconditions.requireNonEmpty(queries);
        Preconditions.requireNoNullElements(queries);
        return false;
    }

    private static class Text extends TextIndexCapability {

        @Override
        protected double costMultiplierBad() {
            return COST_MULTIPLIER_TEXT_BAD;
        }

        @Override
        protected double costMultiplierGood() {
            return COST_MULTIPLIER_TEXT_GOOD;
        }
    }

    private static class Trigram extends TextIndexCapability {

        @Override
        protected double costMultiplierBad() {
            return COST_MULTIPLIER_TRIGRAM_BAD;
        }

        @Override
        protected double costMultiplierGood() {
            return COST_MULTIPLIER_TRIGRAM_GOOD;
        }
    }
}
