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
package org.neo4j.internal.schema;

import org.neo4j.internal.schema.IndexQuery.IndexQueryType;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueCategory;

/**
 * Capabilities of an index.
 * Capabilities of an index cannot change during the indexes lifetimes.
 * Caching of IndexCapability is allowed.
 * It does NOT describe the capabilities of the index at some given moment. For example, it does not describe
 * index state. Rather, it describes the functionality that the index provides given that it is available.
 */
public interface IndexCapability {
    double COST_MULTIPLIER_STANDARD = 1.0;
    IndexBehaviour[] BEHAVIOURS_NONE = new IndexBehaviour[0];

    /**
     * Is this index capable of ordering the values (both ascending and descending).
     */
    boolean supportsOrdering();

    /**
     * Is the index capable of providing values.
     */
    boolean supportsReturningValues();

    /**
     * Checks whether the index can accept values for a given combination of {@link ValueCategory}.
     * Ordering of ValueCategory correspond to ordering of {@link SchemaDescriptor#getPropertyIds()}.
     *
     * @param valueCategories Ordered array of {@link ValueCategory ValueCategories} for which index should be queried. Note that valueCategory
     * must correspond to related {@link SchemaDescriptor#getPropertyIds()}.
     * @throws IllegalArgumentException if {@code valueCategories} empty, {@code null}, or contains {@code null}s.
     * @return true if the index is capable of accepting values for a provided array of value categories; false otherwise.
     */
    boolean areValueCategoriesAccepted(ValueCategory... valueCategories);

    /**
     * Checks whether the index can accept values for a given combination of {@link Value}.
     * Ordering of Value correspond to ordering of {@link SchemaDescriptor#getPropertyIds()}.
     *
     * @param values Ordered array of {@link Value Values} for which index should be queried. Note that value
     * must correspond to related {@link SchemaDescriptor#getPropertyIds()}.
     * @throws IllegalArgumentException if {@code values} empty, {@code null}, or contains {@code null}s.
     * @return true if the index is capable of accepting values; false otherwise.
     */
    default boolean areValuesAccepted(Value... values) {
        Preconditions.requireNonEmpty(values);
        Preconditions.requireNoNullElements(values);
        final var categories = new ValueCategory[values.length];
        for (int i = 0; i < values.length; i++) {
            categories[i] = values[i].valueGroup().category();
        }
        return areValueCategoriesAccepted(categories);
    }

    /**
     * Checks whether the index supports a query type for a given value category.
     * This does not take into account how query types can be combined for composite index queries.
     * <p>
     * When asking about support for Index queries that doesn't have any value category,
     * like {@link IndexQueryType#TOKEN_LOOKUP}, use {@link ValueCategory#NO_CATEGORY}.
     */
    boolean isQuerySupported(IndexQueryType queryType, ValueCategory valueCategory);

    /**
     * The multiplier for the cost per returned row of executing a (potentially composite) index query with the given types.
     * A multiplier of 1.0 means that it’s an "average" cost. A higher value means more expensive and a lower value means cheaper.
     * <p>
     * This method does not provide the absolute cost per row, but just a multiplier that can be used to decide between different index types.
     * <p>
     * Even thought the method is implemented for all index types, this check is useful only for value indexes.
     */
    double getCostMultiplier(IndexQueryType... queryTypes);

    /**
     * Does the index support PartitionedScan for given {@code queries}.
     *
     * @param queries a relevant set of queries for the index; such as TokenPredicate, for token indexes; or PropertyIndexQuery, for property indexes.
     * @return {@code true} if the index supports PartitionedScan for the given {@code queries}; false otherwise.
     * @throws IllegalArgumentException if {@code queries} empty, {@code null}, or contains {@code null}s.
     */
    boolean supportPartitionedScan(IndexQuery... queries);

    /**
     * @return an array of behaviours that are particular to the implementation or configuration of this index.
     * It could be anything that planning could look at and either try to avoid, seek out, or issue warning for.
     */
    default IndexBehaviour[] behaviours() {
        return BEHAVIOURS_NONE;
    }

    IndexCapability NO_CAPABILITY = new IndexCapability() {
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
            return false;
        }

        @Override
        public boolean isQuerySupported(IndexQueryType queryType, ValueCategory valueCategory) {
            return false;
        }

        @Override
        public double getCostMultiplier(IndexQueryType... queryTypes) {
            return COST_MULTIPLIER_STANDARD;
        }

        @Override
        public boolean supportPartitionedScan(IndexQuery... queries) {
            Preconditions.requireNonEmpty(queries);
            Preconditions.requireNoNullElements(queries);
            return false;
        }
    };
}
