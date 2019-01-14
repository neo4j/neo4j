/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.kernel.api;

import org.neo4j.values.storable.ValueCategory;

/**
 * Capabilities of an index.
 * Capabilities of an index can not change during the indexes lifetimes.
 * Caching of IndexCapability is allowed.
 * It does NOT describe the capabilities of the index at some given moment. For example it does not describe
 * index state. Rather it describe the functionality that index provide given that it is available.
 */
public interface IndexCapability
{
    IndexOrder[] ORDER_ASC = {IndexOrder.ASCENDING};
    IndexOrder[] ORDER_NONE = new IndexOrder[0];
    IndexLimitation[] LIMITIATION_NONE = new IndexLimitation[0];

    /**
     * What possible orderings is this index capable to provide for a query on given combination of {@link ValueCategory}.
     * Ordering of ValueCategory correspond to ordering of related {@link IndexReference#properties()}.
     *
     * @param valueCategories Ordered array of {@link ValueCategory ValueCategories} for which index should be queried. Note that valueCategory
     * must correspond to related {@link IndexReference#properties()}. A {@code null} value in the array
     * ({@code new ValueCategory[]{null}}) is interpreted as a wildcard for any {@link ValueCategory}. Note that this is not the same as
     * {@code order(null)} which is undefined.
     * @return {@link IndexOrder} array containing all possible orderings for provided value categories or empty array if no explicit
     * ordering is possible or if length of {@code valueCategories} and {@link IndexReference#properties()} differ.
     */
    IndexOrder[] orderCapability( ValueCategory... valueCategories );

    /**
     * Is the index capable of providing values for a query on given combination of {@link ValueCategory}.
     * Ordering of ValueCategory correspond to ordering of {@code properties} in related {@link IndexReference}.
     *
     * @param valueCategories Ordered array of {@link ValueCategory ValueCategories} for which index should be queried. Note that valueCategory
     * must correspond to related {@link IndexReference#properties()}. {@link ValueCategory#UNKNOWN} can be used as a wildcard for
     * any {@link ValueCategory}. Behaviour is undefined for empty {@code null} array and {@code null} values in array.
     * @return {@link IndexValueCapability#YES} if index is capable of providing values for query on provided array of value categories,
     * {@link IndexValueCapability#NO} if not or {@link IndexValueCapability#PARTIAL} for some results. If length of
     * {@code valueCategories} and {@link IndexReference#properties()} differ {@link IndexValueCapability#NO} is returned.
     */
    IndexValueCapability valueCapability( ValueCategory... valueCategories );

    /**
     * @return an array of limitations that this index has. It could be anything that planning could look at and
     * either try to avoid or issue warning for.
     */
    default IndexLimitation[] limitations()
    {
        return LIMITIATION_NONE;
    }

    default boolean singleWildcard( ValueCategory[] valueCategories )
    {
        return valueCategories.length == 1 && valueCategories[0] == ValueCategory.UNKNOWN;
    }

    IndexCapability NO_CAPABILITY = new IndexCapability()
    {
        @Override
        public IndexOrder[] orderCapability( ValueCategory... valueCategories )
        {
            return ORDER_NONE;
        }

        @Override
        public IndexValueCapability valueCapability( ValueCategory... valueCategories )
        {
            return IndexValueCapability.NO;
        }
    };
}
