/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.internal.schema;

import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueCategory;
import org.neo4j.values.storable.ValueGroup;

/**
 * Provides a minimal interface for property index queries.
 */
public interface IndexQuery
{
    /**
     * @return The ID of the property key, this queries against.
     */
    int propertyKeyId();

    /**
     * @param value to test against the query.
     * @return true if the {@code value} satisfies the query; false otherwise.
     */
    boolean acceptsValue( Value value );

    /**
     * @return Target {@link ValueGroup} for query or {@link ValueGroup#UNKNOWN} if not targeting single group.
     */
    ValueGroup valueGroup();

    /**
     * @return Target {@link ValueCategory} for query
     */
    ValueCategory valueCategory();

    IndexQueryType type();

    enum IndexQueryType
    {
        exists,
        exact,
        range,
        stringPrefix,
        stringSuffix,
        stringContains,
        fulltextSearch
    }
}
