/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.graphdb.schema;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

/**
 * The index type describes the overall behaviour and performance profile of an index.
 */
@PublicApi
public enum IndexType
{
    /**
     * For B+Tree based indexes. All types of values are indexed and stored in sort-order. This means they are good at all types of exact matching,
     * and range queries. They can also support index-backed order-by.
     * <p>
     * BTREE indexes have the following abilities and limitations:
     * <ul>
     *     <li>They can be used as the {@link ConstraintCreator#withIndexType(IndexType) constraint index type} for index-backed constraints.</li>
     *     <li>They can have their spatial indexing behaviour fine-tuned, using the {@linkplain IndexSetting index settings} that start with "spatial_".</li>
     *     <li>They do not support {@linkplain Schema#indexFor(Label...) creating} {@linkplain IndexDefinition#isMultiTokenIndex() multi-token} indexes.</li>
     *     <li>They can be created on both {@link Schema#indexFor(Label) labels}, and {@link Schema#indexFor(RelationshipType) relationship types}.</li>
     * </ul>
     */
    BTREE,
    /**
     * For full-text indexes. These indexes only index string values, and cannot answer all types of queries.
     * On the other hand, they are good at matching sub-strings of the indexed values, and they can do fuzzy matching, and scoring.
     * <p>
     * FULLTEXT indexes have the following abilities and limitations:
     * <ul>
     *     <li>They cannot be used as the {@link ConstraintCreator#withIndexType(IndexType) constraint index type} for index-backed constraints.</li>
     *     <li>They can have their behaviour fine-tuned, using the {@linkplain IndexSetting index settings} that start with "fulltext_".</li>
     *     <li>They can be {@linkplain Schema#indexFor(Label...) created} as {@linkplain IndexDefinition#isMultiTokenIndex() multi-token} indexes.</li>
     *     <li>They can be created on both {@link Schema#indexFor(Label) labels}, and {@link Schema#indexFor(RelationshipType) relationship types}.</li>
     * </ul>
     */
    FULLTEXT
}
