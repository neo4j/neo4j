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
package org.neo4j.graphdb.schema;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.StringSearchMode;

/**
 * The index type describes the overall behaviour and performance profile of an index.
 */
@PublicApi
public enum IndexType {
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
    FULLTEXT,

    /**
     * Type of index that maps all labels or all relationship types to nodes or relationships respectively. This type of index is used to find all entities
     * that have specific token.
     * <p>
     * LOOKUP indexes have the following abilities and limitations:
     * <ul>
     *     <li>They cannot be used as the {@link ConstraintCreator#withIndexType(IndexType) constraint index type} for index-backed constraints.</li>
     *     <li>They can not have their behaviour fine-tuned, using the {@linkplain IndexSetting index settings}.</li>
     *     <li>They do not support {@linkplain Schema#indexFor(Label...) creating} {@linkplain IndexDefinition#isMultiTokenIndex() multi-token} indexes.</li>
     *     <li>They can be {@link Schema#indexFor(AnyTokens) created} as {@link AnyTokens#ANY_LABELS label} or
     *     {@link AnyTokens#ANY_RELATIONSHIP_TYPES relationship type} token index.</li>
     * </ul>
     */
    LOOKUP,

    /**
     * Text indexes only index string values, it can only answer {@link StringSearchMode  basic string queries}
     *
     * <p>
     * TEXT indexes have the following abilities and limitations:
     * <ul>
     *     <li>They do not support composite indexes.</li>
     *     <li>They do not support fuzzy matching and scoring, use FULLTEXT index for advance text searching</li>
     *     <li>They do not support {@linkplain Schema#indexFor(Label...) creating} {@linkplain IndexDefinition#isMultiTokenIndex() multi-token} indexes.</li>
     *     <li>They can be created on both {@link Schema#indexFor(Label) labels}, and {@link Schema#indexFor(RelationshipType) relationship types}.</li>
     * </ul>
     */
    TEXT,

    /**
     * For B+Tree based indexes. All types of values are indexed and stored in sort-order. This means they are good at all types of exact matching,
     * and range queries. They can also support index-backed order-by.
     * <p>
     * RANGE indexes have the following abilities and limitations:
     * <ul>
     *     <li>They can be used as the {@link ConstraintCreator#withIndexType(IndexType) constraint index type} for index-backed constraints.</li>
     *     <li>They do not support spatial queries, like 'distance' for example.</li>
     *     <li>They do not support {@linkplain Schema#indexFor(Label...) creating} {@linkplain IndexDefinition#isMultiTokenIndex() multi-token} indexes.</li>
     *     <li>They can be created on both {@link Schema#indexFor(Label) labels}, and {@link Schema#indexFor(RelationshipType) relationship types}.</li>
     * </ul>
     */
    RANGE,

    /**
     * Point indexes only index point values (point arrays are not supported).
     * They are designed to answer geometric queries like getting points within a given distance from another point
     * or getting points within a given bounding box.
     *
     * <p>
     * POINT indexes have the following abilities and limitations:
     * <ul>
     *     <li>They cannot be used as the {@link ConstraintCreator#withIndexType(IndexType) constraint index type} for index-backed constraints.</li>
     *     <li>They do not support composite indexes.</li>
     *     <li>They can have their spatial indexing behaviour fine-tuned, using the {@linkplain IndexSetting index settings} that start with "spatial_".</li>
     *     <li>They do not support {@linkplain Schema#indexFor(Label...) creating} {@linkplain IndexDefinition#isMultiTokenIndex() multi-token} indexes.</li>
     *     <li>They can be created on both {@link Schema#indexFor(Label) labels}, and {@link Schema#indexFor(RelationshipType) relationship types}.</li>
     *     <li>They do not support ordering.</li>
     * </ul>
     */
    POINT,

    /**
     * Vector indexes only index float- and double-arrays with finite-float elements of a specified dimensionality.
     * They are designed to answer approximate nearest neighbor queries.
     * <p>
     * It is required that both the {@link IndexSetting#vector_Dimensions() dimensionality} and the
     * {@link IndexSetting#vector_Similarity_Function() similarity function} are set. This is done automatically
     * when using the procedure {@code db.index.vector.createNodeIndex}.
     * <p>
     * VECTOR indexes have the following limitations:
     * <ul>
     *     <li>They cannot be used as the {@link ConstraintCreator#withIndexType(IndexType) constraint index type} for index-backed constraints.</li>
     *     <li>They do not support composite indexes.</li>
     *     <li>They do not support {@linkplain Schema#indexFor(Label...) creating} {@linkplain IndexDefinition#isMultiTokenIndex() multi-token} indexes.</li>
     *     <li>They can only be created on {@link Schema#indexFor(Label) labels}.</li>
     * </ul>
     */
    VECTOR
}
