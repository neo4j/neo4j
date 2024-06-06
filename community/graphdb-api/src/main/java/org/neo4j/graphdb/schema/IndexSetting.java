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

/**
 * Index settings are used for fine-tuning the behaviour of schema indexes.
 * All indexes have a configuration associated with them, and it is only necessary to specify the particular settings in that configuration,
 * that should differ from their defaults.
 * <p>
 * Index settings can only be specified when the index is created.
 * Once the index has been created, the index configuration becomes immutable.
 * <p>
 * Here is an example where a full-text index is created with a custom analyzer:
 *
 * <pre><code>
 *     try ( Transaction tx = database.beginTx() )
 *     {
 *         tx.schema().indexFor( Label.label( "Email" ) ).on( "from" ).on( "to" ).on( "cc" ).on( "bcc" )
 *                 .withName( "email-addresses" )
 *                 .withIndexType( IndexType.FULLTEXT )
 *                 .withIndexConfiguration( Map.of( IndexSetting.fulltext_Analyzer(), "email" ) )
 *                 .create();
 *         tx.commit();
 *     }
 * </code></pre>
 */
@PublicApi
public interface IndexSetting {
    String getSettingName();

    Class<?> getType();

    /**
     * Configure the analyzer used in a full-text index, indexes of type {@link IndexType#FULLTEXT}.
     * <p>
     * The list of possible analyzers are available via the {@code db.index.fulltext.listAvailableAnalyzers()} procedure.
     * <p>
     * This setting is given as a String.
     */
    static IndexSetting fulltext_Analyzer() {
        return IndexSettingImpl.FULLTEXT_ANALYZER;
    }

    /**
     * Configure if a full-text index is allowed to be eventually consistent.
     * By default full-text indexes are fully consistent, just like other schema indexes.
     * <p>
     * This setting is given as a boolean.
     */
    static IndexSetting fulltext_Eventually_Consistent() {
        return IndexSettingImpl.FULLTEXT_EVENTUALLY_CONSISTENT;
    }

    /**
     * Fine tune behaviour for spatial values in point index, indexes of type {@link IndexType#POINT}.
     * <p>
     * Configuration for cartesian coordinate reference system.
     * <p>
     * Set min value for envelope in dimension order {@code [minX, minY]}.
     */
    static IndexSetting spatial_Cartesian_Min() {
        return IndexSettingImpl.SPATIAL_CARTESIAN_MIN;
    }

    /**
     * Fine tune behaviour for spatial values in point index, indexes of type {@link IndexType#POINT}.
     * <p>
     * Configuration for cartesian coordinate reference system.
     * <p>
     * Set max value for envelope in dimension order {@code [maxX, maxY]}.
     */
    static IndexSetting spatial_Cartesian_Max() {
        return IndexSettingImpl.SPATIAL_CARTESIAN_MAX;
    }

    /**
     * Fine tune behaviour for spatial values in point index, indexes of type {@link IndexType#POINT}.
     * <p>
     * Configuration for cartesian coordinate reference system.
     * <p>
     * Set min value for envelope in dimension order {@code [minX, minY, minZ]}.
     */
    static IndexSetting spatial_Cartesian_3D_Min() {
        return IndexSettingImpl.SPATIAL_CARTESIAN_3D_MIN;
    }

    /**
     * Fine tune behaviour for spatial values in point index, indexes of type {@link IndexType#POINT}.
     * <p>
     * Configuration for cartesian coordinate reference system.
     * <p>
     * Set max value for envelope in dimension order {@code [maxX, maxY, maxZ]}.
     */
    static IndexSetting spatial_Cartesian_3D_Max() {
        return IndexSettingImpl.SPATIAL_CARTESIAN_3D_MAX;
    }

    /**
     * Fine tune behaviour for spatial values in point index, indexes of type {@link IndexType#POINT}.
     * <p>
     * Configuration for cartesian coordinate reference system.
     * <p>
     * Set min value for envelope in dimension order {@code [minLongitude, minLatitude]}.
     */
    static IndexSetting spatial_Wgs84_Min() {
        return IndexSettingImpl.SPATIAL_WGS84_MIN;
    }

    /**
     * Fine tune behaviour for spatial values in point index, indexes of type {@link IndexType#POINT}.
     * <p>
     * Configuration for cartesian coordinate reference system.
     * <p>
     * Set max value for envelope in dimension order {@code [maxLongitude, maxLatitude]}.
     */
    static IndexSetting spatial_Wgs84_Max() {
        return IndexSettingImpl.SPATIAL_WGS84_MAX;
    }

    /**
     * Fine tune behaviour for spatial values in point index, indexes of type {@link IndexType#POINT}.
     * <p>
     * Configuration for cartesian coordinate reference system.
     * <p>
     * Set min value for envelope in dimension order {@code [minLongitude, minLatitude, minZ]}.
     */
    static IndexSetting spatial_Wgs84_3D_Min() {
        return IndexSettingImpl.SPATIAL_WGS84_3D_MIN;
    }

    /**
     * Fine tune behaviour for spatial values in point index, indexes of type {@link IndexType#POINT}.
     * <p>
     * Configuration for cartesian coordinate reference system.
     * <p>
     * Set max value for envelope in dimension order {@code [maxLongitude, maxLatitude, maxZ]}.
     */
    static IndexSetting spatial_Wgs84_3D_Max() {
        return IndexSettingImpl.SPATIAL_WGS84_3D_MAX;
    }

    /**
     * Configure the dimensionality of the vectors used in vector indexes; indexes of type {@link IndexType#VECTOR}.
     * This setting is given as an {@link Integer}.
     */
    static IndexSetting vector_Dimensions() {
        return IndexSettingImpl.VECTOR_DIMENSIONS;
    }

    /**
     * Configure the similarity function of the vectors used in vector indexes; indexes of type
     * {@link IndexType#VECTOR}.
     * This setting is given as a {@link String}.
     * Possible values are {@code "EUCLIDEAN"} and {@code "COSINE"}.
     */
    static IndexSetting vector_Similarity_Function() {
        return IndexSettingImpl.VECTOR_SIMILARITY_FUNCTION;
    }

    static IndexSetting vector_Quantization() {
        return IndexSettingImpl.VECTOR_QUANTIZATION;
    }
}
