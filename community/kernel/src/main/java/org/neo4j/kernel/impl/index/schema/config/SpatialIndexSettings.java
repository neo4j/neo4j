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
package org.neo4j.kernel.impl.index.schema.config;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.graphdb.config.Setting;

import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.DOUBLE;
import static org.neo4j.configuration.SettingValueParsers.INT;

@ServiceProvider
public class SpatialIndexSettings implements SettingsDeclaration
{
    @Description( "When searching the spatial index we need to convert a 2D range in the quad tree into a set of 1D ranges on the " +
            "underlying 1D space filling curve index. There is a balance to be made between many small 1D ranges that have few false " +
            "positives, and fewer, larger 1D ranges that have more false positives. The former has a more efficient filtering of false " +
            "positives, while the latter will have a more efficient search of the numerical index. The maximum depth to which the quad tree is " +
            "processed when mapping 2D to 1D is based on the size of the search area compared to the size of the 2D tiles at that depth. " +
            "This setting will cause the algorithm to search deeper, reducing false positives." )
    @Internal
    public static final Setting<Integer> space_filling_curve_extra_levels =
            newBuilder( "unsupported.dbms.index.spatial.curve.extra_levels", INT, 1 ).build();

    @Description( "When searching the spatial index we need to convert a 2D range in the quad tree into a set of 1D ranges on the " +
            "underlying 1D space filling curve index. There is a balance to be made between many small 1D ranges that have few false " +
            "positives, and fewer, larger 1D ranges that have more false positives. The former has a more efficient filtering of false " +
            "positives, while the latter will have a more efficient search of the numerical index. The maximum depth to which the quad tree is " +
            "processed when mapping 2D to 1D is based on the size of the search area compared to the size of the 2D tiles at that depth. " +
            "When traversing the tree to this depth, we can stop early based on when the search envelope overlaps the current tile by " +
            "more than a certain threshold. The threshold is calculated based on depth, from the `top_threshold` at the top of the tree " +
            "to the `bottom_threshold` at the depth calculated by the area comparison. Setting the top to 0.99 and the bottom to 0.5, " +
            "for example would mean that if we reached the maximum depth, and the search area overlapped the current tile by more than " +
            "50%, we would stop traversing the tree, and return the 1D range for that entire tile to the search set. If the overlap is even " +
            "higher, we would stop higher in the tree. This technique reduces the number of 1D ranges passed to the underlying space filling " +
            "curve index. Setting this value to zero turns off this feature." )
    @Internal
    public static final Setting<Double> space_filling_curve_top_threshold =
            newBuilder( "unsupported.dbms.index.spatial.curve.top_threshold", DOUBLE, 0.0 ).build();

    @Description( "When searching the spatial index we need to convert a 2D range in the quad tree into a set of 1D ranges on the " +
            "underlying 1D space filling curve index. There is a balance to be made between many small 1D ranges that have few false " +
            "positives, and fewer, larger 1D ranges that have more false positives. The former has a more efficient filtering of false " +
            "positives, while the latter will have a more efficient search of the numerical index. The maximum depth to which the quad tree is " +
            "processed when mapping 2D to 1D is based on the size of the search area compared to the size of the 2D tiles at that depth. " +
            "When traversing the tree to this depth, we can stop early based on when the search envelope overlaps the current tile by " +
            "more than a certain threshold. The threshold is calculated based on depth, from the `top_threshold` at the top of the tree " +
            "to the `bottom_threshold` at the depth calculated by the area comparison. Setting the top to 0.99 and the bottom to 0.5, " +
            "for example would mean that if we reached the maximum depth, and the search area overlapped the current tile by more than " +
            "50%, we would stop traversing the tree, and return the 1D range for that entire tile to the search set. If the overlap is even " +
            "higher, we would stop higher in the tree. This technique reduces the number of 1D ranges passed to the underlying space filling " +
            "curve index. Setting this value to zero turns off this feature." )
    @Internal
    public static final Setting<Double> space_filling_curve_bottom_threshold =
            newBuilder( "unsupported.dbms.index.spatial.curve.bottom_threshold", DOUBLE, 0.0 ).build();
}
