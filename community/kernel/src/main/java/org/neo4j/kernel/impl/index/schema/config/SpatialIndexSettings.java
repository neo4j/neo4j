/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.neo4j.configuration.Description;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.Setting;

import static org.neo4j.kernel.configuration.Settings.DOUBLE;
import static org.neo4j.kernel.configuration.Settings.INTEGER;
import static org.neo4j.kernel.configuration.Settings.setting;

public class SpatialIndexSettings implements LoadableConfig
{
    @Description( "When searching the spatial index we need to convert a 2D range in the quad tree into a set of 1D ranges on the " +
            "underlying 1D space filling curve index. There is a balance to be made between many small 1D ranges that have few false " +
            "positives, and fewer, larger 1D ranges that have more false positives. The former has a more efficient filtering of false " +
            "positives, while the latter will have a more efficient search of the numerical index. The maximum depth to which the quad tree is " +
            "processed when mapping 2D to 1D is based on the size of the search area compared to the size of the 2D tiles at that depth. " +
            "This setting will cause the algorithm to search deeper, reducing false positives." )
    @Internal
    public static final Setting<Integer> space_filling_curve_extra_levels = setting(
            "unsupported.dbms.index.spatial.curve.extra_levels", INTEGER, "1" );

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
    public static final Setting<Double> space_filling_curve_top_threshold = setting(
            "unsupported.dbms.index.spatial.curve.top_threshold", DOUBLE, "0" );

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
    public static final Setting<Double> space_filling_curve_bottom_threshold = setting(
            "unsupported.dbms.index.spatial.curve.bottom_threshold", DOUBLE, "0" );

    @Description( "The maximum number of bits to use for levels in the quad tree representing the spatial index. When creating the spatial index, we " +
            "simulate a quad tree using a 2D (or 3D) to 1D mapping function. This requires that the extents of the index and the depth " +
            "of the tree be defined in advance, so ensure the 2D to 1D mapping is deterministic and repeatable. This setting will define " +
            "the maximum depth of any future spatial index created, calculated as max_bits / dimensions. For example 60 bits will define 30 levels in 2D " +
            "and 20 levels in 3D. Existing indexes will not be changed, and need to be recreated if you wish to use the new value. " +
            "For 2D indexes, a value of 30 is the largest supported. For 3D indexes 20 is the largest." )
    @Internal
    public static final Setting<Integer> space_filling_curve_max_bits = setting(
            "unsupported.dbms.index.spatial.curve.max_bits", INTEGER, "60" );

    // TODO useful descriptions
    // TODO dynamic list of settings to avoid code duplication
    @Description( "The minimum x value for the index extents for 2D Cartesian spatial index. " +
            "The 2D to 1D mapping function divides all 2D space into discrete tiles, and orders these using a space filling curve designed " +
            "to optimize the requirement that tiles that are close together in this ordered list are also close together in 2D space. " +
            "This requires that the extents of the 2D space be known in advance and never changed. If you do change these settings, you " +
            "need to recreate any affected index in order for the settings to apply, otherwise the index will retain the previous settings." )
    @Internal
    public static final Setting<Integer> spatial_crs_Cartesian_x_min = setting( "unsupported.dbms.db.spatial.crs.Cartesian.x.min", INTEGER, "-1000000" );

    @Description( "The maximum x value for the index extents for 2D Cartesian spatial index. " +
            "The 2D to 1D mapping function divides all 2D space into discrete tiles, and orders these using a space filling curve designed " +
            "to optimize the requirement that tiles that are close together in this ordered list are also close together in 2D space. " +
            "This requires that the extents of the 2D space be known in advance and never changed. If you do change these settings, you " +
            "need to recreate any affected index in order for the settings to apply, otherwise the index will retain the previous settings." )
    @Internal
    public static final Setting<Integer> spatial_crs_Cartesian_x_max = setting( "unsupported.dbms.db.spatial.crs.Cartesian.x.max", INTEGER, "1000000" );

    @Description( "The minimum y value for the index extents for 2D Cartesian spatial index. " +
            "The 2D to 1D mapping function divides all 2D space into discrete tiles, and orders these using a space filling curve designed " +
            "to optimize the requirement that tiles that are close together in this ordered list are also close together in 2D space. " +
            "This requires that the extents of the 2D space be known in advance and never changed. If you do change these settings, you " +
            "need to recreate any affected index in order for the settings to apply, otherwise the index will retain the previous settings." )
    @Internal
    public static final Setting<Integer> spatial_crs_Cartesian_y_min = setting( "unsupported.dbms.db.spatial.crs.Cartesian.y.min", INTEGER, "-1000000" );

    @Description( "The maximum y value for the index extents for 2D Cartesian spatial index. " +
            "The 2D to 1D mapping function divides all 2D space into discrete tiles, and orders these using a space filling curve designed " +
            "to optimize the requirement that tiles that are close together in this ordered list are also close together in 2D space. " +
            "This requires that the extents of the 2D space be known in advance and never changed. If you do change these settings, you " +
            "need to recreate any affected index in order for the settings to apply, otherwise the index will retain the previous settings." )
    @Internal
    public static final Setting<Integer> spatial_crs_Cartesian_y_max = setting( "unsupported.dbms.db.spatial.crs.Cartesian.y.max", INTEGER, "1000000" );

    @Description( "The minimum x value for the index extents for 3D Cartesian spatial index. " +
            "The 3D to 1D mapping function divides all 3D space into discrete blocks, and orders these using a space filling curve designed " +
            "to optimize the requirement that blocks that are close together in this ordered list are also close together in 3D space. " +
            "This requires that the extents of the 3D space be known in advance and never changed. If you do change these settings, you " +
            "need to recreate any affected index in order for the settings to apply, otherwise the index will retain the previous settings." )
    @Internal
    public static final Setting<Integer> spatial_crs_Cartesian_3D_x_min = setting( "unsupported.dbms.db.spatial.crs.Cartesian-3D.x.min", INTEGER, "-1000000" );

    @Description( "The maximum x value for the index extents for 3D Cartesian spatial index. " +
            "The 3D to 1D mapping function divides all 3D space into discrete blocks, and orders these using a space filling curve designed " +
            "to optimize the requirement that blocks that are close together in this ordered list are also close together in 3D space. " +
            "This requires that the extents of the 3D space be known in advance and never changed. If you do change these settings, you " +
            "need to recreate any affected index in order for the settings to apply, otherwise the index will retain the previous settings." )
    @Internal
    public static final Setting<Integer> spatial_crs_Cartesian_3D_x_max = setting( "unsupported.dbms.db.spatial.crs.Cartesian-3D.x.max", INTEGER, "1000000" );

    @Description( "The minimum y value for the index extents for 3D Cartesian spatial index. " +
            "The 3D to 1D mapping function divides all 3D space into discrete blocks, and orders these using a space filling curve designed " +
            "to optimize the requirement that blocks that are close together in this ordered list are also close together in 3D space. " +
            "This requires that the extents of the 3D space be known in advance and never changed. If you do change these settings, you " +
            "need to recreate any affected index in order for the settings to apply, otherwise the index will retain the previous settings." )
    @Internal
    public static final Setting<Integer> spatial_crs_Cartesian_3D_y_min = setting( "unsupported.dbms.db.spatial.crs.Cartesian-3D.y.min", INTEGER, "-1000000" );

    @Description( "The maximum y value for the index extents for 3D Cartesian spatial index. " +
            "The 3D to 1D mapping function divides all 3D space into discrete blocks, and orders these using a space filling curve designed " +
            "to optimize the requirement that blocks that are close together in this ordered list are also close together in 3D space. " +
            "This requires that the extents of the 3D space be known in advance and never changed. If you do change these settings, you " +
            "need to recreate any affected index in order for the settings to apply, otherwise the index will retain the previous settings." )
    @Internal
    public static final Setting<Integer> spatial_crs_Cartesian_3D_y_max = setting( "unsupported.dbms.db.spatial.crs.Cartesian-3D.y.max", INTEGER, "1000000" );

    @Description( "The minimum z value for the index extents for 3D Cartesian spatial index. " +
            "The 3D to 1D mapping function divides all 3D space into discrete blocks, and orders these using a space filling curve designed " +
            "to optimize the requirement that blocks that are close together in this ordered list are also close together in 3D space. " +
            "This requires that the extents of the 3D space be known in advance and never changed. If you do change these settings, you " +
            "need to recreate any affected index in order for the settings to apply, otherwise the index will retain the previous settings." )
    @Internal
    public static final Setting<Integer> spatial_crs_Cartesian_3D_z_min = setting( "unsupported.dbms.db.spatial.crs.Cartesian-3D.z.min", INTEGER, "-1000000" );

    @Description( "The maximum z value for the index extents for 3D Cartesian spatial index. " +
            "The 3D to 1D mapping function divides all 3D space into discrete blocks, and orders these using a space filling curve designed " +
            "to optimize the requirement that blocks that are close together in this ordered list are also close together in 3D space. " +
            "This requires that the extents of the 3D space be known in advance and never changed. If you do change these settings, you " +
            "need to recreate any affected index in order for the settings to apply, otherwise the index will retain the previous settings." )
    @Internal
    public static final Setting<Integer> spatial_crs_Cartesian_3D_z_max = setting( "unsupported.dbms.db.spatial.crs.Cartesian-3D.z.max", INTEGER, "1000000" );

    @Description( "The minimum x value for the index extents for 2D WGS-84 (Geographic) spatial index. " +
            "The 2D to 1D mapping function divides all 2D space into discrete tiles, and orders these using a space filling curve designed " +
            "to optimize the requirement that tiles that are close together in this ordered list are also close together in 2D space. " +
            "This requires that the extents of the 2D space be known in advance and never changed. If you do change these settings, you " +
            "need to recreate any affected index in order for the settings to apply, otherwise the index will retain the previous settings." )
    @Internal
    public static final Setting<Integer> spatial_crs_WGS_84_x_min = setting( "unsupported.dbms.db.spatial.crs.WGS-84.x.min", INTEGER, "-180" );

    @Description( "The maximum x value for the index extents for 2D WGS-84 (Geographic) spatial index. " +
            "The 2D to 1D mapping function divides all 2D space into discrete tiles, and orders these using a space filling curve designed " +
            "to optimize the requirement that tiles that are close together in this ordered list are also close together in 2D space. " +
            "This requires that the extents of the 2D space be known in advance and never changed. If you do change these settings, you " +
            "need to recreate any affected index in order for the settings to apply, otherwise the index will retain the previous settings." )
    @Internal
    public static final Setting<Integer> spatial_crs_WGS_84_x_max = setting( "unsupported.dbms.db.spatial.crs.WGS-84.x.max", INTEGER, "180" );

    @Description( "The minimum y value for the index extents for 2D WGS-84 (Geographic) spatial index. " +
            "The 2D to 1D mapping function divides all 2D space into discrete tiles, and orders these using a space filling curve designed " +
            "to optimize the requirement that tiles that are close together in this ordered list are also close together in 2D space. " +
            "This requires that the extents of the 2D space be known in advance and never changed. If you do change these settings, you " +
            "need to recreate any affected index in order for the settings to apply, otherwise the index will retain the previous settings." )
    @Internal
    public static final Setting<Integer> spatial_crs_WGS_84_y_min = setting( "unsupported.dbms.db.spatial.crs.WGS-84.y.min", INTEGER, "-90" );

    @Description( "The maximum y value for the index extents for 2D WGS-84 (Geographic) spatial index. " +
            "The 2D to 1D mapping function divides all 2D space into discrete tiles, and orders these using a space filling curve designed " +
            "to optimize the requirement that tiles that are close together in this ordered list are also close together in 2D space. " +
            "This requires that the extents of the 2D space be known in advance and never changed. If you do change these settings, you " +
            "need to recreate any affected index in order for the settings to apply, otherwise the index will retain the previous settings." )
    @Internal
    public static final Setting<Integer> spatial_crs_WGS_84_y_max = setting( "unsupported.dbms.db.spatial.crs.WGS-84.y.max", INTEGER, "90" );

    @Description( "The minimum x value for the index extents for 3D WGS-84 (Geographic) spatial index. " +
            "The 3D to 1D mapping function divides all 3D space into discrete blocks, and orders these using a space filling curve designed " +
            "to optimize the requirement that blocks that are close together in this ordered list are also close together in 3D space. " +
            "This requires that the extents of the 3D space be known in advance and never changed. If you do change these settings, you " +
            "need to recreate any affected index in order for the settings to apply, otherwise the index will retain the previous settings." )
    @Internal
    public static final Setting<Integer> spatial_crs_WGS_84_3D_x_min = setting( "unsupported.dbms.db.spatial.crs.WGS-84-3D.x.min", INTEGER, "-180" );

    @Description( "The maximum x value for the index extents for 3D WGS-84 (Geographic) spatial index. " +
            "The 3D to 1D mapping function divides all 3D space into discrete blocks, and orders these using a space filling curve designed " +
            "to optimize the requirement that blocks that are close together in this ordered list are also close together in 3D space. " +
            "This requires that the extents of the 3D space be known in advance and never changed. If you do change these settings, you " +
            "need to recreate any affected index in order for the settings to apply, otherwise the index will retain the previous settings." )
    @Internal
    public static final Setting<Integer> spatial_crs_WGS_84_3D_x_max = setting( "unsupported.dbms.db.spatial.crs.WGS-84-3D.x.max", INTEGER, "180" );

    @Description( "The minimum y value for the index extents for 3D WGS-84 (Geographic) spatial index. " +
            "The 3D to 1D mapping function divides all 3D space into discrete blocks, and orders these using a space filling curve designed " +
            "to optimize the requirement that blocks that are close together in this ordered list are also close together in 3D space. " +
            "This requires that the extents of the 3D space be known in advance and never changed. If you do change these settings, you " +
            "need to recreate any affected index in order for the settings to apply, otherwise the index will retain the previous settings." )
    @Internal
    public static final Setting<Integer> spatial_crs_WGS_84_3D_y_min = setting( "unsupported.dbms.db.spatial.crs.WGS-84-3D.y.min", INTEGER, "-90" );

    @Description( "The maximum y value for the index extents for 3D WGS-84 (Geographic) spatial index. " +
            "The 3D to 1D mapping function divides all 3D space into discrete blocks, and orders these using a space filling curve designed " +
            "to optimize the requirement that blocks that are close together in this ordered list are also close together in 3D space. " +
            "This requires that the extents of the 3D space be known in advance and never changed. If you do change these settings, you " +
            "need to recreate any affected index in order for the settings to apply, otherwise the index will retain the previous settings." )
    @Internal
    public static final Setting<Integer> spatial_crs_WGS_84_3D_y_max = setting( "unsupported.dbms.db.spatial.crs.WGS-84-3D.y.max", INTEGER, "90" );

    @Description( "The minimum z value for the index extents for 3D WGS-84 (Geographic) spatial index. " +
            "The 3D to 1D mapping function divides all 3D space into discrete blocks, and orders these using a space filling curve designed " +
            "to optimize the requirement that blocks that are close together in this ordered list are also close together in 3D space. " +
            "This requires that the extents of the 3D space be known in advance and never changed. If you do change these settings, you " +
            "need to recreate any affected index in order for the settings to apply, otherwise the index will retain the previous settings." )
    @Internal
    public static final Setting<Integer> spatial_crs_WGS_84_3D_z_min = setting( "unsupported.dbms.db.spatial.crs.WGS-84-3D.z.min", INTEGER, "-1000000" );

    @Description( "The maximum z value for the index extents for 3D WGS-84 (Geographic) spatial index. " +
            "The 3D to 1D mapping function divides all 3D space into discrete blocks, and orders these using a space filling curve designed " +
            "to optimize the requirement that blocks that are close together in this ordered list are also close together in 3D space. " +
            "This requires that the extents of the 3D space be known in advance and never changed. If you do change these settings, you " +
            "need to recreate any affected index in order for the settings to apply, otherwise the index will retain the previous settings." )
    @Internal
    public static final Setting<Integer> spatial_crs_WGS_84_3D_z_max = setting( "unsupported.dbms.db.spatial.crs.WGS-84-3D.z.max", INTEGER, "1000000" );

}
