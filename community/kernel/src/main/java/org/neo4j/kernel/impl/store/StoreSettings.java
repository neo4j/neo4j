/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.store;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.configuration.Description;
import org.neo4j.kernel.configuration.Internal;
import org.neo4j.kernel.configuration.Title;

import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.DURATION;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.kernel.configuration.Settings.INTEGER;
import static org.neo4j.kernel.configuration.Settings.TRUE;
import static org.neo4j.kernel.configuration.Settings.min;
import static org.neo4j.kernel.configuration.Settings.setting;

public class StoreSettings
{
    @Description("Use a quick approach for rebuilding the ID generators. This give quicker recovery time, " +
            "but will limit the ability to reuse the space of deleted entities.")
    @Internal
    public static final Setting<Boolean> rebuild_idgenerators_fast = setting("rebuild_idgenerators_fast", BOOLEAN, TRUE );

    @Description( "Relationship count threshold for considering a node to be dense" )
    public static final Setting<Integer> dense_node_threshold = setting( "dense_node_threshold", INTEGER, "50", min(1) );

    @Description("Specifies the block size for storing labels exceeding in-lined space in node record. " +
            "This parameter is only honored when the store is created, otherwise it is ignored. " +
            "The default block size is 60 bytes, and the overhead of each block is the same as for string blocks, " +
            "i.e., 8 bytes.")
    @Internal
    public static final Setting<Integer> label_block_size = setting("label_block_size", INTEGER, "60",min(1));

    @Description("Specifies the block size for storing strings. This parameter is only honored when the store is " +
            "created, otherwise it is ignored. " +
            "Note that each character in a string occupies two bytes, meaning that a block size of 120 (the default " +
            "size) will hold a 60 character " +
            "long string before overflowing into a second block. Also note that each block carries an overhead of 8 " +
            "bytes. " +
            "This means that if the block size is 120, the size of the stored records will be 128 bytes.")
    @Internal
    public static final Setting<Integer> string_block_size = setting("string_block_size", INTEGER, "120",min(1));

    @Description("Specifies the block size for storing arrays. This parameter is only honored when the store is " +
            "created, otherwise it is ignored. " +
            "The default block size is 120 bytes, and the overhead of each block is the same as for string blocks, " +
            "i.e., 8 bytes.")
    @Internal
    public static final Setting<Integer> array_block_size = setting("array_block_size", INTEGER, "120",min(1));

    @Title("Read only database")
    @Description("Only allow read operations from this Neo4j instance. " +
                 "This mode still requires write access to the directory for lock purposes.")
    public static final Setting<Boolean> read_only = setting( "read_only", BOOLEAN, FALSE );

    @Description( "Maximum time interval for log rotation to wait for active transaction completion" )
    @Internal
    public static final Setting<Long> store_interval_log_rotation_wait_time =
            setting( "store.interval.log.rotation", DURATION, "10m" );
}
