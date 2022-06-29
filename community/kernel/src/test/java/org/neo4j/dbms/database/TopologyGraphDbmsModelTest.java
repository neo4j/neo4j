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
package org.neo4j.dbms.database;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.InstanceModeConstraint.NONE;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.InstanceModeConstraint.PRIMARY;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.InstanceModeConstraint.SECONDARY;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.InstanceModeConstraint.SINGLE;

import org.junit.jupiter.api.Test;

public class TopologyGraphDbmsModelTest {

    @Test
    void allowsMode() {
        assertTrue(PRIMARY.allowsMode(TopologyGraphDbmsModel.HostedOnMode.raft));
        assertFalse(SECONDARY.allowsMode(TopologyGraphDbmsModel.HostedOnMode.raft));
        assertFalse(SINGLE.allowsMode(TopologyGraphDbmsModel.HostedOnMode.raft));
        assertTrue(NONE.allowsMode(TopologyGraphDbmsModel.HostedOnMode.raft));

        assertFalse(PRIMARY.allowsMode(TopologyGraphDbmsModel.HostedOnMode.replica));
        assertTrue(SECONDARY.allowsMode(TopologyGraphDbmsModel.HostedOnMode.replica));
        assertFalse(SINGLE.allowsMode(TopologyGraphDbmsModel.HostedOnMode.replica));
        assertTrue(NONE.allowsMode(TopologyGraphDbmsModel.HostedOnMode.replica));

        assertTrue(PRIMARY.allowsMode(TopologyGraphDbmsModel.HostedOnMode.single));
        assertFalse(SECONDARY.allowsMode(TopologyGraphDbmsModel.HostedOnMode.single));
        assertTrue(SINGLE.allowsMode(TopologyGraphDbmsModel.HostedOnMode.single));
        assertTrue(NONE.allowsMode(TopologyGraphDbmsModel.HostedOnMode.single));
    }
}
