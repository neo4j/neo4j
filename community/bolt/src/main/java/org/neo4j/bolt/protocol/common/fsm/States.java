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
package org.neo4j.bolt.protocol.common.fsm;

import org.neo4j.bolt.fsm.state.StateReference;

public final class States {
    private States() {}

    public static final StateReference NEGOTIATION = new StateReference("NEGOTIATION");
    public static final StateReference AUTHENTICATION = new StateReference("AUTHENTICATION");

    public static final StateReference READY = new StateReference("READY");

    public static final StateReference AUTO_COMMIT = new StateReference("AUTO_COMMIT");
    public static final StateReference IN_TRANSACTION = new StateReference("IN_TRANSACTION");
}
