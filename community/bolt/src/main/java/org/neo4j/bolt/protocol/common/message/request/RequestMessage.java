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
package org.neo4j.bolt.protocol.common.message.request;

public interface RequestMessage {

    /**
     * Indicates whether this message may be ignored when the state machine is in a failed state.
     * <p />
     * Receiving this message while a state machine is in failed state will result in the connection
     * being killed when this method returns false.
     *
     * @return true if permitted while in failure state, false otherwise.
     * @deprecated This functionality no longer serves a purpose and is to be removed within the
     *             next possible major release.
     */
    @Deprecated(forRemoval = true)
    default boolean isIgnoredWhenFailed() {
        return true;
    }

    /**
     * Indicates whether this message requires entering into the admission control process.
     * @return true if the message should join admission control queue, false otherwise.
     */
    default boolean requiresAdmissionControl() {
        return false;
    }
}
