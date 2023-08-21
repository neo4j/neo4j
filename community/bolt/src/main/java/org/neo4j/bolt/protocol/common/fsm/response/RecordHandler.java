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
package org.neo4j.bolt.protocol.common.fsm.response;

import org.neo4j.values.AnyValue;

/**
 * Handles the conversion of records to their network representation.
 */
public interface RecordHandler {

    /**
     * Handles the beginning of a new record.
     */
    default void onBegin() {}

    /**
     * Consumes the contents of a given field within the current record.
     *
     * @param value an arbitrarily typed field value.
     */
    void onField(AnyValue value);

    /**
     * Handles the successful completion of a given record.
     */
    void onCompleted();

    /**
     * Handles the exceptional termination of a record.
     * <p />
     * When this function is invoked, implementations are generally expected to discard any
     * previously submitted fields if possible and prematurely terminate the stream.
     * <p />
     * Note that additional error information will be provided to the parent response handler
     * instance in this case.
     */
    void onFailure();
}
