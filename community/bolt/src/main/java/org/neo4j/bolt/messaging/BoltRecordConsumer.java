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
package org.neo4j.bolt.messaging;

import java.io.IOException;

import org.neo4j.values.AnyValue;

/**
 * Defines how a record is consumed.
 * <p>
 * The consumer expects to be called like:
 * <pre>
 *     beginRecord(3);
 *     consumeField(0, "foo");
 *     consumeField(1, "bar");
 *     consumeField(2, "baz");
 *     endRecord();
 * </pre>
 */
public interface BoltRecordConsumer
{
    /**
     * Called once before the each received record.
     * @param numberOfFields The number of fields in the record
     */
    void beginRecord( int numberOfFields ) throws IOException;

    /**
     * Called once for each field of the record, in order of increasing offsets.
     * @param value the value of this field
     */
    void consumeField( AnyValue value ) throws IOException;

    /**
     * Called once at the end of each received record.
     */
    void endRecord() throws IOException;

    /**
     * Called when an error is received.
     */
    void onError() throws IOException;
}
