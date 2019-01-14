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
package org.neo4j.bolt.logging;

import java.util.function.Supplier;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.virtual.MapValue;

public interface BoltMessageLogger
{
    void clientEvent( String eventName );

    void clientEvent( String eventName, Supplier<String> detailsSupplier );

    void clientError( String eventName, String errorMessage, Supplier<String> detailsSupplier );

    void serverEvent( String eventName );

    void serverEvent( String eventName, Supplier<String> detailsSupplier );

    void serverError( String eventName, String errorMessage );

    void serverError( String eventName, Status status );

    void logInit( String userAgent );

    void logRun();

    void logPullAll();

    void logDiscardAll();

    void logAckFailure();

    void logReset();

    void logSuccess( Supplier<MapValue> metadataSupplier );

    void logFailure( Status status );

    void logIgnored();
}
