/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.performance.domain;

public final class Units
{
    // Generic concepts

    /**
     * An operation of any kind.
     */
    public static final Unit OPERATION = new Unit( "operation" );

    /**
     * Average operation, to be used to denote "average ops per second"
     */
    public static final Unit AVERAGE_OPERATION = new Unit( "avg. operation" );

    // Neo concepts

    /**
     * A basic write transaction that uses the core API basic functions.
     */
    public static final Unit CORE_API_WRITE_TRANSACTION = new Unit( "write tx" );

    /**
     * This represents a non-transactional read operation
     * using the core API and indexes.
     */
    public static final Unit CORE_API_READ = new Unit( "core API read" );

    // Time units

    public static final Unit MINUTE      = new Unit("minute");
    public static final Unit SECOND      = new Unit("s");
    public static final Unit MILLISECOND = new Unit("ms");
    public static final Unit MICROSECOND = new Unit("µs");
    public static final Unit NANOSECOND  = new Unit("ns");

    // Other

    public static final Unit UNKNOWN = new Unit( "unknown" );
    public static final Unit BYTE = new Unit( "b" );
    public static final Unit OBJECT = new Unit( "object" );

    private Units(){};
}
