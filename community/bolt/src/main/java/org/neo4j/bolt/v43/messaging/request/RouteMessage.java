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
package org.neo4j.bolt.v43.messaging.request;

import org.neo4j.bolt.messaging.RequestMessage;

public class RouteMessage implements RequestMessage
{
    public static final byte SIGNATURE = 0x66;
    public static final RouteMessage INSTANCE = new RouteMessage();
    private static final String NAME = "ROUTE";

    private RouteMessage()
    {

    }

    @Override
    public boolean safeToProcessInAnyState()
    {
        return true;
    }

    @Override
    public String toString()
    {
        return NAME;
    }
}
