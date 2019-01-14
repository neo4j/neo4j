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
package org.neo4j.kernel.impl.util;

import java.util.Optional;
import javax.annotation.Nullable;

import org.neo4j.helpers.HostnamePort;

public class OptionalHostnamePort
{
    private Optional<String> hostname;
    private Optional<Integer> port;
    private Optional<Integer> upperRangePort;

    public OptionalHostnamePort( Optional<String> hostname, Optional<Integer> port, Optional<Integer> upperRangePort )
    {
        this.hostname = hostname;
        this.port = port;
        this.upperRangePort = upperRangePort;
    }

    public OptionalHostnamePort( @Nullable String hostname, @Nullable Integer port, @Nullable Integer upperRangePort )
    {
        this.hostname = Optional.ofNullable( hostname );
        this.port = Optional.ofNullable( port );
        this.upperRangePort = Optional.ofNullable( upperRangePort );
    }

    public Optional<String> getHostname()
    {
        return hostname;
    }

    public Optional<Integer> getPort()
    {
        return port;
    }

    public Optional<Integer> getUpperRangePort()
    {
        return upperRangePort;
    }
    @Override
    public String toString()
    {
        return String.format( "OptionalHostnamePort<%s,%s,%s>", hostname, port, upperRangePort );
    }
}
