/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.protocol.handshake;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.protocol.Protocol;

public class SupportedProtocols<T extends Protocol>
{
    private final Protocol.Identifier<T> identifier;
    private final List<Integer> versions;

    /**
     * @param versions Empty means support everything
     */
    public SupportedProtocols( Protocol.Identifier<T> identifier, List<Integer> versions )
    {
        this.identifier = identifier;
        this.versions = Collections.unmodifiableList( versions );
    }

    public Set<Integer> mutuallySupportedVersionsFor( Set<Integer> requestedVersions )
    {
        if ( versions().isEmpty() )
        {
            return requestedVersions;
        }
        else
        {
            return requestedVersions.stream().filter( versions()::contains ).collect( Collectors.toSet() );
        }
    }

    public Protocol.Identifier<T> identifier()
    {
        return identifier;
    }

    /**
     * @return If an empty list then all versions of a matching protocol will be supported
     */
    public List<Integer> versions()
    {
        return versions;
    }
}
