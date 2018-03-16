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

/**
 * Keeps track of protocols which are supported by this instance. This is later used when
 * matching for mutually supported versions during a protocol negotiation.
 *
 * @param <U> Comparable version type.
 * @param <T> Protocol type.
 */
public abstract class SupportedProtocols<U extends Comparable<U>,T extends Protocol<U>>
{
    private final Protocol.Category<T> category;
    private final List<U> versions;

    /**
     * @param category The protocol category.
     * @param versions List of supported versions. An empty list means that every version is supported.
     */
    SupportedProtocols( Protocol.Category<T> category, List<U> versions )
    {
        this.category = category;
        this.versions = Collections.unmodifiableList( versions );
    }

    public Set<U> mutuallySupportedVersionsFor( Set<U> requestedVersions )
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

    public Protocol.Category<T> identifier()
    {
        return category;
    }

    /**
     * @return If an empty list then all versions of a matching protocol will be supported
     */
    public List<U> versions()
    {
        return versions;
    }
}
