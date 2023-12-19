/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
