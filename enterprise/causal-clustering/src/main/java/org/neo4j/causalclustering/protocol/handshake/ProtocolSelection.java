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
import java.util.Set;

import org.neo4j.causalclustering.protocol.Protocol;

public abstract class ProtocolSelection<U extends Comparable<U>, T extends Protocol<U>>
{
    private final String identifier;
    private final Set<U> versions;

    public ProtocolSelection( String identifier, Set<U> versions )
    {
        this.identifier = identifier;
        this.versions = Collections.unmodifiableSet( versions );
    }

    public String identifier()
    {
        return identifier;
    }

    public Set<U> versions()
    {
        return versions;
    }
}
