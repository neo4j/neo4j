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
package org.neo4j.causalclustering.routing;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.neo4j.causalclustering.discovery.ClientConnector;
import org.neo4j.helpers.AdvertisedSocketAddress;

import static java.util.Collections.emptyList;

public class Util
{
    private Util()
    {
    }

    public static <T> List<T> asList( @SuppressWarnings( "OptionalUsedAsFieldOrParameterType" ) Optional<T> optional )
    {
        return optional.map( Collections::singletonList ).orElse( emptyList() );
    }

    public static Function<ClientConnector,AdvertisedSocketAddress> extractBoltAddress()
    {
        return c -> c.connectors().boltAddress();
    }
}
