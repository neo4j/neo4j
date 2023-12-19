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
package org.neo4j.server.rest.causalclustering;

import org.neo4j.server.rest.repr.MappingRepresentation;
import org.neo4j.server.rest.repr.MappingSerializer;

import static org.neo4j.server.rest.causalclustering.CausalClusteringService.AVAILABLE;
import static org.neo4j.server.rest.causalclustering.CausalClusteringService.READ_ONLY;
import static org.neo4j.server.rest.causalclustering.CausalClusteringService.WRITABLE;

public class CausalClusteringDiscovery extends MappingRepresentation
{
    private static final String DISCOVERY_REPRESENTATION_TYPE = "discovery";

    private final String basePath;

    CausalClusteringDiscovery( String basePath )
    {
        super( DISCOVERY_REPRESENTATION_TYPE );
        this.basePath = basePath;
    }

    @Override
    protected void serialize( MappingSerializer serializer )
    {
        serializer.putRelativeUri( AVAILABLE, basePath + "/" + AVAILABLE );
        serializer.putRelativeUri( READ_ONLY, basePath + "/" + READ_ONLY );
        serializer.putRelativeUri( WRITABLE, basePath + "/" + WRITABLE );
    }
}
