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
package org.neo4j.server.rest;

import org.neo4j.server.rest.repr.MappingRepresentation;
import org.neo4j.server.rest.repr.MappingSerializer;

public class CoreDatabaseAvailabilityDiscoveryRepresentation extends MappingRepresentation
{
    private static final String WRITABLE_KEY = "writable";
    private static final String DISCOVERY_REPRESENTATION_TYPE = "discovery";

    private final String basePath;
    private final String isWritableUri;

    public CoreDatabaseAvailabilityDiscoveryRepresentation( String basePath, String isWritableUri )
    {
        super( DISCOVERY_REPRESENTATION_TYPE );
        this.basePath = basePath;
        this.isWritableUri = isWritableUri;
    }

    @Override
    protected void serialize( MappingSerializer serializer )
    {
        serializer.putRelativeUri( WRITABLE_KEY, basePath + isWritableUri );
    }
}
