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
package org.neo4j.server.enterprise;

import java.io.IOException;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.server.NeoServer;
import org.neo4j.server.NeoServerRestartTestIT;
import org.neo4j.server.enterprise.helpers.EnterpriseServerBuilder;
import org.neo4j.server.helpers.CommunityServerBuilder;

public class NeoServerRestartTestEnterpriseIT extends NeoServerRestartTestIT
{
    @Override
    protected NeoServer getNeoServer( String customPageSwapperName ) throws IOException
    {
        CommunityServerBuilder builder = EnterpriseServerBuilder.serverOnRandomPorts()
                .withProperty( GraphDatabaseSettings.pagecache_swapper.name(), customPageSwapperName );
        return builder.build();
    }
}
