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
package org.neo4j.test.server.ha;

import java.io.File;
import java.io.IOException;

import org.neo4j.server.enterprise.OpenEnterpriseNeoServer;
import org.neo4j.server.enterprise.helpers.EnterpriseServerBuilder;

public class EnterpriseServerHelper
{
    private EnterpriseServerHelper()
    {
    }

    public static OpenEnterpriseNeoServer createNonPersistentServer( File databaseDir ) throws IOException
    {
        return createServer( databaseDir, false );
    }

    private static OpenEnterpriseNeoServer createServer( File databaseDir, boolean persistent ) throws IOException
    {
        EnterpriseServerBuilder builder = EnterpriseServerBuilder.serverOnRandomPorts().usingDataDir( databaseDir.getAbsolutePath() );
        if ( persistent )
        {
            builder = (EnterpriseServerBuilder) builder.persistent();
        }
        builder.withDefaultDatabaseTuning();
        OpenEnterpriseNeoServer server = builder.build();
        server.start();
        return server;
    }
}
