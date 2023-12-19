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
package org.neo4j.harness.internal;

import java.io.File;
import java.util.Map;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.server.AbstractNeoServer;
import org.neo4j.server.enterprise.OpenEnterpriseNeoServer;

public class EnterpriseInProcessServerBuilder extends AbstractInProcessServerBuilder
{
    public EnterpriseInProcessServerBuilder()
    {
        this( new File( System.getProperty( "java.io.tmpdir" ) ) );
    }

    public EnterpriseInProcessServerBuilder( File workingDir )
    {
        super( workingDir );
    }

    public EnterpriseInProcessServerBuilder( File workingDir, String dataSubDir )
    {
        super( workingDir, dataSubDir );
    }

    @Override
    protected AbstractNeoServer createNeoServer( Map<String,String> config,
            GraphDatabaseFacadeFactory.Dependencies dependencies, FormattedLogProvider userLogProvider )
    {
        return new OpenEnterpriseNeoServer( Config.defaults( config ), dependencies, userLogProvider );
    }
}
