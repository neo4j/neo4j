/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.harness.internal;

import java.io.File;

import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory.Dependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.AbstractNeoServer;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.database.CommunityGraphFactory;
import org.neo4j.server.database.GraphFactory;

public class InProcessServerBuilder extends AbstractInProcessServerBuilder
{
    public InProcessServerBuilder()
    {
        this( new File( System.getProperty( "java.io.tmpdir" ) ) );
    }

    public InProcessServerBuilder( File workingDir )
    {
        super( workingDir );
    }

    @Override
    protected GraphFactory createGraphFactory( Config config )
    {
        return new CommunityGraphFactory();
    }

    @Override
    protected AbstractNeoServer createNeoServer( GraphFactory graphFactory, Config config, Dependencies dependencies )
    {
        return new CommunityNeoServer( config, graphFactory, dependencies );
    }
}
