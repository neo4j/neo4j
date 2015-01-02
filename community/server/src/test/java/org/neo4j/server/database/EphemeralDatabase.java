/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.server.database;

import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.test.TestGraphDatabaseFactory;

public class EphemeralDatabase extends CommunityDatabase
{
    public EphemeralDatabase( Configurator configurator )
    {
        super( configurator, DevNullLoggingService.DEV_NULL );
    }

    @Override
    protected AbstractGraphDatabase createDb()
    {
        return (AbstractGraphDatabase) new TestGraphDatabaseFactory()
            .newImpermanentDatabaseBuilder()
            .setConfig( getDbTuningPropertiesWithServerDefaults() )
            .newGraphDatabase();
    }

    @Override
    public void shutdown()
    {
        if ( this.getGraph() != null )
        {
            this.getGraph().shutdown();
        }
    }
}
