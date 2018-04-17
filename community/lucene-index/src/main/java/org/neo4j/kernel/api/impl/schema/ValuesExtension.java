/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.schema;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.values.storable.Values;

public class ValuesExtension implements Lifecycle
{
    private final LifeSupport life = new LifeSupport();
    private Config configuration;

    ValuesExtension( ValuesKernelExtensionFactory.Dependencies dependencies )
    {
        configuration = dependencies.getConfig();
    }

    @Override
    public void init()
    {
        life.init();
    }

    @Override
    public void start()
    {
        life.start();
        int nanoPrecisionConfig = configuration.get( GraphDatabaseSettings.temporal_nanosecond_precision );

        // If two db instances in the same JVM try to set this to different values, the last setting will override the previous one
        if ( nanoPrecisionConfig != Values.getNanoPrecision() && Values.nanoPrecisionConfigAltered )
        {
            throw new IllegalArgumentException( "dbms.temporal.nanosecond_precision can only be set once" );
        }
        Values.setNanoPrecision( nanoPrecisionConfig );
        Values.nanoPrecisionConfigAltered = true;
    }

    @Override
    public void stop()
    {
        life.stop();
    }

    @Override
    public void shutdown()
    {
        Values.nanoPrecisionConfigAltered = false;
        life.shutdown();
    }
}
