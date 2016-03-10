/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.enterprise;

import java.util.HashMap;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.BlockingBootstrapper;
import org.neo4j.server.CommunityBootstrapper;
import org.neo4j.server.NeoServer;

import static java.util.Arrays.asList;

import static org.neo4j.server.enterprise.EnterpriseServerSettings.mode;

public class EnterpriseBootstrapper extends CommunityBootstrapper
{
    public static void main( String[] args )
    {
        int exit = start( new EnterpriseBootstrapper(), args );
        if ( exit != 0 )
        {
            System.exit( exit );
        }
    }

    private static BlockingBootstrapper bootstrapper;

    public static void start( String[] args )
    {
        bootstrapper = new BlockingBootstrapper( new EnterpriseBootstrapper() );
        System.exit( start( bootstrapper, args ) );
    }

    public static void stop( @SuppressWarnings("UnusedParameters") String[] args )
    {
        if ( bootstrapper != null )
        {
            bootstrapper.stop();
        }
    }

    @Override
    protected NeoServer createNeoServer( Config configurator, GraphDatabaseDependencies dependencies, LogProvider
            userLogProvider )
    {
        return new EnterpriseNeoServer( configurator, dependencies, userLogProvider );
    }

    @Override
    protected Iterable<Class<?>> settingsClasses( HashMap<String, String> settings )
    {
        if ( isHAMode( settings ) )
        {
            return Iterables.concat(
                    super.settingsClasses( settings ),
                    asList( HaSettings.class, ClusterSettings.class ) );
        }
        else
        {
            return super.settingsClasses( settings );
        }
    }

    private boolean isHAMode( HashMap<String, String> settings )
    {
        return new Config( settings, EnterpriseServerSettings.class ).get( mode ).equals( "HA" );
    }
}
