/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.graphdb.factory;

import java.time.Clock;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.kernel.impl.locking.DynamicLocksFactory;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.LocksFactory;
import org.neo4j.kernel.impl.locking.community.CommunityLocksFactory;
import org.neo4j.lock.ResourceTypes;
import org.neo4j.service.Services;

public final class EditionLocksFactories
{
    public static Locks createLockManager( LocksFactory locksFactory, Config config, Clock clock )
    {
        return locksFactory.newInstance( config, clock, ResourceTypes.values() );
    }

    public static LocksFactory createLockFactory( Config config )
    {
        String key = config.get( GraphDatabaseSettings.lock_manager );
        for ( DynamicLocksFactory candidate : Services.loadAll( DynamicLocksFactory.class ) )
        {
            if ( key.equals( candidate.getName() ) )
            {
                return candidate;
            }
        }

        if ( CommunityLocksFactory.NAME.equals( key ) )
        {
            return new CommunityLocksFactory();
        }

        throw new IllegalArgumentException( "No lock manager found with the name '" + key + "'." );
    }
}
