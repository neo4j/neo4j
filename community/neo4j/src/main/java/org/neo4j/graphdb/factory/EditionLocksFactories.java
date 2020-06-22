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
package org.neo4j.graphdb.factory;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.LocksFactory;
import org.neo4j.lock.ResourceTypes;
import org.neo4j.logging.internal.LogService;
import org.neo4j.service.Services;
import org.neo4j.time.SystemNanoClock;

public final class EditionLocksFactories
{
    private EditionLocksFactories()
    {
    }

    public static Locks createLockManager( LocksFactory locksFactory, Config config, SystemNanoClock clock )
    {
        return locksFactory.newInstance( config, clock, ResourceTypes.values() );
    }

    public static LocksFactory createLockFactory( Config config, LogService logService )
    {
        LocksFactory locksFactory = getLocksFactory( config.get( GraphDatabaseInternalSettings.lock_manager ) );
        logService.getInternalLog( EditionLocksFactories.class ).info( "Locking implementation '" + locksFactory.getName() + "' selected." );
        return locksFactory;
    }

    private static LocksFactory getLocksFactory( String key )
    {
        if ( key.isEmpty() )
        {
            return Services.loadByPriority( LocksFactory.class ).orElseThrow( () -> new IllegalArgumentException( "No lock manager found" ) );
        }

        return Services.load( LocksFactory.class, key )
                .orElseThrow(() -> new IllegalArgumentException( "No lock manager found with the name '" + key + "'." ) );
    }
}
