/*
 * Copyright (c) "Neo4j"
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

import org.eclipse.jetty.util.StringUtil;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.LocksFactory;
import org.neo4j.kernel.impl.locking.forseti.ForsetiLocksFactory;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.internal.LogService;
import org.neo4j.service.Services;
import org.neo4j.time.SystemNanoClock;

public final class EditionLocksFactories
{
    private static final String OLD_COMMUNITY_LOCK_MANAGER_NAME = "community";

    private EditionLocksFactories()
    {
    }

    public static Locks createLockManager( LocksFactory locksFactory, Config config, SystemNanoClock clock )
    {
        return locksFactory.newInstance( config, clock );
    }

    public static LocksFactory createLockFactory( Config config, LogService logService )
    {
        InternalLog lockFactoriesLog = logService.getInternalLog( EditionLocksFactories.class );
        LocksFactory locksFactory = getLocksFactory( config.get( GraphDatabaseInternalSettings.lock_manager ), lockFactoriesLog );
        lockFactoriesLog.info( "Locking implementation '" + locksFactory.getName() + "' selected." );
        return locksFactory;
    }

    private static LocksFactory getLocksFactory( String key, InternalLog lockFactoriesLog )
    {
        // we can have community lock manager configured in the wild. Ignore that and log warning message.
        var factoryKey = checkForOldCommunityValue( lockFactoriesLog, key );
        if ( StringUtil.isEmpty( factoryKey ) )
        {
            return Services.loadByPriority( LocksFactory.class ).orElseThrow( () -> new IllegalArgumentException( "No lock manager found" ) );
        }

        return Services.load( LocksFactory.class, factoryKey )
                .orElseThrow(() -> new IllegalArgumentException( "No lock manager found with the name '" + key + "'." ) );
    }

    private static String checkForOldCommunityValue( InternalLog lockFactoriesLog, String factoryKey )
    {
        if ( OLD_COMMUNITY_LOCK_MANAGER_NAME.equals( factoryKey ) )
        {
            lockFactoriesLog.warn( "Old community lock manager is configured to be used. Ignoring and fallback to default lock manager." );
            return ForsetiLocksFactory.FORSETI_LOCKS_NAME;
        }
        return factoryKey;
    }
}
