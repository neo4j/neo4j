/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.server.modules;

import org.apache.commons.configuration.Configuration;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.server.NeoServer;
import org.neo4j.server.plugins.Injectable;
import org.neo4j.server.plugins.UnmanagedExtensionLifecycle;

import java.util.Collection;
import java.util.HashSet;

public class ExtensionInitializer
{
    private final Iterable<UnmanagedExtensionLifecycle> extensionLifecycles;
    private final NeoServer neoServer;

    public ExtensionInitializer( NeoServer neoServer )
    {
        this.neoServer = neoServer;
        extensionLifecycles = Service.load( UnmanagedExtensionLifecycle.class );
    }

    public Collection<Injectable<?>> intitializePackages( Iterable<String> packageNames )
    {
        AbstractGraphDatabase graphDatabaseService = neoServer.getDatabase().graph;
        Configuration configuration = neoServer.getConfiguration();

        Collection<Injectable<?>> injectables = new HashSet<Injectable<?>>();
        for ( UnmanagedExtensionLifecycle extensionLifecycle : extensionLifecycles )
        {
            if ( hasPackage( extensionLifecycle, packageNames ) )
            {
                Collection<Injectable<?>> start = extensionLifecycle.start( graphDatabaseService, configuration );
                injectables.addAll( start );
            }
        }
        return injectables;
    }

    private boolean hasPackage( UnmanagedExtensionLifecycle extensionLifecycle, Iterable<String> packageNames )
    {
        String lifecyclePackageName = extensionLifecycle.getClass().getPackage().getName();
        for ( String packageName : packageNames )
        {
            if ( lifecyclePackageName.startsWith( packageName ) )
            {
                return true;
            }
        }
        return false;
    }

    public void stop()
    {
        for ( UnmanagedExtensionLifecycle extensionLifecycle : extensionLifecycles )
        {
            extensionLifecycle.stop();
        }
    }
}
