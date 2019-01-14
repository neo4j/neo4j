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
package org.neo4j.kernel.impl.api;

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.ResourceManager;
import org.neo4j.kernel.api.exceptions.ResourceCloseFailureException;

public class CloseableResourceManager implements ResourceManager
{
    private Collection<AutoCloseable> closeableResources;

    // ResourceTracker

    @Override
    public final void registerCloseableResource( AutoCloseable closeable )
    {
        if ( closeableResources == null )
        {
            closeableResources = new ArrayList<>( 8 );
        }
        closeableResources.add( closeable );
    }

    @Override
    public final void unregisterCloseableResource( AutoCloseable closeable )
    {
        if ( closeableResources != null )
        {
            closeableResources.remove( closeable );
        }
    }

    // ResourceManager

    @Override
    public final void closeAllCloseableResources()
    {
        if ( closeableResources != null )
        {
            // Make sure we reset closeableResource before doing anything which may throw an exception that
            // _may_ result in a recursive call to this close-method
            Collection<AutoCloseable> resourcesToClose = closeableResources;
            closeableResources = null;

            IOUtils.closeAll( ResourceCloseFailureException.class, resourcesToClose.toArray( new AutoCloseable[0] ) );
        }
    }
}
