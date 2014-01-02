/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.cleanup;

import java.lang.ref.PhantomReference;

import org.neo4j.graphdb.Resource;

class CleanupReference extends PhantomReference<Object>
{
    private final ReferenceQueueBasedCleanupService cleanupService;
    private final String referenceDescription;

    private Resource resource;
    CleanupReference prev, next;

    CleanupReference( Object referent, ReferenceQueueBasedCleanupService cleanupService, Resource resource )
    {
        super( referent, cleanupService.collectedReferences.queue );
        this.referenceDescription = referent.toString();
        this.cleanupService = cleanupService;
        this.resource = resource;
    }

    public String description()
    {
        return referenceDescription;
    }

    void cleanupNow( boolean explicit )
    {
        boolean shouldUnlink = false;
        try
        {
            synchronized ( this )
            {
                if ( resource != null )
                {
                    shouldUnlink = true;
                    if ( !explicit )
                    {
                        cleanupService.logLeakedReference( this );
                    }
                    try
                    {
                        resource.close();
                    }
                    finally
                    {
                        resource = null;
                    }
                }
            }
        }
        finally
        {
            if ( shouldUnlink )
            {
                cleanupService.unlink( this );
            }
        }
    }
}
