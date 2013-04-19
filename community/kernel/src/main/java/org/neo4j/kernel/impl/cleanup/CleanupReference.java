/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.PhantomReference;

class CleanupReference extends PhantomReference<Object>
{
    private final ReferenceQueueBasedCleanupService cleanupService;
    private final String referenceDescription;

    private Closeable handler;
    CleanupReference prev, next;

    CleanupReference( Object referent, ReferenceQueueBasedCleanupService cleanupService, Closeable handler )
    {
        super( referent, cleanupService.collectedReferences.queue );
        this.referenceDescription = referent.toString();
        this.cleanupService = cleanupService;
        this.handler = handler;
    }

    public String description()
    {
        return referenceDescription;
    }

    void cleanupNow( boolean explicit ) throws IOException
    {
        boolean shouldUnlink = false;
        try
        {
            synchronized ( this )
            {
                if ( handler != null )
                {
                    shouldUnlink = true;
                    if ( !explicit )
                    {
                        cleanupService.logLeakedReference( this );
                    }
                    try
                    {
                        handler.close();
                    }
                    finally
                    {
                        handler = null;
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
