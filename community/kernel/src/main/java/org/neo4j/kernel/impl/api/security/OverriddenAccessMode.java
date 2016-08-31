/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.security;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.api.security.AuthSubject;

public class OverriddenAccessMode implements AccessMode
{
    private final AccessMode originalMode;
    private final AccessMode overriddenMode;

    public OverriddenAccessMode( AccessMode originalMode, AccessMode overriddenMode )
    {
        this.originalMode = originalMode;
        this.overriddenMode = overriddenMode;
    }

    @Override
    public boolean allowsReads()
    {
        return overriddenMode.allowsReads() &&
               (overriddenMode.overrideOriginalMode() || originalMode.allowsReads());
    }

    @Override
    public boolean allowsWrites()
    {
        return overriddenMode.allowsWrites() &&
               (overriddenMode.overrideOriginalMode() || originalMode.allowsWrites());
    }

    @Override
    public boolean allowsSchemaWrites()
    {
        return overriddenMode.allowsSchemaWrites() &&
               (overriddenMode.overrideOriginalMode() || originalMode.allowsSchemaWrites());
    }

    @Override
    public boolean overrideOriginalMode()
    {
        return false;
    }

    @Override
    public AuthorizationViolationException onViolation( String msg )
    {
        return overriddenMode.onViolation( msg );
    }

    @Override
    public String name()
    {
        if ( overriddenMode.overrideOriginalMode() )
        {
            return originalMode.name() + " overridden by " + overriddenMode.name();
        }
        else
        {
            return originalMode.name() + " restricted to " + overriddenMode.name();
        }
    }

    public String username()
    {
        return getUsernameFromAccessMode( originalMode );
    }

    public static String getUsernameFromAccessMode( AccessMode accessMode )
    {
        if ( accessMode instanceof AuthSubject )
        {
            return ((AuthSubject) accessMode).username();
        }
        else if ( accessMode instanceof OverriddenAccessMode )
        {
            return ((OverriddenAccessMode) accessMode).username();
        }
        else
        {
            return ""; // Should never clash with a valid username
        }
    }
}
