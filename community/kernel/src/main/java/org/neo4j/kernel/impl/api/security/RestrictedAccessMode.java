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

public class RestrictedAccessMode implements AccessMode
{
    private final AccessMode originalMode;
    private final AccessMode restrictedMode;

    public RestrictedAccessMode( AccessMode originalMode, AccessMode restrictedMode )
    {
        this.originalMode = originalMode;
        this.restrictedMode = restrictedMode;
    }

    @Override
    public boolean allowsReads()
    {
        return restrictedMode.allowsReads() &&
               (restrictedMode.overrideOriginalMode() || originalMode.allowsReads());
    }

    @Override
    public boolean allowsWrites()
    {
        return restrictedMode.allowsWrites() &&
               (restrictedMode.overrideOriginalMode() || originalMode.allowsWrites());
    }

    @Override
    public boolean allowsSchemaWrites()
    {
        return restrictedMode.allowsSchemaWrites() &&
               (restrictedMode.overrideOriginalMode() || originalMode.allowsSchemaWrites());
    }

    @Override
    public boolean overrideOriginalMode()
    {
        return false;
    }

    @Override
    public AuthorizationViolationException onViolation( String msg )
    {
        return restrictedMode.onViolation( msg );
    }

    @Override
    public String name()
    {
        if ( restrictedMode.overrideOriginalMode() )
        {
            return originalMode.name() + " overridden by " + restrictedMode.name();
        }
        else
        {
            return originalMode.name() + " restricted to " + restrictedMode.name();
        }
    }
}
