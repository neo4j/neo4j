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

public class AccessModeSnapshot implements AccessMode
{
    private final boolean allowsReads;
    private final boolean allowsWrites;
    private final boolean allowsSchemaWrites;
    private final boolean overrideOriginalMode;

    private final AccessMode accessMode;

    public static AccessMode createAccessModeSnapshot( AccessMode accessMode )
    {
        return new AccessModeSnapshot( accessMode );
    }

    private AccessModeSnapshot( AccessMode accessMode )
    {
        allowsReads = accessMode.allowsReads();
        allowsWrites = accessMode.allowsWrites();
        allowsSchemaWrites = accessMode.allowsSchemaWrites();
        overrideOriginalMode = accessMode.overrideOriginalMode();

        // We use this for onViolation() and name()
        this.accessMode = accessMode;
    }

    @Override
    public boolean allowsReads()
    {
        return allowsReads;
    }

    @Override
    public boolean allowsWrites()
    {
        return allowsWrites;
    }

    @Override
    public boolean allowsSchemaWrites()
    {
        return allowsSchemaWrites;
    }

    @Override
    public boolean overrideOriginalMode()
    {
        return overrideOriginalMode;
    }

    @Override
    public AuthorizationViolationException onViolation( String msg )
    {
        return accessMode.onViolation( msg );
    }

    @Override
    public String name()
    {
        return accessMode.name();
    }

    // TODO: Move this to AccessMode interface with default implementation to support recursive case
    public AccessMode getOriginalAccessMode()
    {
        return accessMode;
    }
}
