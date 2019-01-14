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
package org.neo4j.kernel.impl.api.security;

import org.neo4j.internal.kernel.api.security.AccessMode;

/**
 * Access mode that restricts the original access mode with the restricting mode. Allows things that both the
 * original and the restricting mode allows, while retaining the meta data of the original mode only.
 */
public class RestrictedAccessMode extends WrappedAccessMode
{
    public RestrictedAccessMode( AccessMode original, AccessMode restricting )
    {
        super( original, restricting );
    }

    @Override
    public boolean allowsReads()
    {
        return original.allowsReads() && wrapping.allowsReads();
    }

    @Override
    public boolean allowsWrites()
    {
        return original.allowsWrites() && wrapping.allowsWrites();
    }

    @Override
    public boolean allowsTokenCreates()
    {
        return original.allowsTokenCreates() && wrapping.allowsTokenCreates();
    }

    @Override
    public boolean allowsSchemaWrites()
    {
        return original.allowsSchemaWrites() && wrapping.allowsSchemaWrites();
    }

    @Override
    public boolean allowsPropertyReads( int propertyKey )
    {
        return original.allowsPropertyReads( propertyKey ) && wrapping.allowsPropertyReads( propertyKey );
    }

    @Override
    public boolean allowsProcedureWith( String[] allowed )
    {
        return false;
    }

    @Override
    public String name()
    {
        return original.name() + " restricted to " + wrapping.name();
    }
}
