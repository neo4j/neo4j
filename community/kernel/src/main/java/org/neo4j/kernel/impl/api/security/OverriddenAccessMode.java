/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.AccessMode;

/**
 * Access mode that overrides the original access mode with the overriding mode. Allows exactly what the overriding
 * mode allows, while retaining the meta data of the original mode only.
 */
public class OverriddenAccessMode extends WrappedAccessMode
{
    public OverriddenAccessMode( AccessMode original, AccessMode overriding )
    {
        super( original, overriding );
    }

    @Override
    public boolean allowsReads()
    {
        return wrapping.allowsReads();
    }

    @Override
    public boolean allowsWrites()
    {
        return wrapping.allowsWrites();
    }

    @Override
    public boolean allowsTokenCreates()
    {
        return wrapping.allowsTokenCreates();
    }

    @Override
    public boolean allowsSchemaWrites()
    {
        return wrapping.allowsSchemaWrites();
    }

    @Override
    public boolean allowsProcedureWith( String[] allowed ) throws InvalidArgumentsException
    {
        return false;
    }

    @Override
    public String name()
    {
        return original.name() + " overridden by " + wrapping.name();
    }
}
