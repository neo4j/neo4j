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

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.security.AccessMode;

/**
 * Access mode that wraps an access mode with a wrapping access mode. The resulting access mode allows things based
 * on both the original and the wrapping mode, while retaining the meta data of the original mode only.
 */
abstract class WrappedAccessMode implements AccessMode
{
    protected final AccessMode original;
    protected final AccessMode wrapping;

    WrappedAccessMode( AccessMode original, AccessMode wrapping )
    {
        this.original = original;
        this.wrapping = wrapping;
    }

    @Override
    public AuthorizationViolationException onViolation( String msg )
    {
        return wrapping.onViolation( msg );
    }

    @Override
    public boolean isOverridden()
    {
        return true;
    }
}
