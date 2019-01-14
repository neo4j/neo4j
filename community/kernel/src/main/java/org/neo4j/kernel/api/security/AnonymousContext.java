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
package org.neo4j.kernel.api.security;

import java.util.function.Function;

import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;

/** Controls the capabilities of a KernelTransaction. */
public class AnonymousContext implements LoginContext
{
    private final AccessMode accessMode;

    private AnonymousContext( AccessMode accessMode )
    {
        this.accessMode = accessMode;
    }

    public static AnonymousContext none()
    {
        return new AnonymousContext( AccessMode.Static.NONE );
    }

    public static AnonymousContext read()
    {
        return new AnonymousContext( AccessMode.Static.READ );
    }

    public static AnonymousContext write()
    {
        return new AnonymousContext( AccessMode.Static.WRITE );
    }

    public static AnonymousContext writeToken()
    {
        return new AnonymousContext( AccessMode.Static.TOKEN_WRITE );
    }

    public static AnonymousContext writeOnly()
    {
        return new AnonymousContext( AccessMode.Static.WRITE_ONLY );
    }

    public static AnonymousContext full()
    {
        return new AnonymousContext( AccessMode.Static.FULL );
    }

    @Override
    public AuthSubject subject()
    {
        return AuthSubject.ANONYMOUS;
    }

    @Override
    public SecurityContext authorize( Function<String, Integer> propertyIdLookup )
    {
        return new SecurityContext( subject(), accessMode );
    }
}
