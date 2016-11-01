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
package org.neo4j.kernel.api.security;

/** Controls the capabilities of a KernelTransaction. */
public class AnonymousContext implements SecurityContext
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

    public static AnonymousContext writeOnly()
    {
        return new AnonymousContext( AccessMode.Static.WRITE_ONLY );
    }

    @Override
    public AuthSubject subject()
    {
        return AuthSubject.ANONYMOUS;
    }

    @Override
    public boolean isAdmin()
    {
        return false;
    }

    @Override
    public SecurityContext freeze()
    {
        return this;
    }

    @Override
    public SecurityContext withMode( AccessMode mode )
    {
        return new Frozen( subject(), mode );
    }

    @Override
    public AccessMode mode()
    {
        return accessMode;
    }

    @Override
    public String toString()
    {
        return defaultString( "anonymous" );
    }
}
