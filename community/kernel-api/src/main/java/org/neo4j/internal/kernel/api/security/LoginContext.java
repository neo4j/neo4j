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
package org.neo4j.internal.kernel.api.security;

import org.neo4j.exceptions.KernelException;

/**
 * The LoginContext hold the executing authenticated user (subject).
 * By calling {@link #authorize(IdLookup, String)} the user is also authorized, and a full SecurityContext is returned,
 * which can be used to assert user permissions during query execution.
 */
public interface LoginContext
{
    /**
     * Get the authenticated user.
     */
    AuthSubject subject();

    /**
     * Authorize the user and return a SecurityContext.
     *
     * @param idLookup token lookup, used to compile fine grained security verification
     * @param dbName the name of the database the user should be authorized against
     * @return the security context
     */
    SecurityContext authorize( IdLookup idLookup, String dbName ) throws KernelException;

    LoginContext AUTH_DISABLED = new LoginContext()
    {
        @Override
        public AuthSubject subject()
        {
            return AuthSubject.AUTH_DISABLED;
        }

        @Override
        public SecurityContext authorize( IdLookup idLookup, String dbName )
        {
            return SecurityContext.AUTH_DISABLED;
        }
    };

    interface IdLookup
    {
        int getOrCreatePropertyKeyId( String name ) throws KernelException;

        int getOrCreateLabelId( String name ) throws KernelException;

        IdLookup EMPTY = new IdLookup()
        {
            @Override
            public int getOrCreatePropertyKeyId( String name )
            {
                return -1;
            }

            @Override
            public int getOrCreateLabelId( String name )
            {
                return -1;
            }
        };
    }
}
