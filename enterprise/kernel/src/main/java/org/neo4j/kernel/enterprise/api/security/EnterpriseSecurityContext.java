/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.enterprise.api.security;

import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.SecurityContext;

/**
 * A logged in and authorized user.
 */
public class EnterpriseSecurityContext extends SecurityContext
{
    private final Set<String> roles;
    private final boolean isAdmin;

    public EnterpriseSecurityContext( AuthSubject subject, AccessMode mode, Set<String> roles, boolean isAdmin )
    {
        super( subject, mode );
        this.roles = roles;
        this.isAdmin = isAdmin;
    }

    @Override
    public boolean isAdmin()
    {
        return isAdmin;
    }

    @Override
    public EnterpriseSecurityContext authorize( Function<String, Integer> propertyIdLookup )
    {
        return this;
    }

    @Override
    public EnterpriseSecurityContext withMode( AccessMode mode )
    {
        return new EnterpriseSecurityContext( subject, mode, roles, isAdmin );
    }

    /**
     * Get the roles of the authenticated user.
     */
    public Set<String> roles()
    {
        return roles;
    }

    /** Allows all operations. */
    public static final EnterpriseSecurityContext AUTH_DISABLED = authDisabled( AccessMode.Static.FULL );

    private static EnterpriseSecurityContext authDisabled( AccessMode mode )
    {
        return new EnterpriseSecurityContext( AuthSubject.AUTH_DISABLED, mode, Collections.emptySet(), true )
        {

            @Override
            public EnterpriseSecurityContext withMode( AccessMode mode )
            {
                return authDisabled( mode );
            }

            @Override
            public String description()
            {
                return "AUTH_DISABLED with " + mode().name();
            }

            @Override
            public String toString()
            {
                return defaultString( "enterprise-auth-disabled" );
            }
        };
    }
}
