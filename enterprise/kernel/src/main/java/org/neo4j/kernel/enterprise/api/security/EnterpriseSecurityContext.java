/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
