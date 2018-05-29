/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.security.SecurityContext;

/**
 * A logged in user.
 */
public interface EnterpriseSecurityContext extends SecurityContext
{
    @Override
    EnterpriseSecurityContext freeze();

    @Override
    EnterpriseSecurityContext withMode( AccessMode mode );

    Set<String> roles();

    EnterpriseSecurityContext AUTH_DISABLED = new AuthDisabled( AccessMode.Static.FULL );

    /** Allows all operations. */
    final class AuthDisabled implements EnterpriseSecurityContext
    {
        private final AccessMode mode;

        private AuthDisabled( AccessMode mode )
        {
            this.mode = mode;
        }

        @Override
        public EnterpriseSecurityContext freeze()
        {
            return this;
        }

        @Override
        public EnterpriseSecurityContext withMode( AccessMode mode )
        {
            return new EnterpriseSecurityContext.AuthDisabled( mode );
        }

        @Override
        public Set<String> roles()
        {
            return Collections.emptySet();
        }

        @Override
        public AuthSubject subject()
        {
            return AuthSubject.AUTH_DISABLED;
        }

        @Override
        public AccessMode mode()
        {
            return mode;
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

        @Override
        public boolean isAdmin()
        {
            return true;
        }
    }

    final class Frozen implements EnterpriseSecurityContext
    {
        private final AuthSubject subject;
        private final AccessMode mode;
        private final Set<String> roles;
        private final boolean isAdmin;

        public Frozen( AuthSubject subject, AccessMode mode, Set<String> roles, boolean isAdmin )
        {
            this.subject = subject;
            this.mode = mode;
            this.roles = roles;
            this.isAdmin = isAdmin;
        }

        @Override
        public boolean isAdmin()
        {
            return isAdmin;
        }

        @Override
        public AccessMode mode()
        {
            return mode;
        }

        @Override
        public AuthSubject subject()
        {
            return subject;
        }

        @Override
        public EnterpriseSecurityContext freeze()
        {
            return this;
        }

        @Override
        public EnterpriseSecurityContext withMode( AccessMode mode )
        {
            return new EnterpriseSecurityContext.Frozen( subject, mode, roles, isAdmin );
        }

        @Override
        public Set<String> roles()
        {
            return roles;
        }
    }
}
