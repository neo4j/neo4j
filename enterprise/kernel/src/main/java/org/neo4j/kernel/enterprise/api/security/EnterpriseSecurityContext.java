/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.kernel.api.security.Allowance;
import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.security.SecurityContext;

/**
 * A logged in user.
 */
public interface EnterpriseSecurityContext extends SecurityContext, CouldBeAdmin
{
    EnterpriseSecurityContext AUTH_DISABLED = new EnterpriseSecurityContext()
    {
        @Override
        public AuthSubject subject()
        {
            return AuthSubject.AUTH_DISABLED;
        }

        @Override
        public Allowance allows()
        {
            return Allowance.Static.FULL;
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
    };
}
