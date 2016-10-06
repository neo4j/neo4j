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
package org.neo4j.kernel.impl.api.security;

import org.neo4j.kernel.api.security.Allowance;
import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.security.SecurityContext;

public class OverriddenSecurityContext implements SecurityContext
{

    private final SecurityContext originalContext;
    private final OverriddenAllowance allowance;

    public OverriddenSecurityContext( SecurityContext originalContext, Allowance overriddenAllowance )
    {
        this.originalContext = originalContext;
        this.allowance = new OverriddenAllowance( originalContext.allows(), overriddenAllowance );
    }

    public String username()
    {
        return getUsernameFromSecurityContext( originalContext );
    }

    public static String getUsernameFromSecurityContext( SecurityContext securityContext )
    {
        if ( securityContext instanceof AuthSubject )
        {
            return ((AuthSubject) securityContext).username();
        }
        else if ( securityContext instanceof OverriddenSecurityContext )
        {
            return ((OverriddenSecurityContext) securityContext).username();
        }
        else
        {
            return ""; // Should never clash with a valid username
        }
    }

    @Override
    public Allowance allows()
    {
        return allowance;
    }

    @Override
    public String name()
    {
        return allowance.name();
    }

}
