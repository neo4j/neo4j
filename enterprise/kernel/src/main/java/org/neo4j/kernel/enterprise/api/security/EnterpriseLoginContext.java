/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.enterprise.api.security;

import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.LoginContext;

public interface EnterpriseLoginContext extends LoginContext
{
    Set<String> roles();

    EnterpriseSecurityContext authorize( Function<String, Integer> propertyIdLookup );

    EnterpriseLoginContext AUTH_DISABLED = new EnterpriseLoginContext()
    {
        @Override
        public AuthSubject subject()
        {
            return AuthSubject.AUTH_DISABLED;
        }

        @Override
        public Set<String> roles()
        {
            return Collections.emptySet();
        }

        @Override
        public EnterpriseSecurityContext authorize( Function<String, Integer> propertyIdLookup )
        {
            return EnterpriseSecurityContext.AUTH_DISABLED;
        }
    };
}
