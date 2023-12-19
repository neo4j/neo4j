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

import java.util.Map;

import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;

public interface EnterpriseAuthManager extends AuthManager
{
    void clearAuthCache();

    @Override
    EnterpriseLoginContext login( Map<String,Object> authToken ) throws InvalidAuthTokenException;

    /**
     * Implementation that does no authentication.
     */
    EnterpriseAuthManager NO_AUTH = new EnterpriseAuthManager()
    {
        @Override
        public EnterpriseLoginContext login( Map<String,Object> authToken )
        {
            return EnterpriseLoginContext.AUTH_DISABLED;
        }

        @Override
        public void init()
        {
        }

        @Override
        public void start()
        {
        }

        @Override
        public void stop()
        {
        }

        @Override
        public void shutdown()
        {
        }

        @Override
        public void clearAuthCache()
        {
        }
    };
}
