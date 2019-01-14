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

import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.kernel.lifecycle.Lifecycle;

public interface UserManagerSupplier extends Lifecycle
{
    UserManager getUserManager( AuthSubject authSubject, boolean isUserManager );

    UserManager getUserManager();

    UserManagerSupplier NO_AUTH = new UserManagerSupplier()
    {
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
        public UserManager getUserManager( AuthSubject authSubject, boolean isUserManager )
        {
            return UserManager.NO_AUTH;
        }

        @Override
        public UserManager getUserManager()
        {
            return UserManager.NO_AUTH;
        }
    };
}
