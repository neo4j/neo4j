/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.server.security.auth;

public interface Privileges
{
    /* Design note: This is just a shell, because we needed to differentiate between an authorized and an unauthorized
       user. These privileges are not currently persisted anywhere, so if you go about introducing new special privs,
       you need to also implement storage for privileges.
     */

    public static final Privileges ADMIN = new Privileges()
    {
        @Override
        public boolean APIAccess()
        {
            return true;
        }
    };

    public static final Privileges NONE = new Privileges() {
        @Override
        public boolean APIAccess()
        {
            return false;
        }
    };

    /** This signals the user has access to the Data API and can execute read and write transactions against the database. */
    boolean APIAccess();
}
