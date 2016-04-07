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
package org.neo4j.server.security.auth;

import java.io.IOException;

import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.server.security.auth.exception.IllegalCredentialsException;

public interface AuthSubject extends AccessMode
{
    void logout();

    AuthenticationResult getAuthenticationResult();

    /**
     * Set the password for the AuthSubject
     * @param password The new password
     * @throws IOException If the new credentials cannot be serialized to disk.
     * @throws IllegalCredentialsException If the new password is invalid.
     */
    void setPassword( String password ) throws IOException, IllegalCredentialsException;
}
