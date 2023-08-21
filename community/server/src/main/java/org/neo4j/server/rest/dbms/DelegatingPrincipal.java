/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.server.rest.dbms;

import java.security.Principal;
import org.neo4j.internal.kernel.api.security.LoginContext;

public class DelegatingPrincipal implements Principal {
    private String username;
    private final LoginContext loginContext;

    DelegatingPrincipal(String username, LoginContext loginContext) {
        this.username = username;
        this.loginContext = loginContext;
    }

    @Override
    public String getName() {
        return username;
    }

    public LoginContext getLoginContext() {
        return loginContext;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DelegatingPrincipal that)) {
            return false;
        }

        return username.equals(that.username);
    }

    @Override
    public int hashCode() {
        return username.hashCode();
    }

    @Override
    public String toString() {
        return "DelegatingPrincipal{" + "username='" + username + '\'' + '}';
    }
}
