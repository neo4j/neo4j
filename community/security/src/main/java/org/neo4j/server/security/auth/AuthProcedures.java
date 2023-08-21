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
package org.neo4j.server.security.auth;

import static java.util.Collections.emptyList;
import static org.neo4j.kernel.impl.security.User.PASSWORD_CHANGE_REQUIRED;
import static org.neo4j.procedure.Mode.DBMS;

import java.util.List;
import java.util.stream.Stream;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.procedure.SystemProcedure;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Procedure;

@SuppressWarnings({"unused", "WeakerAccess"})
public class AuthProcedures {
    @Context
    public SecurityContext securityContext;

    @SystemProcedure
    @Description("Show the current user.")
    @Procedure(name = "dbms.showCurrentUser", mode = DBMS)
    public Stream<UserResult> showCurrentUser() {
        String username = securityContext.subject().executingUser();
        return Stream.of(new UserResult(username, false));
    }

    private static final List<String> changeRequiredList = List.of(PASSWORD_CHANGE_REQUIRED);

    public static class UserResult {
        public final String username;
        public final List<String> roles =
                null; // this is just so that the community version has the same signature as in enterprise
        public final List<String> flags;

        UserResult(String username, boolean changeRequired) {
            this.username = username;
            this.flags = changeRequired ? changeRequiredList : emptyList();
        }
    }
}
