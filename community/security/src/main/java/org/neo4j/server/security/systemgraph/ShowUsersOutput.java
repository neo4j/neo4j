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
package org.neo4j.server.security.systemgraph;

import java.util.List;
import java.util.Map;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

public class ShowUsersOutput {
    public static final String USERS = "users";
    public static final String COMMAND = "command";
    public static final String USER = "user";
    public static final String ROLES = "roles";
    public static final String PASSWORD_CHANGE_REQ = "passwordChangeRequired";
    public static final String SUSPENDED = "suspended";
    public static final String HOME = "home";
    public static final String PROVIDER = "provider";
    public static final String AUTH = "auth";
    public static final String TAGS = "tags";

    private final List<ShowUsersRow> usersRows;

    public ShowUsersOutput(List<ShowUsersRow> usersRows) {
        this.usersRows = usersRows;
    }

    public MapValue toMapValue() {
        return VirtualValues.singletonMap(
                "__internal_" + USERS,
                VirtualValues.list(usersRows.stream().map(ShowUsersRow::asMap).toArray(AnyValue[]::new)));
    }

    public record ShowUsersRow(
            String command,
            String user,
            List<String> roles,
            Boolean changeReq,
            Boolean suspended,
            String home,
            String provider,
            Map<String, AnyValue> auth,
            List<String> tags) {

        MapValue asMap() {
            return VirtualValues.map(
                    new String[] {COMMAND, USER, ROLES, PASSWORD_CHANGE_REQ, SUSPENDED, HOME, PROVIDER, AUTH, TAGS},
                    new AnyValue[] {
                        Values.stringOrNoValue(command),
                        Values.stringValue(user),
                        roles != null
                                ? VirtualValues.list(
                                        roles.stream().map(Values::stringValue).toArray(AnyValue[]::new))
                                : Values.NO_VALUE,
                        changeReq != null ? Values.booleanValue(changeReq) : Values.NO_VALUE,
                        suspended != null ? Values.booleanValue(suspended) : Values.NO_VALUE,
                        Values.stringOrNoValue(home),
                        Values.stringOrNoValue(provider),
                        auth != null
                                ? VirtualValues.map(
                                        auth.keySet().toArray(new String[0]),
                                        auth.values().toArray(new AnyValue[0]))
                                : Values.NO_VALUE,
                        tags != null
                                ? VirtualValues.list(
                                        tags.stream().map(Values::stringValue).toArray(AnyValue[]::new))
                                : Values.NO_VALUE
                    });
        }
    }
}
