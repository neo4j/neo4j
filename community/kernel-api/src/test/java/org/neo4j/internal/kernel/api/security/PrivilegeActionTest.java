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
package org.neo4j.internal.kernel.api.security;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ACCESS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ADMIN;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ALIAS_MANAGEMENT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ALTER_ALIAS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ALTER_DATABASE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ALTER_USER;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ASSIGN_PRIVILEGE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ASSIGN_ROLE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.COMPOSITE_DATABASE_MANAGEMENT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CONSTRAINT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CREATE_ALIAS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CREATE_COMPOSITE_DATABASE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CREATE_CONSTRAINT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CREATE_DATABASE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CREATE_ELEMENT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CREATE_INDEX;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CREATE_LABEL;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CREATE_PROPERTYKEY;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CREATE_RELTYPE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CREATE_ROLE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CREATE_USER;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DATABASE_ACTIONS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DATABASE_MANAGEMENT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DBMS_ACTIONS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DELETE_ELEMENT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DROP_ALIAS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DROP_COMPOSITE_DATABASE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DROP_CONSTRAINT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DROP_DATABASE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DROP_INDEX;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DROP_ROLE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DROP_USER;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.EXECUTE_ADMIN;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.GRAPH_ACTIONS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.IMPERSONATE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.INDEX;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.LOAD;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.LOAD_CIDR;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.LOAD_URL;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.MATCH;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.MERGE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.PRIVILEGE_MANAGEMENT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.READ;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.REMOVE_LABEL;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.REMOVE_PRIVILEGE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.REMOVE_ROLE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.RENAME_ROLE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.RENAME_USER;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ROLE_MANAGEMENT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SERVER_MANAGEMENT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SET_AUTH;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SET_DATABASE_ACCESS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SET_LABEL;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SET_PASSWORDS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SET_PROPERTY;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SET_USER_HOME_DATABASE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SET_USER_STATUS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SHOW_ALIAS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SHOW_CONNECTION;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SHOW_CONSTRAINT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SHOW_INDEX;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SHOW_PRIVILEGE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SHOW_ROLE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SHOW_SERVER;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SHOW_TRANSACTION;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SHOW_USER;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.START_DATABASE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.STOP_DATABASE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TERMINATE_CONNECTION;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TERMINATE_TRANSACTION;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TOKEN;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TRANSACTION_MANAGEMENT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TRAVERSE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.USER_MANAGEMENT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.WRITE;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class PrivilegeActionTest {
    private static final Map<PrivilegeAction, Set<PrivilegeAction>> expected = new HashMap<>();

    static {
        expected.put(ADMIN, Set.of(DBMS_ACTIONS, TRANSACTION_MANAGEMENT, START_DATABASE, STOP_DATABASE));
        expected.put(
                TRANSACTION_MANAGEMENT,
                Set.of(SHOW_TRANSACTION, TERMINATE_TRANSACTION, SHOW_CONNECTION, TERMINATE_CONNECTION));
        expected.put(ROLE_MANAGEMENT, Set.of(SHOW_ROLE, CREATE_ROLE, RENAME_ROLE, DROP_ROLE, ASSIGN_ROLE, REMOVE_ROLE));
        expected.put(USER_MANAGEMENT, Set.of(SHOW_USER, CREATE_USER, RENAME_USER, DROP_USER, ALTER_USER));
        expected.put(ALTER_USER, Set.of(SET_USER_STATUS, SET_PASSWORDS, SET_AUTH, SET_USER_HOME_DATABASE));
        expected.put(SET_AUTH, Set.of(SET_PASSWORDS));
        expected.put(
                DATABASE_MANAGEMENT,
                Set.of(CREATE_DATABASE, DROP_DATABASE, ALTER_DATABASE, COMPOSITE_DATABASE_MANAGEMENT));
        expected.put(COMPOSITE_DATABASE_MANAGEMENT, Set.of(CREATE_COMPOSITE_DATABASE, DROP_COMPOSITE_DATABASE));
        expected.put(ALTER_DATABASE, Set.of(SET_DATABASE_ACCESS));
        expected.put(ALIAS_MANAGEMENT, Set.of(CREATE_ALIAS, DROP_ALIAS, ALTER_ALIAS, SHOW_ALIAS));
        expected.put(PRIVILEGE_MANAGEMENT, Set.of(SHOW_PRIVILEGE, ASSIGN_PRIVILEGE, REMOVE_PRIVILEGE));
        expected.put(WRITE, Set.of(SET_LABEL, REMOVE_LABEL, CREATE_ELEMENT, DELETE_ELEMENT, SET_PROPERTY));
        expected.put(GRAPH_ACTIONS, Set.of(TRAVERSE, READ, WRITE, MATCH));
        expected.put(MERGE, Set.of(MATCH, TRAVERSE, READ, CREATE_ELEMENT, SET_PROPERTY));
        expected.put(MATCH, Set.of(TRAVERSE, READ));
        expected.put(INDEX, Set.of(CREATE_INDEX, DROP_INDEX, SHOW_INDEX));
        expected.put(CONSTRAINT, Set.of(CREATE_CONSTRAINT, DROP_CONSTRAINT, SHOW_CONSTRAINT));
        expected.put(TOKEN, Set.of(CREATE_LABEL, CREATE_RELTYPE, CREATE_PROPERTYKEY));
        expected.put(DATABASE_ACTIONS, Set.of(INDEX, CONSTRAINT, TOKEN, ACCESS));
        expected.put(
                DBMS_ACTIONS,
                Set.of(
                        ROLE_MANAGEMENT,
                        USER_MANAGEMENT,
                        DATABASE_MANAGEMENT,
                        ALIAS_MANAGEMENT,
                        PRIVILEGE_MANAGEMENT,
                        EXECUTE_ADMIN,
                        IMPERSONATE,
                        SERVER_MANAGEMENT));
        expected.put(SERVER_MANAGEMENT, Set.of(SHOW_SERVER));
        expected.put(LOAD, Set.of(LOAD_CIDR, LOAD_URL));
    }

    @Test
    void shouldSatisfySelf() {
        for (var action : PrivilegeAction.values()) {
            assertTrue(action.satisfies(action));
        }
    }

    @Test
    void shouldSatisfyAtAllLevels() {
        for (var groupAction : expected.keySet()) {
            assertGroupSatisfies(groupAction, expected.get(groupAction));
        }
    }

    static void assertGroupSatisfies(PrivilegeAction group, Set<PrivilegeAction> actions) {
        for (var action : actions) {
            if (expected.containsKey(action)) {
                assertGroupSatisfies(group, expected.get(action));
            }
            assertTrue(group.satisfies(action), String.format("%s should satisfy %s", group, action));
        }
    }

    @Test
    void shouldNotSatisfy() {
        for (var action : PrivilegeAction.values()) {
            for (var notSatisfied : notChild(action)) {
                assertFalse(
                        action.satisfies(notSatisfied),
                        String.format("%s should not satisfy %s", action, notSatisfied));
            }
        }
    }

    private static Set<PrivilegeAction> notChild(PrivilegeAction action) {
        var notChildren = new HashSet<>(Arrays.asList(PrivilegeAction.values()));
        removeChildren(action, notChildren);
        return notChildren;
    }

    private static void removeChildren(PrivilegeAction action, Set<PrivilegeAction> notChildren) {
        notChildren.remove(action);
        if (expected.containsKey(action)) {
            for (var child : expected.get(action)) {
                removeChildren(child, notChildren);
            }
        }
    }
}
