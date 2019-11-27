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
package org.neo4j.internal.kernel.api.security;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ACCESS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ADMIN;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ALTER_USER;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CONSTRAINT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CREATE_CONSTRAINT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CREATE_DATABASE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CREATE_INDEX;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CREATE_LABEL;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CREATE_PROPERTYKEY;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CREATE_RELTYPE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CREATE_ROLE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CREATE_USER;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DATABASE_ACTIONS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DATABASE_MANAGEMENT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DENY_PRIVILEGE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DROP_CONSTRAINT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DROP_DATABASE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DROP_INDEX;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DROP_ROLE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DROP_USER;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.EXECUTE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.GRANT_PRIVILEGE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ASSIGN_ROLE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.GRAPH_ACTIONS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.INDEX;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.KILL_CONNECTION;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.KILL_TRANSACTION;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.PRIVILEGE_MANAGEMENT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.READ;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.REVOKE_PRIVILEGE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.REMOVE_ROLE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ROLE_MANAGEMENT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SCHEMA;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SHOW_CONNECTION;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SHOW_PRIVILEGE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SHOW_ROLE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SHOW_TRANSACTION;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SHOW_USER;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.START_DATABASE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.STOP_DATABASE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TOKEN;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TRANSACTION_MANAGEMENT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TRAVERSE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.USER_MANAGEMENT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.WRITE;

class PrivilegeActionTest
{
    private static Map<PrivilegeAction,Set<PrivilegeAction>> expected = new HashMap<>();

    static
    {
        expected.put( ADMIN, Set.of( DATABASE_MANAGEMENT, TRANSACTION_MANAGEMENT, ROLE_MANAGEMENT, USER_MANAGEMENT, PRIVILEGE_MANAGEMENT ) );
        expected.put( DATABASE_MANAGEMENT, Set.of( START_DATABASE, STOP_DATABASE, CREATE_DATABASE, DROP_DATABASE ) );
        expected.put( TRANSACTION_MANAGEMENT, Set.of( SHOW_TRANSACTION, KILL_TRANSACTION, SHOW_CONNECTION, KILL_CONNECTION ) );
        expected.put( ROLE_MANAGEMENT, Set.of( SHOW_ROLE, CREATE_ROLE, DROP_ROLE, ASSIGN_ROLE, REMOVE_ROLE ) );
        expected.put( USER_MANAGEMENT, Set.of( SHOW_USER, CREATE_USER, DROP_USER, ALTER_USER ) );
        expected.put( PRIVILEGE_MANAGEMENT, Set.of( SHOW_PRIVILEGE, GRANT_PRIVILEGE, DENY_PRIVILEGE, REVOKE_PRIVILEGE ) );

        expected.put( DATABASE_ACTIONS, Set.of( GRAPH_ACTIONS, SCHEMA, TOKEN, ACCESS, EXECUTE ) );
        expected.put( GRAPH_ACTIONS, Set.of( TRAVERSE, READ, WRITE ) );
        expected.put( INDEX, Set.of( CREATE_INDEX, DROP_INDEX ) );
        expected.put( CONSTRAINT, Set.of( CREATE_CONSTRAINT, DROP_CONSTRAINT ) );
        expected.put( SCHEMA, Set.of( INDEX, CREATE_INDEX, DROP_INDEX, CONSTRAINT, CREATE_CONSTRAINT, DROP_CONSTRAINT ) );
        expected.put( TOKEN, Set.of( CREATE_LABEL, CREATE_RELTYPE, CREATE_PROPERTYKEY ) );
    }

    @Test
    void shouldSatisfySelf()
    {
        for ( var action : PrivilegeAction.values() )
        {
            assertTrue( action.satisfies( action ) );
        }
    }

    @Test
    void shouldSatisfyAtAllLevels()
    {
        for ( var groupAction : expected.keySet() )
        {
            assertGroupSatisfies( groupAction, expected.get( groupAction ) );
        }
    }

    void assertGroupSatisfies( PrivilegeAction group, Set<PrivilegeAction> actions )
    {
        for ( var action : actions )
        {
            if ( expected.containsKey( action ) )
            {
                assertGroupSatisfies( group, expected.get( action ) );
            }
            assertTrue( group.satisfies( action ) );
        }
    }

    @Test
    void shouldNotSatisfy()
    {
        for ( var action : PrivilegeAction.values() )
        {
            for ( var notSatisfied : notChild( action ) )
            {
                assertFalse( action.satisfies( notSatisfied ), String.format( "%s should not satisfy %s", action, notSatisfied ) );
            }
        }
    }

    private Set<PrivilegeAction> notChild( PrivilegeAction action )
    {
        var notChildren = new HashSet<>( Arrays.asList( PrivilegeAction.values() ) );
        removeChildren( action, notChildren );
        return notChildren;
    }

    private void removeChildren( PrivilegeAction action, HashSet<PrivilegeAction> notChildren )
    {
        notChildren.remove( action );
        if ( expected.containsKey( action ) )
        {
            for ( var child : expected.get( action ) )
            {
                removeChildren( child, notChildren );
            }
        }
    }
}
