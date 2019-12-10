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

public enum PrivilegeAction
{
    // Database actions
    /** ACCESS database */
    ACCESS,

    /** MATCH element and read labels */
    TRAVERSE,

    /** Read properties of element */
    READ,

    /** Create, update and delete elements and properties */
    WRITE,

    /** Execute procedure/view with elevated access */
    EXECUTE,

    CREATE_LABEL,
    CREATE_RELTYPE,
    CREATE_PROPERTYKEY,

    CREATE_INDEX,
    DROP_INDEX,
    CREATE_CONSTRAINT,
    DROP_CONSTRAINT,

    START_DATABASE,
    STOP_DATABASE,

    SHOW_TRANSACTION,
    KILL_TRANSACTION,
    SHOW_CONNECTION,
    KILL_CONNECTION,

    // DBMS actions
    CREATE_DATABASE,
    DROP_DATABASE,

    SHOW_USER,
    CREATE_USER,
    ALTER_USER,
    DROP_USER,

    SHOW_ROLE,
    CREATE_ROLE,
    DROP_ROLE,
    ASSIGN_ROLE,
    REMOVE_ROLE,

    SHOW_PRIVILEGE,
    GRANT_PRIVILEGE,
    DENY_PRIVILEGE,
    REVOKE_PRIVILEGE,

    // Some grouping actions that represent super-sets of other actions

    ADMIN
            {
                @Override
                public boolean satisfies( PrivilegeAction action )
                {
                    return USER_MANAGEMENT.satisfies( action ) ||
                           ROLE_MANAGEMENT.satisfies( action ) ||
                           PRIVILEGE_MANAGEMENT.satisfies( action ) ||
                           TRANSACTION_MANAGEMENT.satisfies( action ) ||
                           DATABASE_MANAGEMENT.satisfies( action ) ||
                           this == action;
                }
            },

    TOKEN
            {
                @Override
                public boolean satisfies( PrivilegeAction action )
                {
                    switch ( action )
                    {
                    case CREATE_LABEL:
                    case CREATE_RELTYPE:
                    case CREATE_PROPERTYKEY:
                        return true;
                    default:
                        return this == action;
                    }
                }
            },

    SCHEMA
            {
                @Override
                public boolean satisfies( PrivilegeAction action )
                {
                    return INDEX.satisfies( action ) || CONSTRAINT.satisfies( action ) || this == action;
                }
            },

    CONSTRAINT
            {
                public boolean satisfies( PrivilegeAction action )
                {
                    switch ( action )
                    {
                    case CREATE_CONSTRAINT:
                    case DROP_CONSTRAINT:
                        return true;
                    default:
                        return this == action;
                    }
                }
            },

    INDEX
            {
                public boolean satisfies( PrivilegeAction action )
                {
                    switch ( action )
                    {
                    case CREATE_INDEX:
                    case DROP_INDEX:
                        return true;
                    default:
                        return this == action;
                    }
                }
            },

    DATABASE_MANAGEMENT
            {
                @Override
                public boolean satisfies( PrivilegeAction action )
                {
                    switch ( action )
                    {
                    case CREATE_DATABASE:
                    case DROP_DATABASE:
                    case START_DATABASE:
                    case STOP_DATABASE:
                        return true;
                    default:
                        return this == action;
                    }
                }
            },

    TRANSACTION_MANAGEMENT
            {
                @Override
                public boolean satisfies( PrivilegeAction action )
                {
                    switch ( action )
                    {
                    case SHOW_TRANSACTION:
                    case KILL_TRANSACTION:
                    case SHOW_CONNECTION:
                    case KILL_CONNECTION:
                        return true;
                    default:
                        return this == action;
                    }
                }
            },

    USER_MANAGEMENT
            {
                @Override
                public boolean satisfies( PrivilegeAction action )
                {
                    switch ( action )
                    {
                    case SHOW_USER:
                    case CREATE_USER:
                    case ALTER_USER:
                    case DROP_USER:
                        return true;
                    default:
                        return this == action;
                    }
                }
            },

    ROLE_MANAGEMENT
            {
                @Override
                public boolean satisfies( PrivilegeAction action )
                {
                    switch ( action )
                    {
                    case SHOW_ROLE:
                    case CREATE_ROLE:
                    case DROP_ROLE:
                    case ASSIGN_ROLE:
                    case REMOVE_ROLE:
                        return true;
                    default:
                        return this == action;
                    }
                }
            },

    PRIVILEGE_MANAGEMENT
            {
                @Override
                public boolean satisfies( PrivilegeAction action )
                {
                    switch ( action )
                    {
                    case SHOW_PRIVILEGE:
                    case GRANT_PRIVILEGE:
                    case DENY_PRIVILEGE:
                    case REVOKE_PRIVILEGE:
                        return true;
                    default:
                        return this == action;
                    }
                }
            },

    GRAPH_ACTIONS
            {
                @Override
                public boolean satisfies( PrivilegeAction action )
                {
                    switch ( action )
                    {
                    case READ:
                    case WRITE:
                    case TRAVERSE:
                        return true;
                    default:
                        return this == action;
                    }
                }
            },

    DATABASE_ACTIONS
            {
                @Override
                public boolean satisfies( PrivilegeAction action )
                {
                    switch ( action )
                    {
                    case ACCESS:
                    case EXECUTE:
                        return true;
                    default:
                        return SCHEMA.satisfies( action ) ||
                               TOKEN.satisfies( action ) ||
                               GRAPH_ACTIONS.satisfies( action ) ||
                               this == action;
                    }
                }
            };

    /**
     * @return true if this action satifies the specified action. For example any broad-scope action satisfies many other actions, but a narrow scope action
     * satifies only itself.
     */
    public boolean satisfies( PrivilegeAction action )
    {
        return this == action;
    }

    @Override
    public String toString()
    {
        return super.toString().toLowerCase();
    }

    public static PrivilegeAction from( String name )
    {
        try
        {
            return PrivilegeAction.valueOf( name.toUpperCase() );
        }
        catch ( IllegalArgumentException e )
        {
            return null;
        }
    }
}
