/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.util.Locale;

public enum PrivilegeAction
{
    // Database actions
    /** ACCESS database */
    ACCESS,

    /** MATCH element and read labels */
    TRAVERSE,

    /** Read properties of element */
    READ,

    /** Set and remove labels from nodes */
    SET_LABEL,
    REMOVE_LABEL,

    CREATE_ELEMENT,
    DELETE_ELEMENT,

    SET_PROPERTY,

    /** Execute procedure or user defined function */
    EXECUTE,
    /** Execute procedure or user defined function with elevated access */
    EXECUTE_BOOSTED,
    /** Execute @Admin procedure with elevated access */
    EXECUTE_ADMIN,

    CREATE_LABEL,
    CREATE_RELTYPE,
    CREATE_PROPERTYKEY,

    CREATE_INDEX,
    DROP_INDEX,
    SHOW_INDEX,
    CREATE_CONSTRAINT,
    DROP_CONSTRAINT,
    SHOW_CONSTRAINT,

    START_DATABASE,
    STOP_DATABASE,

    SHOW_TRANSACTION,
    TERMINATE_TRANSACTION,
    SHOW_CONNECTION,
    TERMINATE_CONNECTION,

    // DBMS actions
    CREATE_DATABASE,
    DROP_DATABASE,

    SHOW_USER,
    CREATE_USER,
    SET_USER_STATUS,
    SET_PASSWORDS,
    SET_USER_DEFAULT_DATABASE,
    DROP_USER,

    SHOW_ROLE,
    CREATE_ROLE,
    DROP_ROLE,
    ASSIGN_ROLE,
    REMOVE_ROLE,

    SHOW_PRIVILEGE,
    ASSIGN_PRIVILEGE,
    REMOVE_PRIVILEGE,

    // Some grouping actions that represent super-sets of other actions

    ADMIN
            {
                @Override
                public boolean satisfies( PrivilegeAction action )
                {
                    return DBMS_ACTIONS.satisfies( action ) ||
                           TRANSACTION_MANAGEMENT.satisfies( action ) ||
                           START_DATABASE.satisfies( action ) ||
                           STOP_DATABASE.satisfies( action ) ||
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

    CONSTRAINT
            {
                @Override
                public boolean satisfies( PrivilegeAction action )
                {
                    switch ( action )
                    {
                    case CREATE_CONSTRAINT:
                    case DROP_CONSTRAINT:
                    case SHOW_CONSTRAINT:
                        return true;
                    default:
                        return this == action;
                    }
                }
            },

    INDEX
            {
                @Override
                public boolean satisfies( PrivilegeAction action )
                {
                    switch ( action )
                    {
                    case CREATE_INDEX:
                    case DROP_INDEX:
                    case SHOW_INDEX:
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
                    case TERMINATE_TRANSACTION:
                    case SHOW_CONNECTION:
                    case TERMINATE_CONNECTION:
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
                    case DROP_USER:
                        return true;
                    default:
                        return ALTER_USER.satisfies( action ) || this == action;
                    }
                }
            },

    ALTER_USER
            {
                @Override
                public boolean satisfies( PrivilegeAction action )
                {
                    switch ( action )
                    {
                    case SET_USER_STATUS:
                    case SET_PASSWORDS:
                    case SET_USER_DEFAULT_DATABASE:
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
                    case ASSIGN_PRIVILEGE:
                    case REMOVE_PRIVILEGE:
                        return true;
                    default:
                        return this == action;
                    }
                }
            },

    /** MATCH element and read labels and properties */
    MATCH
            {
                @Override
                public boolean satisfies( PrivilegeAction action )
                {
                    switch ( action )
                    {
                    case READ:
                    case TRAVERSE:
                        return true;
                    default:
                        return this == action;
                    }
                }
            },

    /** Create, update and delete elements and properties */
    WRITE
            {
                @Override
                public boolean satisfies( PrivilegeAction action )
                {
                    switch ( action )
                    {
                    case SET_LABEL:
                    case REMOVE_LABEL:
                    case CREATE_ELEMENT:
                    case DELETE_ELEMENT:
                    case SET_PROPERTY:
                        return true;
                    default:
                        return this == action;
                    }
                }
    },

    MERGE
            {
                @Override
                public boolean satisfies( PrivilegeAction action )
                {
                    switch ( action )
                    {
                    case MATCH:
                    case TRAVERSE:
                    case READ:
                    case CREATE_ELEMENT:
                    case SET_PROPERTY:
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
                    case TRAVERSE:
                    case MATCH:
                        return true;
                    default:
                        return WRITE.satisfies( action ) || this == action;
                    }
                }
            },

    DATABASE_ACTIONS
            {
                @Override
                public boolean satisfies( PrivilegeAction action )
                {
                    return action == PrivilegeAction.ACCESS ||
                           INDEX.satisfies( action ) ||
                           CONSTRAINT.satisfies( action ) ||
                           TOKEN.satisfies( action ) ||
                           this == action;
                }
            },

    DBMS_ACTIONS
            {
                @Override
                public boolean satisfies( PrivilegeAction action )
                {
                    return ROLE_MANAGEMENT.satisfies( action ) ||
                           USER_MANAGEMENT.satisfies( action ) ||
                           DATABASE_MANAGEMENT.satisfies( action ) ||
                           PRIVILEGE_MANAGEMENT.satisfies( action ) ||
                           EXECUTE_ADMIN == action ||
                           this == action;
                }
            };

    /**
     * @return true if this action satisfies the specified action. For example any broad-scope action satisfies many other actions, but a narrow scope action
     * satisfies only itself.
     */
    public boolean satisfies( PrivilegeAction action )
    {
        return this == action;
    }

    @Override
    public String toString()
    {
        return super.toString().toLowerCase( Locale.ROOT );
    }

    public static PrivilegeAction from( String name )
    {
        try
        {
            return PrivilegeAction.valueOf( name.toUpperCase( Locale.ROOT ) );
        }
        catch ( IllegalArgumentException e )
        {
            return null;
        }
    }
}
