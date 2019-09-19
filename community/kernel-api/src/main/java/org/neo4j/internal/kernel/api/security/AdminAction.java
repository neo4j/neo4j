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

public enum AdminAction
{
    CREATE_LABEL,
    CREATE_RELTYPE,
    CREATE_PROPERTYKEY,

    CREATE_INDEX,
    DROP_INDEX,
    CREATE_CONSTRAINT,
    DROP_CONSTRAINT,

    START_DATABASE,
    STOP_DATABASE,
    CREATE_DATABASE,
    DROP_DATABASE,

    SHOW_TRANSACTION,
    KILL_TRANSACTION,
    SHOW_CONNECTION,
    KILL_CONNECTION,

    SHOW_USER,
    CREATE_USER,
    ALTER_USER,
    DROP_USER,

    SHOW_ROLE,
    CREATE_ROLE,
    DROP_ROLE,
    GRANT_ROLE,
    REVOKE_ROLE,

    SHOW_PRIVILEGE,
    GRANT_PRIVILEGE,
    DENY_PRIVILEGE,
    REVOKE_PRIVILEGE,

    // Some grouping actions that represent super-sets of other actions

    ALL_ADMIN  // TODO: Remove this entry once all admin privileges are property supported as fine grained
            {
                public boolean satisfies( AdminAction action )
                {
                    return true;
                }
            },

    ALL_TOKEN
            {
                public boolean satisfies( AdminAction action )
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

    ALL_SCHEMA
            {
                public boolean satisfies( AdminAction action )
                {
                    switch ( action )
                    {
                    case CREATE_INDEX:
                    case DROP_INDEX:
                    case CREATE_CONSTRAINT:
                    case DROP_CONSTRAINT:
                        return true;
                    default:
                        return this == action;
                    }
                }
            },

    ALL_TRANSACTION
            {
                public boolean satisfies( AdminAction action )
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

    ALL_USER
            {
                public boolean satisfies( AdminAction action )
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

    ALL_ROLE
            {
                public boolean satisfies( AdminAction action )
                {
                    switch ( action )
                    {
                    case SHOW_ROLE:
                    case CREATE_ROLE:
                    case DROP_ROLE:
                    case GRANT_ROLE:
                    case REVOKE_ROLE:
                        return true;
                    default:
                        return this == action;
                    }
                }
            },

    ALL_PRIVILEGE
            {
                public boolean satisfies( AdminAction action )
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
            };

    /**
     * @return true if this action satifies the specified action. For example any broad-scope action satisfies many other actions, but a narrow scope action
     * satifies only itself.
     */
    public boolean satisfies( AdminAction action )
    {
        return this == action;
    }
}
