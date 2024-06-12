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

import java.util.Locale;
import java.util.Objects;

public enum PrivilegeAction {
    // Database actions
    /**
     * ACCESS database
     */
    ACCESS,

    /**
     * MATCH element and read labels
     */
    TRAVERSE,

    /**
     * Read properties of element
     */
    READ,

    /**
     * Set and remove labels from nodes
     */
    SET_LABEL,
    REMOVE_LABEL,

    CREATE_ELEMENT,
    DELETE_ELEMENT,

    SET_PROPERTY,

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
    SET_DATABASE_ACCESS,
    CREATE_COMPOSITE_DATABASE,
    DROP_COMPOSITE_DATABASE,

    CREATE_ALIAS,
    DROP_ALIAS,
    ALTER_ALIAS,
    SHOW_ALIAS,

    SHOW_USER,
    CREATE_USER,
    RENAME_USER,
    SET_USER_STATUS,
    SET_PASSWORDS,
    SET_USER_HOME_DATABASE,
    DROP_USER,

    IMPERSONATE,

    SHOW_ROLE,
    CREATE_ROLE,
    RENAME_ROLE,
    DROP_ROLE,
    ASSIGN_ROLE,
    REMOVE_ROLE,

    SHOW_PRIVILEGE,
    ASSIGN_PRIVILEGE,
    REMOVE_PRIVILEGE,

    ASSIGN_IMMUTABLE_PRIVILEGE,
    REMOVE_IMMUTABLE_PRIVILEGE,

    /**
     * Execute procedure or user defined function
     */
    EXECUTE,
    /**
     * Execute procedure or user defined function with elevated access
     */
    EXECUTE_BOOSTED,
    /**
     * Execute @Admin procedure with elevated access
     */
    EXECUTE_ADMIN,

    SHOW_SERVER,

    SHOW_SETTING,

    /**
     * Load data using LOAD CSV
     */
    LOAD_CIDR,
    LOAD_URL,

    // Some grouping actions that represent super-sets of other actions

    ADMIN {
        @Override
        public boolean satisfies(PrivilegeAction action) {
            return DBMS_ACTIONS.satisfies(action)
                    || TRANSACTION_MANAGEMENT.satisfies(action)
                    || START_DATABASE.satisfies(action)
                    || STOP_DATABASE.satisfies(action)
                    || this == action;
        }
    },

    TOKEN {
        @Override
        public boolean satisfies(PrivilegeAction action) {
            return switch (action) {
                case CREATE_LABEL, CREATE_RELTYPE, CREATE_PROPERTYKEY -> true;
                default -> this == action;
            };
        }
    },

    CONSTRAINT {
        @Override
        public boolean satisfies(PrivilegeAction action) {
            return switch (action) {
                case CREATE_CONSTRAINT, DROP_CONSTRAINT, SHOW_CONSTRAINT -> true;
                default -> this == action;
            };
        }
    },

    INDEX {
        @Override
        public boolean satisfies(PrivilegeAction action) {
            return switch (action) {
                case CREATE_INDEX, DROP_INDEX, SHOW_INDEX -> true;
                default -> this == action;
            };
        }
    },

    COMPOSITE_DATABASE_MANAGEMENT {
        @Override
        public boolean satisfies(PrivilegeAction action) {
            return switch (action) {
                case CREATE_COMPOSITE_DATABASE, DROP_COMPOSITE_DATABASE -> true;
                default -> this == action;
            };
        }
    },

    DATABASE_MANAGEMENT {
        @Override
        public boolean satisfies(PrivilegeAction action) {
            return switch (action) {
                case CREATE_DATABASE, DROP_DATABASE -> true;
                default -> ALTER_DATABASE.satisfies(action)
                        || COMPOSITE_DATABASE_MANAGEMENT.satisfies(action)
                        || this == action;
            };
        }
    },

    ALTER_DATABASE {
        @Override
        public boolean satisfies(PrivilegeAction action) {
            if (Objects.requireNonNull(action) == PrivilegeAction.SET_DATABASE_ACCESS) {
                return true;
            }
            return this == action;
        }
    },

    ALIAS_MANAGEMENT {
        @Override
        public boolean satisfies(PrivilegeAction action) {
            return switch (action) {
                case CREATE_ALIAS, DROP_ALIAS, ALTER_ALIAS, SHOW_ALIAS -> true;
                default -> this == action;
            };
        }
    },

    TRANSACTION_MANAGEMENT {
        @Override
        public boolean satisfies(PrivilegeAction action) {
            return switch (action) {
                case SHOW_TRANSACTION, TERMINATE_TRANSACTION, SHOW_CONNECTION, TERMINATE_CONNECTION -> true;
                default -> this == action;
            };
        }
    },

    USER_MANAGEMENT {
        @Override
        public boolean satisfies(PrivilegeAction action) {
            return switch (action) {
                case SHOW_USER, CREATE_USER, RENAME_USER, DROP_USER -> true;
                default -> ALTER_USER.satisfies(action) || this == action;
            };
        }
    },

    ALTER_USER {
        @Override
        public boolean satisfies(PrivilegeAction action) {
            return switch (action) {
                case SET_USER_STATUS, SET_PASSWORDS, SET_AUTH, SET_USER_HOME_DATABASE -> true;
                default -> this == action;
            };
        }
    },

    SET_AUTH {
        @Override
        public boolean satisfies(PrivilegeAction action) {
            return switch (action) {
                case SET_PASSWORDS -> true;
                default -> this == action;
            };
        }
    },

    ROLE_MANAGEMENT {
        @Override
        public boolean satisfies(PrivilegeAction action) {
            return switch (action) {
                case SHOW_ROLE, CREATE_ROLE, RENAME_ROLE, DROP_ROLE, ASSIGN_ROLE, REMOVE_ROLE -> true;
                default -> this == action;
            };
        }
    },

    PRIVILEGE_MANAGEMENT {
        @Override
        public boolean satisfies(PrivilegeAction action) {
            return switch (action) {
                case SHOW_PRIVILEGE, ASSIGN_PRIVILEGE, REMOVE_PRIVILEGE -> true;
                default -> this == action;
            };
        }
    },

    /**
     * MATCH element and read labels and properties
     */
    MATCH {
        @Override
        public boolean satisfies(PrivilegeAction action) {
            return switch (action) {
                case READ, TRAVERSE -> true;
                default -> this == action;
            };
        }
    },

    /**
     * Create, update and delete elements and properties
     */
    WRITE {
        @Override
        public boolean satisfies(PrivilegeAction action) {
            return switch (action) {
                case SET_LABEL, REMOVE_LABEL, CREATE_ELEMENT, DELETE_ELEMENT, SET_PROPERTY -> true;
                default -> this == action;
            };
        }
    },

    MERGE {
        @Override
        public boolean satisfies(PrivilegeAction action) {
            return switch (action) {
                case MATCH, TRAVERSE, READ, CREATE_ELEMENT, SET_PROPERTY -> true;
                default -> this == action;
            };
        }
    },

    GRAPH_ACTIONS {
        @Override
        public boolean satisfies(PrivilegeAction action) {
            return switch (action) {
                case READ, TRAVERSE, MATCH -> true;
                default -> WRITE.satisfies(action) || this == action;
            };
        }
    },

    DATABASE_ACTIONS {
        @Override
        public boolean satisfies(PrivilegeAction action) {
            return action == PrivilegeAction.ACCESS
                    || INDEX.satisfies(action)
                    || CONSTRAINT.satisfies(action)
                    || TOKEN.satisfies(action)
                    || this == action;
        }
    },

    DBMS_ACTIONS {
        @Override
        public boolean satisfies(PrivilegeAction action) {
            return ROLE_MANAGEMENT.satisfies(action)
                    || USER_MANAGEMENT.satisfies(action)
                    || DATABASE_MANAGEMENT.satisfies(action)
                    || ALIAS_MANAGEMENT.satisfies(action)
                    || PRIVILEGE_MANAGEMENT.satisfies(action)
                    || EXECUTE_ADMIN == action
                    || IMPERSONATE == action
                    || SERVER_MANAGEMENT.satisfies(action)
                    || this == action;
        }
    },

    /**
     * Privileges for enabling/deallocating/dropping/showing servers in a cluster
     */
    SERVER_MANAGEMENT {
        @Override
        public boolean satisfies(PrivilegeAction action) {
            return SHOW_SERVER.satisfies(action) || this == action;
        }
    },

    /**
     * Load data using LOAD CSV
     */
    LOAD {
        @Override
        public boolean satisfies(PrivilegeAction action) {
            return switch (action) {
                case LOAD_CIDR, LOAD_URL -> true;
                default -> this == action;
            };
        }
    };

    /**
     * @return true if this action satisfies the specified action.
     * For example any broad-scope action satisfies many other actions, but a narrow scope action satisfies only itself.
     */
    public boolean satisfies(PrivilegeAction action) {
        return this == action;
    }

    @Override
    public String toString() {
        return super.toString().toLowerCase(Locale.ROOT);
    }

    public static PrivilegeAction from(String name) {
        try {
            return PrivilegeAction.valueOf(name.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
