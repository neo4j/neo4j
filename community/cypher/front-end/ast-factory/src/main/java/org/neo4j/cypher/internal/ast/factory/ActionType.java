/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.ast.factory;

public enum ActionType
{
    // Database actions
    DATABASE_ALL,
    DATABASE_START,
    DATABASE_STOP,
    ACCESS,
    CREATE_TOKEN,
    CREATE_PROPERTYKEY,
    CREATE_LABEL,
    CREATE_RELTYPE,
    INDEX_ALL,
    INDEX_CREATE,
    INDEX_DROP,
    INDEX_SHOW,
    CONSTRAINT_ALL,
    CONSTRAINT_CREATE,
    CONSTRAINT_DROP,
    CONSTRAINT_SHOW,
    TRANSACTION_ALL,
    TRANSACTION_SHOW,
    TRANSACTION_TERMINATE,

    // DBMS actions
    DBMS_ALL,
    USER_ALL,
    USER_SHOW,
    USER_CREATE,
    USER_RENAME,
    USER_ALTER,
    USER_STATUS,
    USER_PASSWORD,
    USER_HOME,
    USER_DROP,
    USER_IMPERSONATE,
    ROLE_ALL,
    ROLE_SHOW,
    ROLE_CREATE,
    ROLE_RENAME,
    ROLE_DROP,
    ROLE_ASSIGN,
    ROLE_REMOVE,
    DATABASE_MANAGEMENT,
    DATABASE_CREATE,
    DATABASE_DROP,
    DATABASE_ALTER,
    SET_DATABASE_ACCESS,
    ALIAS_MANAGEMENT,
    ALIAS_CREATE,
    ALIAS_DROP,
    ALIAS_ALTER,
    ALIAS_SHOW,
    PRIVILEGE_ALL,
    PRIVILEGE_SHOW,
    PRIVILEGE_ASSIGN,
    PRIVILEGE_REMOVE,

    // Graph actions
    GRAPH_ALL,
    GRAPH_WRITE,
    GRAPH_CREATE,
    GRAPH_MERGE,
    GRAPH_DELETE,
    GRAPH_LABEL_SET,
    GRAPH_LABEL_REMOVE,
    GRAPH_PROPERTY_SET,
    GRAPH_MATCH,
    GRAPH_READ,
    GRAPH_TRAVERSE
}
