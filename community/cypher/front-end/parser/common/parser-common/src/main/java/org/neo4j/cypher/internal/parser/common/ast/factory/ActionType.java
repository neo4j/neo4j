/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.parser.common.ast.factory;

public enum ActionType {
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
    USER_AUTH,
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
    DATABASE_COMPOSITE_MANAGEMENT,
    DATABASE_COMPOSITE_CREATE,
    DATABASE_COMPOSITE_DROP,
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
    EXECUTE_FUNCTION,
    EXECUTE_BOOSTED_FUNCTION,
    EXECUTE_PROCEDURE,
    EXECUTE_BOOSTED_PROCEDURE,
    EXECUTE_ADMIN_PROCEDURE,
    SERVER_SHOW,
    SERVER_MANAGEMENT,
    SETTING_SHOW,

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
