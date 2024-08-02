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
package org.neo4j.cypher.internal.parser.javacc;

import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ACCESS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ACTIVE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ADMIN;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ADMINISTRATOR;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ALIAS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ALIASES;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ALL;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ALL_SHORTEST_PATH;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ALTER;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.AND;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ANY;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ARRAY;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.AS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ASC;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ASCENDING;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ASSERT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ASSIGN;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.AT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.AUTH;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.BINDINGS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.BOOL;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.BOOLEAN;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.BOOSTED;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.BOTH;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.BREAK;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.BRIEF;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.BTREE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.BUILT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.BY;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.CALL;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.CASE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.CHANGE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.CIDR;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.COLLECT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.COMMAND;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.COMMANDS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.COMMIT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.COMPOSITE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.CONCURRENT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.CONSTRAINT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.CONSTRAINTS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.CONTAINS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.CONTINUE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.COPY;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.COUNT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.CREATE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.CSV;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.CURRENT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DATA;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DATABASE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DATABASES;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DATE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DATETIME;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DBMS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DEALLOCATE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DEFAULT_TOKEN;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DEFINED;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DELETE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DENY;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DESC;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DESCENDING;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DESTROY;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DETACH;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DIFFERENT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DISTINCT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DRIVER;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DROP;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DRYRUN;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DUMP;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DURATION;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.EACH;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.EDGE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ELEMENT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ELEMENTS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ELSE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ENABLE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ENCRYPTED;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.END;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ENDS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ERROR;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ESCAPED_SYMBOLIC_NAME;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.EXECUTABLE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.EXECUTE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.EXIST;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.EXISTENCE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.EXISTS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.FAIL;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.FALSE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.FIELDTERMINATOR;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.FINISH;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.FLOAT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.FOR;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.FOREACH;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.FROM;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.FULLTEXT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.FUNCTION;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.FUNCTIONS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.GRANT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.GRAPH;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.GRAPHS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.GROUP;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.GROUPS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.HEADERS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.HOME;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ID;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.IDENTIFIER;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.IF;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.IMMUTABLE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.IMPERSONATE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.IN;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.INDEX;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.INDEXES;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.INF;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.INFINITY;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.INSERT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.INT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.INTEGER;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.IS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.JOIN;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.KEY;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.LABEL;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.LABELS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.LEADING;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.LIMITROWS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.LIST;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.LOAD;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.LOCAL;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.LOOKUP;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.MANAGEMENT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.MAP;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.MATCH;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.MERGE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.NAME;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.NAMES;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.NAN;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.NEW;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.NFC;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.NFD;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.NFKC;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.NFKD;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.NODE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.NODES;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.NODETACH;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.NONE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.NORMALIZE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.NORMALIZED;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.NOT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.NOTHING;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.NOWAIT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.NULL;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.OF;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ON;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ONLY;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.OPTION;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.OPTIONAL;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.OPTIONS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.OR;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ORDER;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.OUTPUT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.PASSWORD;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.PASSWORDS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.PATH;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.PATHS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.PERIODIC;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.PLAINTEXT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.POINT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.POPULATED;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.PRIMARIES;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.PRIMARY;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.PRIVILEGE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.PRIVILEGES;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.PROCEDURE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.PROCEDURES;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.PROPERTIES;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.PROPERTY;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.PROVIDER;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.PROVIDERS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.RANGE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.READ;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.REALLOCATE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.REDUCE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.REL;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.RELATIONSHIP;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.RELATIONSHIPS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.REMOVE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.RENAME;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.REPEATABLE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.REPLACE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.REPORT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.REQUIRE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.REQUIRED;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.RETURN;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.REVOKE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ROLE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ROLES;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ROW;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ROWS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.SCAN;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.SEC;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.SECOND;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.SECONDARIES;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.SECONDARY;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.SECONDS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.SEEK;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.SERVER;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.SERVERS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.SET;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.SETTING;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.SETTINGS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.SHORTEST;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.SHORTEST_PATH;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.SHOW;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.SIGNED;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.SINGLE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.SKIPROWS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.START;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.STARTS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.STATUS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.STOP;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.STRING;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.SUPPORTED;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.SUSPENDED;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.TARGET;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.TERMINATE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.TEXT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.THEN;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.TIME;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.TIMESTAMP;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.TIMEZONE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.TO;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.TOPOLOGY;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.TRAILING;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.TRANSACTION;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.TRANSACTIONS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.TRAVERSE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.TRIM;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.TRUE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.TYPE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.TYPED;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.TYPES;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.UNION;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.UNIQUE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.UNIQUENESS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.UNWIND;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.URL;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.USE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.USER;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.USERS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.USING;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.VALUE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.VARCHAR;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.VECTOR;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.VERBOSE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.VERTEX;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.WAIT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.WHEN;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.WHERE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.WITH;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.WITHOUT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.WRITE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.XOR;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.YIELD;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ZONE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ZONED;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class IdentifierTokens {
    private static final Set<Integer> identifiers = new HashSet<>(Arrays.asList(
            ESCAPED_SYMBOLIC_NAME,
            // keywords
            ACCESS,
            ACTIVE,
            ADMIN,
            ADMINISTRATOR,
            ALIAS,
            ALIASES,
            ALL_SHORTEST_PATH,
            ALL,
            ALTER,
            AND,
            ANY,
            ARRAY,
            AS,
            ASC,
            ASCENDING,
            ASSERT,
            ASSIGN,
            AT,
            AUTH,
            BINDINGS,
            BOOL,
            BOOLEAN,
            BOOSTED,
            BOTH,
            BREAK,
            BRIEF,
            BTREE,
            BUILT,
            BY,
            CALL,
            CASE,
            CIDR,
            CHANGE,
            COLLECT,
            COMMAND,
            COMMANDS,
            COMMIT,
            COMPOSITE,
            CONCURRENT,
            CONSTRAINT,
            CONSTRAINTS,
            CONTAINS,
            CONTINUE,
            COPY,
            COUNT,
            CREATE,
            CSV,
            CURRENT,
            DATA,
            DATABASE,
            DATABASES,
            DATE,
            DATETIME,
            DBMS,
            DEALLOCATE,
            DEFAULT_TOKEN,
            DEFINED,
            DELETE,
            DENY,
            DESC,
            DESCENDING,
            DESTROY,
            DETACH,
            DIFFERENT,
            DISTINCT,
            DRIVER,
            DROP,
            DRYRUN,
            DUMP,
            DURATION,
            EACH,
            EDGE,
            ELEMENT,
            ELEMENTS,
            ELSE,
            ENABLE,
            ENCRYPTED,
            END,
            ENDS,
            ERROR,
            EXECUTABLE,
            EXECUTE,
            EXIST,
            EXISTENCE,
            EXISTS,
            FAIL,
            FALSE,
            FIELDTERMINATOR,
            FINISH,
            FLOAT,
            FOR,
            FOREACH,
            FROM,
            FULLTEXT,
            FUNCTION,
            FUNCTIONS,
            GRANT,
            GRAPH,
            GRAPHS,
            GROUP,
            GROUPS,
            HEADERS,
            HOME,
            IDENTIFIER,
            ID,
            IF,
            IMPERSONATE,
            IMMUTABLE,
            IN,
            INDEX,
            INDEXES,
            INF,
            INFINITY,
            INSERT,
            INT,
            INTEGER,
            IS,
            JOIN,
            KEY,
            LABEL,
            LABELS,
            LEADING,
            LIMITROWS,
            LIST,
            LOAD,
            LOCAL,
            LOOKUP,
            MANAGEMENT,
            MAP,
            MATCH,
            MERGE,
            NAME,
            NAMES,
            NAN,
            NFC,
            NFD,
            NFKC,
            NFKD,
            NEW,
            NODE,
            NODETACH,
            NODES,
            NONE,
            NORMALIZE,
            NORMALIZED,
            NOT,
            NOTHING,
            NOWAIT,
            NULL,
            OF,
            ON,
            ONLY,
            OPTIONS,
            OPTION,
            OPTIONAL,
            OR,
            ORDER,
            OUTPUT,
            PASSWORD,
            PASSWORDS,
            PATH,
            PATHS,
            PERIODIC,
            PLAINTEXT,
            POINT,
            POPULATED,
            PRIMARY,
            PRIMARIES,
            PRIVILEGE,
            PRIVILEGES,
            PROCEDURE,
            PROCEDURES,
            PROPERTIES,
            PROPERTY,
            PROVIDER,
            PROVIDERS,
            RANGE,
            READ,
            REALLOCATE,
            REDUCE,
            REL,
            RELATIONSHIP,
            RELATIONSHIPS,
            REMOVE,
            RENAME,
            REPEATABLE,
            REPLACE,
            REPORT,
            REQUIRE,
            REQUIRED,
            RETURN,
            REVOKE,
            ROLE,
            ROLES,
            ROW,
            ROWS,
            SCAN,
            SEC,
            SECOND,
            SECONDARY,
            SECONDARIES,
            SECONDS,
            SEEK,
            SERVER,
            SERVERS,
            SET,
            SETTING,
            SETTINGS,
            SHORTEST,
            SHORTEST_PATH,
            SHOW,
            SIGNED,
            SINGLE,
            SKIPROWS,
            START,
            STARTS,
            STATUS,
            STOP,
            STRING,
            SUPPORTED,
            SUSPENDED,
            TARGET,
            TERMINATE,
            TEXT,
            THEN,
            TIME,
            TIMESTAMP,
            TIMEZONE,
            TO,
            TOPOLOGY,
            TRAILING,
            TRANSACTION,
            TRANSACTIONS,
            TRAVERSE,
            TRIM,
            TRUE,
            TYPE,
            TYPED,
            TYPES,
            UNION,
            UNIQUE,
            UNIQUENESS,
            UNWIND,
            URL,
            USE,
            USER,
            USERS,
            USING,
            VARCHAR,
            VALUE,
            VECTOR,
            VERBOSE,
            VERTEX,
            WAIT,
            WITHOUT,
            WHEN,
            WHERE,
            WITH,
            WRITE,
            XOR,
            YIELD,
            ZONE,
            ZONED));

    public static Set<Integer> getIdentifierTokens() {
        return identifiers;
    }
}
