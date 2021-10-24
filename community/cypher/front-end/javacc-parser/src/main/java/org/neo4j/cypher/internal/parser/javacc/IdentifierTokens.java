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
package org.neo4j.cypher.internal.parser.javacc;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ACCESS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ACTIVE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ALIAS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ALL;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ALL_SHORTEST_PATH;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ALTER;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.AND;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ANY;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.AS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ASC;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ASSERT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ASSIGN;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.BRIEF;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.BTREE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.BUILT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.BY;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.CALL;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.CASE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.CATALOG;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.CHANGE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.COMMIT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.CONSTRAINT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.CONSTRAINTS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.CONTAINS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.COPY;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.COUNT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.CREATE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.CSV;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.CURRENT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DATA;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DATABASE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DATABASES;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DBMS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DEFAULT_TOKEN;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DEFINED;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DELETE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DENY;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DESC;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DESTROY;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DETACH;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DISTINCT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DROP;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DUMP;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.EACH;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ELEMENT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ELEMENTS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ELSE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ENCRYPTED;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.END;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ENDS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ESCAPED_SYMBOLIC_NAME;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.EXECUTABLE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.EXIST;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.EXISTENCE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.EXISTS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.EXTRACT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.FALSE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.FIELDTERMINATOR;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.FILTER;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.FOR;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.FOREACH;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.FROM;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.FULLTEXT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.FUNCTION;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.FUNCTIONS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.GRANT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.GRAPH;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.GRAPHS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.HEADERS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.HOME;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.IDENTIFIER;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.IF;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.IMPERSONATE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.IN;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.INDEX;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.INDEXES;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.IS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.JOIN;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.KEY;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.LABEL;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.LABELS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.LIMITROWS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.LOAD;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.LOOKUP;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.MANAGEMENT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.MATCH;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.MERGE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.NAME;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.NAMES;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.NEW;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.NODE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.NODES;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.NONE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.NOT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.NOWAIT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.NULL;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.OF;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ON;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ONLY;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.OPTIONAL;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.OPTIONS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.OR;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ORDER;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.OUTPUT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.PASSWORD;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.PASSWORDS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.PERIODIC;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.PLAINTEXT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.POINT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.POPULATED;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.PRIVILEGE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.PRIVILEGES;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.PROCEDURE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.PROCEDURES;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.PROPERTY;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.RANGE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.READ;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.REDUCE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.REL;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.RELATIONSHIP;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.RELATIONSHIPS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.REMOVE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.RENAME;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.REPLACE;
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
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.SECONDS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.SEEK;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.SET;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.SHORTEST_PATH;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.SHOW;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.SINGLE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.SKIPROWS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.START;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.STARTS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.STATUS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.STOP;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.SUSPENDED;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.TARGET;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.TERMINATE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.TEXT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.THEN;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.TO;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.TRANSACTION;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.TRANSACTIONS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.TRAVERSE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.TRUE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.TYPE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.TYPES;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.UNION;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.UNIQUE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.UNWIND;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.USE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.USER;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.USERS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.USING;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.VERBOSE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.WAIT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.WHEN;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.WHERE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.WITH;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.WRITE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.XOR;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.YIELD;


public class IdentifierTokens
{
    private static final Set<Integer> identifiers = new HashSet<>( Arrays.asList(
            ESCAPED_SYMBOLIC_NAME,
            //keywords
            ACCESS,
            ACTIVE,
            ALIAS,
            ALL_SHORTEST_PATH,
            ALL,
            ALTER,
            AND,
            ANY,
            AS,
            ASC,
            ASSERT,
            ASSIGN,
            BRIEF,
            BTREE,
            BUILT,
            BY,
            CALL,
            CASE,
            CATALOG,
            CHANGE,
            COMMIT,
            CONSTRAINT,
            CONSTRAINTS,
            CONTAINS,
            COPY,
            COUNT,
            CREATE,
            CSV,
            CURRENT,
            DATA,
            DATABASE,
            DATABASES,
            DBMS,
            DEFAULT_TOKEN,
            DEFINED,
            DELETE,
            DENY,
            DESC,
            DESTROY,
            DETACH,
            DISTINCT,
            DROP,
            DUMP,
            EACH,
            ELEMENT,
            ELEMENTS,
            ELSE,
            ENCRYPTED,
            END,
            ENDS,
            EXECUTABLE,
            EXIST,
            EXISTENCE,
            EXISTS,
            EXTRACT,
            FALSE,
            FIELDTERMINATOR,
            FILTER,
            FOR,
            FOREACH,
            FROM,
            FULLTEXT,
            FUNCTION,
            FUNCTIONS,
            GRANT,
            GRAPH,
            GRAPHS,
            HEADERS,
            HOME,
            IDENTIFIER,
            IF,
            IMPERSONATE,
            IN,
            INDEX,
            INDEXES,
            IS,
            JOIN,
            KEY,
            LABEL,
            LABELS,
            LIMITROWS,
            LOAD,
            LOOKUP,
            MANAGEMENT,
            MATCH,
            MERGE,
            NAME,
            NAMES,
            NEW,
            NODE,
            NODES,
            NONE,
            NOT,
            NOWAIT,
            NULL,
            OF,
            ON,
            ONLY,
            OPTIONS,
            OPTIONAL,
            OR,
            ORDER,
            OUTPUT,
            PASSWORD,
            PASSWORDS,
            PERIODIC,
            PLAINTEXT,
            POINT,
            POPULATED,
            PRIVILEGE,
            PRIVILEGES,
            PROCEDURE,
            PROCEDURES,
            PROPERTY,
            RANGE,
            READ,
            REDUCE,
            REL,
            RELATIONSHIP,
            RELATIONSHIPS,
            REMOVE,
            RENAME,
            REPLACE,
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
            SECONDS,
            SEEK,
            SET,
            SHORTEST_PATH,
            SHOW,
            SINGLE,
            SKIPROWS,
            START,
            STARTS,
            STATUS,
            STOP,
            SUSPENDED,
            TARGET,
            TERMINATE,
            TEXT,
            THEN,
            TO,
            TRANSACTION,
            TRANSACTIONS,
            TRAVERSE,
            TRUE,
            TYPE,
            TYPES,
            UNION,
            UNIQUE,
            UNWIND,
            USE,
            USER,
            USERS,
            USING,
            VERBOSE,
            WAIT,
            WHEN,
            WHERE,
            WITH,
            WRITE,
            XOR,
            YIELD
    ) );

    public static Set<Integer> getIdentifierTokens()
    {
        return identifiers;
    }
}
