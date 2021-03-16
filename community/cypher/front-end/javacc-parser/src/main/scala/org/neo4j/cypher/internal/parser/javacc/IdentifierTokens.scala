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
package org.neo4j.cypher.internal.parser.javacc

import org.neo4j.cypher.internal.parser.javacc.CypherConstants.ACTIVE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.ALL
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.ALL_SHORTEST_PATH
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.ALTER
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.AND
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.ANY
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.AS
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.ASC
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.ASSERT
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.BRIEF
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.BTREE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.BY
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.CALL
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.CASE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.CATALOG
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.CHANGE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.COMMIT
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.CONSTRAINT
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.CONTAINS
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.COPY
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.COUNT
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.CREATE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.CSV
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.CURRENT
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.DATA
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.DATABASE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.DATABASES
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.DBMS
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.DEFAULT_TOKEN
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.DELETE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.DESC
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.DESTROY
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.DETACH
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.DISTINCT
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.DROP
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.DUMP
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.ELSE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.ENCRYPTED
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.END
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.ENDS
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.ESCAPED_SYMBOLIC_NAME
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.EXISTS
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.EXTRACT
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.FALSE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.FIELDTERMINATOR
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.FILTER
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.FOREACH
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.FROM
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.GRANT
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.GRAPH
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.HEADERS
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.HOME
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.IF
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.IN
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.INDEX
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.INDEXES
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.IS
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.JOIN
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.KEY
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.LIMITROWS
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.LOAD
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.MATCH
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.MERGE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.NODE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.NONE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.NOT
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.NOWAIT
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.NULL
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.OF
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.ON
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.OPTIONAL
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.OR
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.ORDER
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.OUTPUT
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.PASSWORD
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.PERIODIC
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.PLAINTEXT
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.POPULATED
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.REDUCE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.REMOVE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.RENAME
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.REPLACE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.REQUIRED
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.RETURN
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.REVOKE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.ROLE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.ROLES
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.SCAN
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.SEC
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.SECOND
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.SECONDS
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.SEEK
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.SET
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.SHORTEST_PATH
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.SHOW
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.SINGLE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.SKIPROWS
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.START
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.STARTS
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.STATUS
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.STOP
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.SUSPENDED
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.THEN
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.TO
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.TRUE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.UNION
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.UNIQUE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.UNWIND
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.USE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.USER
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.USERS
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.USING
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.VERBOSE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.WAIT
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.WHEN
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.WHERE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.WITH
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.XOR
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.YIELD

object IdentifierTokens {

  val tokens = Set(
    ESCAPED_SYMBOLIC_NAME,
    //keywords
    ACTIVE,
    ALL_SHORTEST_PATH,
    ALL,
    ALTER,
    AND,
    ANY,
    AS,
    ASC,
    ASSERT,
    BRIEF,
    BTREE,
    BY,
    CALL,
    CASE,
    CATALOG,
    CHANGE,
    COMMIT,
    CONSTRAINT,
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
    DELETE,
    DESC,
    DESTROY,
    DETACH,
    DISTINCT,
    DROP,
    DUMP,
    ELSE,
    ENCRYPTED,
    END,
    ENDS,
    EXISTS,
    EXTRACT,
    FALSE,
    FIELDTERMINATOR,
    FILTER,
    FOREACH,
    FROM,
    GRANT,
    GRAPH,
    HEADERS,
    HOME,
    IF,
    IN,
    INDEX,
    INDEXES,
    IS,
    JOIN,
    KEY,
    LIMITROWS,
    LOAD,
    MATCH,
    MERGE,
    NODE,
    NONE,
    NOT,
    NOWAIT,
    NULL,
    OF,
    ON,
    OPTIONAL,
    OR,
    ORDER,
    OUTPUT,
    PASSWORD,
    PERIODIC,
    PLAINTEXT,
    POPULATED,
    REDUCE,
    REMOVE,
    RENAME,
    REPLACE,
    REQUIRED,
    RETURN,
    REVOKE,
    ROLE,
    ROLES,
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
    THEN,
    TO,
    TRUE,
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
    XOR,
    YIELD
  )
}
