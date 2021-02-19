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

import org.neo4j.cypher.internal.parser.javacc.CypherConstants.ALL
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.ALL_SHORTEST_PATH
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.AND
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.ANY
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.AS
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.ASC
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.ASSERT
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.BY
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.CALL
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.CASE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.CATALOG
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.COMMIT
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.CONSTRAINT
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.CONTAINS
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.COPY
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.COUNT
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.CREATE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.CSV
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.DELETE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.DESC
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.DETACH
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.DISTINCT
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.DROP
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.ELSE
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
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.IF
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.IN
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.INDEX
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
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.NULL
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.OF
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.ON
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.OPTIONAL
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.OR
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.ORDER
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.PERIODIC
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.POPULATED
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.REDUCE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.REMOVE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.REPLACE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.RETURN
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.REVOKE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.ROLE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.ROLES
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.SCAN
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.SEEK
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.SET
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.SHORTEST_PATH
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.SHOW
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.SINGLE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.SKIPROWS
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.STARTS
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.THEN
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.TO
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.TRUE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.UNION
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.UNIQUE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.UNWIND
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.USE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.USERS
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.USING
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.WHEN
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.WHERE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.WITH
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.XOR
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.YIELD

object IdentifierTokens {

  val tokens = Set(
    ESCAPED_SYMBOLIC_NAME,
    RETURN,
    CREATE,
    DELETE,
    SET,
    REMOVE,
    DETACH,
    MATCH,
    WITH,
    UNWIND,
    USE,
    GRAPH,
    CALL,
    YIELD,
    LOAD,
    CSV,
    PERIODIC,
    COMMIT,
    HEADERS,
    FROM,
    FIELDTERMINATOR,
    FOREACH,
    WHERE,
    DISTINCT,
    MERGE,
    OPTIONAL,
    USING,
    ORDER,
    BY,
    DESC,
    ASC,
    SKIPROWS,
    LIMITROWS,
    UNION,
    DROP,
    INDEX,
    SEEK,
    SCAN,
    JOIN,
    CONSTRAINT,
    ASSERT,
    IS,
    NODE,
    KEY,
    UNIQUE,
    ON,
    AS,
    OR,
    XOR,
    AND,
    NOT,
    STARTS,
    ENDS,
    CONTAINS,
    IN,
    COUNT,
    FILTER,
    EXTRACT,
    REDUCE,
    EXISTS,
    ALL,
    ANY,
    NONE,
    SINGLE,
    CASE,
    ELSE,
    WHEN,
    THEN,
    END,
    SHORTEST_PATH,
    ALL_SHORTEST_PATH,
    NULL,
    TRUE,
    FALSE,
    CATALOG,
    SHOW,
    POPULATED,
    ROLES,
    ROLE,
    USERS,
    REPLACE,
    GRANT,
    REVOKE,
    IF,
    COPY,
    OF,
    TO
  )
}
