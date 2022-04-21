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

public enum ShowCommandFilterTypes {
    // index specific
    BTREE("BTREE"),
    RANGE("RANGE"),
    FULLTEXT("FULLTEXT"),
    TEXT("TEXT"),
    POINT("POINT"),
    LOOKUP("LOOKUP"),
    // constraint specific
    UNIQUE("UNIQUE"),
    NODE_KEY("NODE KEY"),
    OLD_EXISTS("EXISTS"),
    OLD_EXIST("EXIST"),
    EXIST("[PROPERTY] EXIST[ENCE]"),
    NODE_OLD_EXISTS("NODE EXISTS"),
    NODE_OLD_EXIST("NODE EXIST"),
    NODE_EXIST("NODE [PROPERTY] EXIST[ENCE]"),
    RELATIONSHIP_OLD_EXISTS("RELATIONSHIP EXISTS"),
    RELATIONSHIP_OLD_EXIST("RELATIONSHIP EXIST"),
    RELATIONSHIP_EXIST("REL[ATIONSHIP] [PROPERTY] EXIST[ENCE]"),
    // function specific
    BUILT_IN("BUILT IN"),
    USER_DEFINED("USER DEFINED"),
    // general
    ALL("ALL"),
    INVALID("INVALID");

    private final String description;

    ShowCommandFilterTypes(String description) {
        this.description = description;
    }

    public String description() {
        return description;
    }
}
