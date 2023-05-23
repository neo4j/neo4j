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

public class ParserCypherTypeName {
    private final String typeName;

    public static final ParserCypherTypeName BOOLEAN = new ParserCypherTypeName("BOOLEAN");
    public static final ParserCypherTypeName STRING = new ParserCypherTypeName("STRING");
    public static final ParserCypherTypeName INTEGER = new ParserCypherTypeName("INTEGER");
    public static final ParserCypherTypeName FLOAT = new ParserCypherTypeName("FLOAT");
    public static final ParserCypherTypeName DATE = new ParserCypherTypeName("DATE");
    public static final ParserCypherTypeName LOCAL_TIME = new ParserCypherTypeName("LOCAL TIME");
    public static final ParserCypherTypeName ZONED_TIME = new ParserCypherTypeName("ZONED TIME");
    public static final ParserCypherTypeName LOCAL_DATETIME = new ParserCypherTypeName("LOCAL DATETIME");
    public static final ParserCypherTypeName ZONED_DATETIME = new ParserCypherTypeName("ZONED DATETIME");
    public static final ParserCypherTypeName DURATION = new ParserCypherTypeName("DURATION");
    public static final ParserCypherTypeName POINT = new ParserCypherTypeName("POINT");

    ParserCypherTypeName(String typeName) {
        this.typeName = typeName;
    }

    public String description() {
        return typeName;
    }
}
