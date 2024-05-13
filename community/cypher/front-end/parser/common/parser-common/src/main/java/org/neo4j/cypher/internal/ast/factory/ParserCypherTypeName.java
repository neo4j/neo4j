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
package org.neo4j.cypher.internal.ast.factory;

import java.util.List;

public class ParserCypherTypeName {
    private final String typeName;

    private Integer offset;
    private Integer line;
    private Integer column;

    public ParserCypherTypeName withPos(Integer offset, Integer line, Integer column) {
        this.offset = offset;
        this.line = line;
        this.column = column;
        return this;
    }

    public static final ParserCypherTypeName NOTHING = new ParserCypherTypeName("NOTHING");
    public static final ParserCypherTypeName NULL = new ParserCypherTypeName("NULL");
    public static final ParserCypherTypeName BOOLEAN = new ParserCypherTypeName("BOOLEAN");
    public static final ParserCypherTypeName BOOLEAN_NOT_NULL = new ParserCypherTypeName("BOOLEAN NOT NULL");
    public static final ParserCypherTypeName STRING = new ParserCypherTypeName("STRING");
    public static final ParserCypherTypeName STRING_NOT_NULL = new ParserCypherTypeName("STRING NOT NULL");
    public static final ParserCypherTypeName INTEGER = new ParserCypherTypeName("INTEGER");
    public static final ParserCypherTypeName INTEGER_NOT_NULL = new ParserCypherTypeName("INTEGER NOT NULL");
    public static final ParserCypherTypeName FLOAT = new ParserCypherTypeName("FLOAT");
    public static final ParserCypherTypeName FLOAT_NOT_NULL = new ParserCypherTypeName("FLOAT NOT NULL");
    public static final ParserCypherTypeName DATE = new ParserCypherTypeName("DATE");
    public static final ParserCypherTypeName DATE_NOT_NULL = new ParserCypherTypeName("DATE NOT NULL");
    public static final ParserCypherTypeName LOCAL_TIME = new ParserCypherTypeName("LOCAL TIME");
    public static final ParserCypherTypeName LOCAL_TIME_NOT_NULL = new ParserCypherTypeName("LOCAL TIME NOT NULL");
    public static final ParserCypherTypeName ZONED_TIME = new ParserCypherTypeName("ZONED TIME");
    public static final ParserCypherTypeName ZONED_TIME_NOT_NULL = new ParserCypherTypeName("ZONED TIME NOT NULL");
    public static final ParserCypherTypeName LOCAL_DATETIME = new ParserCypherTypeName("LOCAL DATETIME");
    public static final ParserCypherTypeName LOCAL_DATETIME_NOT_NULL =
            new ParserCypherTypeName("LOCAL DATETIME NOT NULL");
    public static final ParserCypherTypeName ZONED_DATETIME = new ParserCypherTypeName("ZONED DATETIME");
    public static final ParserCypherTypeName ZONED_DATETIME_NOT_NULL =
            new ParserCypherTypeName("ZONED DATETIME NOT NULL");
    public static final ParserCypherTypeName DURATION = new ParserCypherTypeName("DURATION");
    public static final ParserCypherTypeName DURATION_NOT_NULL = new ParserCypherTypeName("DURATION NOT NULL");
    public static final ParserCypherTypeName POINT = new ParserCypherTypeName("POINT");
    public static final ParserCypherTypeName POINT_NOT_NULL = new ParserCypherTypeName("POINT NOT NULL");
    public static final ParserCypherTypeName NODE = new ParserCypherTypeName("NODE");
    public static final ParserCypherTypeName NODE_NOT_NULL = new ParserCypherTypeName("NODE NOT NULL");
    public static final ParserCypherTypeName RELATIONSHIP = new ParserCypherTypeName("RELATIONSHIP");
    public static final ParserCypherTypeName RELATIONSHIP_NOT_NULL = new ParserCypherTypeName("RELATIONSHIP NOT NULL");
    public static final ParserCypherTypeName MAP = new ParserCypherTypeName("MAP");
    public static final ParserCypherTypeName MAP_NOT_NULL = new ParserCypherTypeName("MAP NOT NULL");
    public static final ParserCypherTypeName PATH = new ParserCypherTypeName("PATH");
    public static final ParserCypherTypeName PATH_NOT_NULL = new ParserCypherTypeName("PATH NOT NULL");
    public static final ParserCypherTypeName ANY = new ParserCypherTypeName("ANY");
    public static final ParserCypherTypeName ANY_NOT_NULL = new ParserCypherTypeName("ANY NOT NULL");
    public static final ParserCypherTypeName PROPERTY_VALUE = new ParserCypherTypeName("PROPERTY VALUE");
    public static final ParserCypherTypeName PROPERTY_VALUE_NOT_NULL =
            new ParserCypherTypeName("PROPERTY VALUE NOT NULL");

    ParserCypherTypeName(String typeName) {
        this.typeName = typeName;
    }

    public static ParserCypherTypeName listOf(ParserCypherTypeName inner) {
        return new ListParserCypherTypeName(inner, true);
    }

    public static ParserCypherTypeName closedDynamicUnionOf(List<ParserCypherTypeName> unionTypes) {
        return new ClosedDynamicUnionParserCypherTypeName(unionTypes);
    }

    public static ParserCypherTypeName listOfNotNull(ParserCypherTypeName inner) {
        return new ListParserCypherTypeName(inner, false);
    }

    public String description() {
        return typeName;
    }

    public static ParserCypherTypeName getNotNullTypeName(ParserCypherTypeName parserCypherTypeName) {
        if (parserCypherTypeName.equals(NOTHING) || parserCypherTypeName.equals(NULL)) {
            return NOTHING;
        } else if (parserCypherTypeName.equals(BOOLEAN)) {
            return BOOLEAN_NOT_NULL;
        } else if (parserCypherTypeName.equals(STRING)) {
            return STRING_NOT_NULL;
        } else if (parserCypherTypeName.equals(INTEGER)) {
            return INTEGER_NOT_NULL;
        } else if (parserCypherTypeName.equals(FLOAT)) {
            return FLOAT_NOT_NULL;
        } else if (parserCypherTypeName.equals(DATE)) {
            return DATE_NOT_NULL;
        } else if (parserCypherTypeName.equals(LOCAL_TIME)) {
            return LOCAL_TIME_NOT_NULL;
        } else if (parserCypherTypeName.equals(ZONED_TIME)) {
            return ZONED_TIME_NOT_NULL;
        } else if (parserCypherTypeName.equals(LOCAL_DATETIME)) {
            return LOCAL_DATETIME_NOT_NULL;
        } else if (parserCypherTypeName.equals(ZONED_DATETIME)) {
            return ZONED_DATETIME_NOT_NULL;
        } else if (parserCypherTypeName.equals(DURATION)) {
            return DURATION_NOT_NULL;
        } else if (parserCypherTypeName.equals(POINT)) {
            return POINT_NOT_NULL;
        } else if (parserCypherTypeName.equals(NODE)) {
            return NODE_NOT_NULL;
        } else if (parserCypherTypeName.equals(RELATIONSHIP)) {
            return RELATIONSHIP_NOT_NULL;
        } else if (parserCypherTypeName.equals(MAP)) {
            return MAP_NOT_NULL;
        } else if (parserCypherTypeName instanceof ListParserCypherTypeName) {
            return listOfNotNull(((ListParserCypherTypeName) parserCypherTypeName).innerType);
        } else if (parserCypherTypeName.equals(PATH)) {
            return PATH_NOT_NULL;
        } else if (parserCypherTypeName.equals(ANY)) {
            return ANY_NOT_NULL;
        } else if (parserCypherTypeName.equals(PROPERTY_VALUE)) {
            return PROPERTY_VALUE_NOT_NULL;
        } else if (parserCypherTypeName instanceof ClosedDynamicUnionParserCypherTypeName) {
            throw new IllegalArgumentException(
                    "Closed Dynamic Union Types can not be appended with `NOT NULL`, specify `NOT NULL` on all inner types instead.");
        }
        throw new IllegalArgumentException(String.format("Unexpected type: %s", parserCypherTypeName.typeName));
    }

    public Integer getOffset() {
        return offset;
    }

    public Integer getLine() {
        return line;
    }

    public Integer getColumn() {
        return column;
    }

    public static class ListParserCypherTypeName extends ParserCypherTypeName {
        final ParserCypherTypeName innerType;
        final boolean isNullable;

        ListParserCypherTypeName(ParserCypherTypeName innerType, boolean isNullable) {
            super(String.format("LIST<%s>%s", innerType.description(), isNullable ? "" : " NOT NULL"));
            this.innerType = innerType;
            this.isNullable = isNullable;
        }

        public ParserCypherTypeName getInnerType() {
            return innerType;
        }

        public boolean isNullable() {
            return isNullable;
        }
    }

    public static class ClosedDynamicUnionParserCypherTypeName extends ParserCypherTypeName {
        final List<ParserCypherTypeName> unionTypes;

        ClosedDynamicUnionParserCypherTypeName(List<ParserCypherTypeName> unionTypes) {
            super(String.join(
                    " | ",
                    unionTypes.stream().map(ParserCypherTypeName::description).toList()));
            this.unionTypes = unionTypes;
        }

        public List<ParserCypherTypeName> getUnionTypes() {
            return unionTypes;
        }
    }
}
