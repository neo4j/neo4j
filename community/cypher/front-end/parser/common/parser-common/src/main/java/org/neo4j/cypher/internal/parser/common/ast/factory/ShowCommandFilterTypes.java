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

public enum ShowCommandFilterTypes {
    // index specific
    BTREE("BTREE"),
    RANGE("RANGE"),
    FULLTEXT("FULLTEXT"),
    TEXT("TEXT"),
    POINT("POINT"),
    VECTOR("VECTOR"),
    LOOKUP("LOOKUP"),
    // constraint specific
    UNIQUE("UNIQUE[NESS]"),
    NODE_UNIQUE("NODE UNIQUE[NESS]"),
    RELATIONSHIP_UNIQUE("REL[ATIONSHIP] UNIQUE[NESS]"),
    KEY("KEY"),
    NODE_KEY("NODE KEY"),
    RELATIONSHIP_KEY("REL[ATIONSHIP] KEY"),
    OLD_EXIST("EXIST"),
    EXIST("[PROPERTY] EXIST[ENCE]"),
    NODE_OLD_EXIST("NODE EXIST"),
    NODE_EXIST("NODE [PROPERTY] EXIST[ENCE]"),
    RELATIONSHIP_OLD_EXIST("RELATIONSHIP EXIST"),
    RELATIONSHIP_EXIST("REL[ATIONSHIP] [PROPERTY] EXIST[ENCE]"),
    PROP_TYPE("PROPERTY TYPE"),
    NODE_PROP_TYPE("NODE PROPERTY TYPE"),
    RELATIONSHIP_PROP_TYPE("REL[ATIONSHIP] PROPERTY TYPE"),
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
