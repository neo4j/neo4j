/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.tooling.procedure.procedures.invalid.bad_return_type;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class BadReturnTypeSproc {

    @Context
    public GraphDatabaseService db;

    @Procedure
    public Long invalidSproc1(@Name("foo") String parameter) {
        return 42L;
    }

    @Procedure
    public Stream<String> invalidSproc2(@Name("foo") String parameter) {
        return Stream.empty();
    }

    @Procedure
    public Stream<Relationship> invalidSproc3(@Name("foo") String parameter) {
        return Stream.empty();
    }

    @Procedure
    public Stream<Map<String, String>> invalidSproc4(@Name("foo") String parameter) {
        return Stream.empty();
    }

    @Procedure
    public Stream<HashMap<String, String>> invalidSproc5(@Name("foo") String parameter) {
        return Stream.empty();
    }

    @Procedure
    public Stream<Object> invalidSproc6(@Name("foo") String parameter) {
        return Stream.empty();
    }

    @Procedure
    public void validSproc(@Name("foo") String parameter) {}
}
