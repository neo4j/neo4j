/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

package org.neo4j.kernel.builtinprocs;

import java.util.List;
import java.util.stream.Stream;

import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;

public class BuiltInSchemaProcedures
{
    @Context
    public GraphDatabaseAPI db;

    @Context
    public KernelTransaction tx;

    @Procedure( name = "db.schemaAsTable", mode = Mode.READ )
    @Description( "Show the schema of the data in tabular form." )
    public Stream<SchemaInfoResult> schemaAsTable()
    {
        return new SchemaCalculator( db, tx ).calculateTabularResultStream();
    }

    // TODO: Next step
//    @Procedure( value = "db.schema.graph", mode = Mode.READ ) // TODO: Change to db.schema when ready to completly replace old one
//    @Description( "Show the schema of the data." ) // old description
//    public Stream<SchemaProcedure.GraphResult> schemaAsGraph()   //TODO: Maybe extract inner result classes
//    {
//        return new SchemaCalculator( db, tx ).calculateGraphResultStream();
//    }

    public static class SchemaInfoResult
    {
        /**
         * Indicates whether the entry is a node or a relationship
         */
        public final String type;
        /**
         * A combination of labels or a relationship
         */
        public final List<String> nodeLabelsOrRelType;
        /**
         * A property that occurs on the given label combination / relationship type
         */
        public final String property;
        /**
         * The CypherType of the given property on the given label combination / relationship type
         */
        public final String cypherType;

        public SchemaInfoResult( String type, List<String> nodeLabelsOrRelType, String property, String cypherType )
        {
            this.type = type;
            this.nodeLabelsOrRelType = nodeLabelsOrRelType;
            this.property = property;
            this.cypherType = cypherType;
        }
    }
}
