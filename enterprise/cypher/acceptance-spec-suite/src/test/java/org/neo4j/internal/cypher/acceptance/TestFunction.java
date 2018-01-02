/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.cypher.acceptance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

public class TestFunction
{
    @Context
    public GraphDatabaseService db;

    @UserFunction( "test.toSet" )
    public List<Object> toSet( @Name( "values" ) List<Object> list )
    {
        return new ArrayList<>( new LinkedHashSet<>( list ) );
    }

    @UserFunction( "test.nodeList" )
    public List<Object> nodeList()
    {
        Result result = db.execute( "MATCH (n) RETURN n LIMIT 1" );
        Object node = result.next().get( "n" );
        return Collections.singletonList( node );
    }
}
