/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.tooling.procedure.procedures.valid;

import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

public class UserFunctions
{

    @UserFunction
    public String simpleInput00()
    {
        return "42";
    }

    @UserFunction
    public String simpleInput01( @Name( "foo" ) String input )
    {
        return "42";
    }

    @UserFunction
    public String simpleInput02( @Name( "foo" ) long input )
    {
        return "42";
    }

    @UserFunction
    public String simpleInput03( @Name( "foo" ) Long input )
    {
        return "42";
    }

    @UserFunction
    public String simpleInput04( @Name( "foo" ) Number input )
    {
        return "42";
    }

    @UserFunction
    public String simpleInput05( @Name( "foo" ) Boolean input )
    {
        return "42";
    }

    @UserFunction
    public String simpleInput06( @Name( "foo" ) boolean input )
    {
        return "42";
    }

    @UserFunction
    public String simpleInput07( @Name( "foo" ) Object input )
    {
        return "42";
    }

    @UserFunction
    public String simpleInput08( @Name( "foo" ) Node input )
    {
        return "42";
    }

    @UserFunction
    public String simpleInput09( @Name( "foo" ) Path input )
    {
        return "42";
    }

    @UserFunction
    public String simpleInput10( @Name( "foo" ) Relationship input )
    {
        return "42";
    }

    @UserFunction
    public String genericInput01( @Name( "foo" ) List<String> input )
    {
        return "42";
    }

    @UserFunction
    public String genericInput02( @Name( "foo" ) List<List<Node>> input )
    {
        return "42";
    }

    @UserFunction
    public String genericInput03( @Name( "foo" ) Map<String,List<Node>> input )
    {
        return "42";
    }

    @UserFunction
    public String genericInput04( @Name( "foo" ) Map<String,Object> input )
    {
        return "42";
    }

    @UserFunction
    public String genericInput05( @Name( "foo" ) Map<String,List<List<Map<String,Map<String,List<Path>>>>>> input )
    {
        return "42";
    }

}
