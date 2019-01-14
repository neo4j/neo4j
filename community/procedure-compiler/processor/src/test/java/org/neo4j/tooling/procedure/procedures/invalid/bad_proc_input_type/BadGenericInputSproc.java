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
package org.neo4j.tooling.procedure.procedures.invalid.bad_proc_input_type;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class BadGenericInputSproc
{

    @Procedure
    public void doSomething( @Name( "test" ) List<List<Map<String,Thread>>> unsupportedType )
    {

    }

    @Procedure
    public void doSomething2( @Name( "test" ) Map<String,List<ExecutorService>> unsupportedType )
    {

    }

    @Procedure
    public void doSomething3( @Name( "test" ) Map unsupportedType )
    {

    }

    @Procedure
    public void works1( @Name( "test" ) List<String> supported )
    {
    }

    @Procedure
    public void works2( @Name( "test" ) List<List<Object>> supported )
    {
    }

    @Procedure
    public void works3( @Name( "test" ) Map<String,Object> supported )
    {
    }

    @Procedure
    public void works4( @Name( "test" ) List<List<List<Map<String,Object>>>> supported )
    {
    }

    @Procedure
    public void works5( @Name( "test" ) List<List<List<Path>>> supported )
    {
    }

    @Procedure
    public void works6( @Name( "test" ) List<Node> supported )
    {
    }

    @Procedure
    public void works7( @Name( "test" ) List<List<Relationship>> supported )
    {
    }

    @Procedure
    public void works8( @Name( "test" ) Map<String,List<List<Relationship>>> supported )
    {
    }

    @Procedure
    public void works9( @Name( "test" ) Map<String,Map<String,List<Node>>> supported )
    {
    }
}
