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
import java.util.stream.Stream;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;

import static org.neo4j.procedure.Mode.DBMS;
import static org.neo4j.procedure.Mode.DEFAULT;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.SCHEMA;
import static org.neo4j.procedure.Mode.WRITE;

public class Procedures
{

    @Procedure
    public Stream<Records.LongWrapper> theAnswer()
    {
        return Stream.of( new Records.LongWrapper( 42L ) );
    }

    @Procedure
    public void simpleInput00()
    {
    }

    @Procedure
    public void simpleInput01( @Name( "foo" ) String input )
    {
    }

    @Procedure
    public void simpleInput02( @Name( "foo" ) long input )
    {
    }

    @Procedure
    public void simpleInput03( @Name( "foo" ) Long input )
    {
    }

    @Procedure
    public void simpleInput04( @Name( "foo" ) Number input )
    {
    }

    @Procedure
    public void simpleInput05( @Name( "foo" ) Boolean input )
    {
    }

    @Procedure
    public void simpleInput06( @Name( "foo" ) boolean input )
    {
    }

    @Procedure
    public void simpleInput07( @Name( "foo" ) Object input )
    {
    }

    @Procedure
    public void simpleInput08( @Name( "foo" ) Node input )
    {
    }

    @Procedure
    public void simpleInput09( @Name( "foo" ) Path input )
    {
    }

    @Procedure
    public void simpleInput10( @Name( "foo" ) Relationship input )
    {
    }

    @Procedure
    public Stream<Records.SimpleTypesWrapper> simpleInput11( @Name( "foo" ) String input )
    {
        return Stream.of( new Records.SimpleTypesWrapper() );
    }

    @Procedure
    public Stream<Records.SimpleTypesWrapper> simpleInput12( @Name( "foo" ) long input )
    {
        return Stream.of( new Records.SimpleTypesWrapper() );
    }

    @Procedure
    public Stream<Records.SimpleTypesWrapper> simpleInput13( @Name( "foo" ) Long input )
    {
        return Stream.of( new Records.SimpleTypesWrapper() );
    }

    @Procedure
    public Stream<Records.SimpleTypesWrapper> simpleInput14( @Name( "foo" ) Number input )
    {
        return Stream.of( new Records.SimpleTypesWrapper() );
    }

    @Procedure
    public Stream<Records.SimpleTypesWrapper> simpleInput15( @Name( "foo" ) Boolean input )
    {
        return Stream.of( new Records.SimpleTypesWrapper() );
    }

    @Procedure
    public Stream<Records.SimpleTypesWrapper> simpleInput16( @Name( "foo" ) boolean input )
    {
        return Stream.of( new Records.SimpleTypesWrapper() );
    }

    @Procedure
    public Stream<Records.SimpleTypesWrapper> simpleInput17( @Name( "foo" ) Object input )
    {
        return Stream.of( new Records.SimpleTypesWrapper() );
    }

    @Procedure
    public Stream<Records.SimpleTypesWrapper> simpleInput18( @Name( "foo" ) Node input )
    {
        return Stream.of( new Records.SimpleTypesWrapper() );
    }

    @Procedure
    public Stream<Records.SimpleTypesWrapper> simpleInput19( @Name( "foo" ) Path input )
    {
        return Stream.of( new Records.SimpleTypesWrapper() );
    }

    @Procedure
    public Stream<Records.SimpleTypesWrapper> simpleInput20( @Name( "foo" ) Relationship input )
    {
        return Stream.of( new Records.SimpleTypesWrapper() );
    }

    @Procedure
    public Stream<Records.SimpleTypesWrapper> simpleInput21()
    {
        return Stream.of( new Records.SimpleTypesWrapper() );
    }

    @Procedure
    public void genericInput01( @Name( "foo" ) List<String> input )
    {
    }

    @Procedure
    public void genericInput02( @Name( "foo" ) List<List<Node>> input )
    {
    }

    @Procedure
    public void genericInput03( @Name( "foo" ) Map<String,List<Node>> input )
    {
    }

    @Procedure
    public void genericInput04( @Name( "foo" ) Map<String,Object> input )
    {
    }

    @Procedure
    public void genericInput05( @Name( "foo" ) Map<String,List<List<Map<String,Map<String,List<Path>>>>>> input )
    {
    }

    @Procedure
    public Stream<Records.GenericTypesWrapper> genericInput06( @Name( "foo" ) List<String> input )
    {
        return Stream.of( new Records.GenericTypesWrapper() );
    }

    @Procedure
    public Stream<Records.GenericTypesWrapper> genericInput07( @Name( "foo" ) List<List<Node>> input )
    {
        return Stream.of( new Records.GenericTypesWrapper() );
    }

    @Procedure
    public Stream<Records.GenericTypesWrapper> genericInput08( @Name( "foo" ) Map<String,List<Node>> input )
    {
        return Stream.of( new Records.GenericTypesWrapper() );
    }

    @Procedure
    public Stream<Records.GenericTypesWrapper> genericInput09( @Name( "foo" ) Map<String,Object> input )
    {
        return Stream.of( new Records.GenericTypesWrapper() );
    }

    @Procedure
    public Stream<Records.GenericTypesWrapper> genericInput10(
            @Name( "foo" ) Map<String,List<List<Map<String,Map<String,List<Path>>>>>> input )
    {
        return Stream.of( new Records.GenericTypesWrapper() );
    }

    @Procedure
    @PerformsWrites
    public void performsWrites()
    {
    }

    @Procedure( mode = DEFAULT )
    public void defaultMode()
    {
    }

    @Procedure( mode = READ )
    public void readMode()
    {
    }

    @Procedure( mode = WRITE )
    public void writeMode()
    {
    }

    @Procedure( mode = SCHEMA )
    public void schemaMode()
    {
    }

    @Procedure( mode = DBMS )
    public void dbmsMode()
    {
    }
}
