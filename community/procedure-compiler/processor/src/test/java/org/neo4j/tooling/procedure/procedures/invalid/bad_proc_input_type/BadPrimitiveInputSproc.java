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

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class BadPrimitiveInputSproc
{

    @Procedure
    public void doSomething( @Name( "test" ) short unsupportedType )
    {

    }

    @Procedure
    public void works01( @Name( "test" ) String supported )
    {
    }

    @Procedure
    public void works02( @Name( "test" ) Long supported )
    {
    }

    @Procedure
    public void works03( @Name( "test" ) long supported )
    {
    }

    @Procedure
    public void works04( @Name( "test" ) Double supported )
    {
    }

    @Procedure
    public void works05( @Name( "test" ) double supported )
    {
    }

    @Procedure
    public void works06( @Name( "test" ) Number supported )
    {
    }

    @Procedure
    public void works07( @Name( "test" ) Boolean supported )
    {
    }

    @Procedure
    public void works08( @Name( "test" ) boolean supported )
    {
    }

    @Procedure
    public void works09( @Name( "test" ) Object supported )
    {
    }

    @Procedure
    public void works10( @Name( "test" ) Node supported )
    {
    }

    @Procedure
    public void works11( @Name( "test" ) Relationship supported )
    {
    }

    @Procedure
    public void works12( @Name( "test" ) Path supported )
    {
    }
}
