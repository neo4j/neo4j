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
import org.neo4j.procedure.UserFunction;

public class BadPrimitiveInputUserFunction
{

    @UserFunction
    public String doSomething( @Name( "test" ) short unsupportedType )
    {
        return "42";
    }

    @UserFunction
    public String works01( @Name( "test" ) String supported )
    {
        return "42";
    }

    @UserFunction
    public String works02( @Name( "test" ) Long supported )
    {
        return "42";
    }

    @UserFunction
    public String works03( @Name( "test" ) long supported )
    {
        return "42";
    }

    @UserFunction
    public String works04( @Name( "test" ) Double supported )
    {
        return "42";
    }

    @UserFunction
    public String works05( @Name( "test" ) double supported )
    {
        return "42";
    }

    @UserFunction
    public String works06( @Name( "test" ) Number supported )
    {
        return "42";
    }

    @UserFunction
    public String works07( @Name( "test" ) Boolean supported )
    {
        return "42";
    }

    @UserFunction
    public String works08( @Name( "test" ) boolean supported )
    {
        return "42";
    }

    @UserFunction
    public String works09( @Name( "test" ) Object supported )
    {
        return "42";
    }

    @UserFunction
    public String works10( @Name( "test" ) Node supported )
    {
        return "42";
    }

    @UserFunction
    public String works11( @Name( "test" ) Relationship supported )
    {
        return "42";
    }

    @UserFunction
    public String works12( @Name( "test" ) Path supported )
    {
        return "42";
    }
}
