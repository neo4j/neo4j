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
package org.neo4j.tooling.procedure.visitors.examples;

import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

public class UserFunctionsExamples
{
    @UserFunction( name = "in_root_namespace" )
    public String functionWithName()
    {
        return "42";
    }

    @UserFunction( value = "in_root_namespace_again" )
    public String functionWithValue()
    {
        return "42";
    }

    @UserFunction( name = "not.in.root.namespace" )
    public String ok()
    {
        return "42";
    }

    @UserFunction( name = "com.acme.foobar" )
    public void wrongReturnType()
    {

    }

    @UserFunction( name = "com.acme.foobar" )
    public String wrongParameterType( @Name( "foo" ) Thread foo )
    {
        return "42";
    }

    @UserFunction( name = "com.acme.foobar" )
    public String missingParameterAnnotation( @Name( "foo" ) String foo, String oops )
    {
        return "42";
    }
}
