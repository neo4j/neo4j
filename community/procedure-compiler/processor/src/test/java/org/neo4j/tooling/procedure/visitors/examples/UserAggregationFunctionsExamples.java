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
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;

public class UserAggregationFunctionsExamples
{
    @UserAggregationFunction( name = "in_root_namespace" )
    public StringAggregator functionWithName()
    {
        return new StringAggregator();
    }

    @UserAggregationFunction( value = "in_root_namespace_again" )
    public StringAggregator functionWithValue()
    {
        return new StringAggregator();
    }

    @UserAggregationFunction( name = "not.in.root.namespace" )
    public StringAggregator ok()
    {
        return new StringAggregator();
    }

    @UserAggregationFunction( name = "com.acme.foobar" )
    public void wrongReturnType()
    {

    }

    @UserAggregationFunction( name = "com.acme.foobar" )
    public StringAggregator shouldNotHaveParameters( @Name( "hello" ) String hello )
    {
        return new StringAggregator();
    }

    @UserAggregationFunction( name = "com.acme.foobar" )
    public StringAggregatorWithWrongUpdateParameterType updateWithWrongParameterType(  )
    {
        return new StringAggregatorWithWrongUpdateParameterType();
    }

    @UserAggregationFunction( name = "com.acme.foobar" )
    public StringAggregatorWithMissingAnnotationOnParameterType missingParameterAnnotation(  )
    {
        return new StringAggregatorWithMissingAnnotationOnParameterType();
    }

    @UserAggregationFunction( name = "com.acme.foobar" )
    public StringAggregatorWithWrongResultReturnType resultWithWrongReturnType(  )
    {
        return new StringAggregatorWithWrongResultReturnType();
    }

    @UserAggregationFunction( name = "com.acme.foobar" )
    public StringAggregatorWithResultMethodWithParameters resultWithParams(  )
    {
        return new StringAggregatorWithResultMethodWithParameters();
    }

    public static class StringAggregator
    {
        @UserAggregationUpdate
        public void doSomething( @Name( "foo" ) String foo )
        {

        }

        @UserAggregationResult
        public long result()
        {
            return 42L;
        }
    }

    public static class StringAggregatorWithWrongUpdateParameterType
    {
        @UserAggregationUpdate
        public void doSomething( @Name( "foo" ) Thread foo )
        {

        }

        @UserAggregationResult
        public long result()
        {
            return 42L;
        }
    }

    public static class StringAggregatorWithMissingAnnotationOnParameterType
    {
        @UserAggregationUpdate
        public void doSomething( long foo )
        {

        }

        @UserAggregationResult
        public long result()
        {
            return 42L;
        }
    }

    public static class StringAggregatorWithWrongResultReturnType
    {
        @UserAggregationUpdate
        public void doSomething( @Name( "foo" ) long foo )
        {

        }

        @UserAggregationResult
        public Thread result()
        {
            return new Thread(  );
        }
    }

    public static class StringAggregatorWithResultMethodWithParameters
    {
        @UserAggregationUpdate
        public void doSomething( @Name( "foo" ) long foo )
        {

        }

        @UserAggregationResult
        public long result( String shouldNotHaveAnyParam )
        {
            return 42L;
        }
    }
}
