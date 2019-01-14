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
package org.neo4j.tooling.procedure;

import com.google.testing.compile.CompilationRule;
import com.google.testing.compile.CompileTester;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import javax.annotation.processing.Processor;
import javax.tools.JavaFileObject;

import org.neo4j.tooling.procedure.testutils.JavaFileObjectUtils;

import static com.google.common.truth.Truth.assert_;
import static com.google.testing.compile.JavaSourceSubjectFactory.javaSource;

public class UserAggregationFunctionProcessorTest extends ExtensionTestBase
{

    @Rule
    public CompilationRule compilation = new CompilationRule();

    private Processor processor = new UserAggregationFunctionProcessor();

    @Test
    public void fails_if_aggregation_function_directly_exposes_parameters()
    {
        JavaFileObject function =
                JavaFileObjectUtils.INSTANCE.procedureSource( "invalid/aggregation/FunctionWithParameters.java" );

        assert_().about( javaSource() ).that( function ).processedWith( processor() ).failsToCompile()
                .withErrorCount( 1 )
                .withErrorContaining( "@UserAggregationFunction usage error: method should be public, non-static and without parameters." )
                .in( function ).onLine( 31 );
    }

    @Test
    public void fails_if_aggregation_function_exposes_non_aggregation_return_type()
    {
        JavaFileObject function =
                JavaFileObjectUtils.INSTANCE.procedureSource( "invalid/aggregation/FunctionWithWrongReturnType.java" );

        assert_().about( javaSource() ).that( function ).processedWith( processor() ).failsToCompile()
                .withErrorCount( 1 )
                .withErrorContaining( "Unsupported return type <void> of aggregation function." )
                .in( function ).onLine( 27 );
    }

    @Test
    @Ignore( "javac fails to publish the deferred diagnostic of the second error to com.google.testing.compile.Compiler" )
    public void fails_if_aggregation_function_exposes_return_type_without_aggregation_methods()
    {
        JavaFileObject function =
                JavaFileObjectUtils.INSTANCE.procedureSource( "invalid/aggregation/FunctionWithoutAggregationMethods.java" );

        CompileTester.UnsuccessfulCompilationClause unsuccessfulCompilationClause =
                assert_().about( javaSource() ).that( function ).processedWith( processor() ).failsToCompile()
                        .withErrorCount( 2 );

        unsuccessfulCompilationClause
                .withErrorContaining( "@UserAggregationUpdate usage error: expected aggregation type " +
                "<org.neo4j.tooling.procedure.procedures.invalid.aggregation.FunctionWithoutAggregationMethods.MyAggregation> " +
                "to define exactly 1 method with this annotation. Found none." )
                .in( function ).onLine( 31 );
        unsuccessfulCompilationClause
                .withErrorContaining( "@UserAggregationResult usage error: expected aggregation type " +
                "<org.neo4j.tooling.procedure.procedures.invalid.aggregation.FunctionWithoutAggregationMethods.MyAggregation> " +
                "to define exactly 1 method with this annotation. Found none." )
                .in( function ).onLine( 31 );
    }

    @Override
    Processor processor()
    {
        return processor;
    }
}
