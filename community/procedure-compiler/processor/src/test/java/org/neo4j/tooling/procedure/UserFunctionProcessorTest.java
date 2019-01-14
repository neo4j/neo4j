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
import com.google.testing.compile.CompileTester.UnsuccessfulCompilationClause;
import org.junit.Rule;
import org.junit.Test;

import javax.annotation.processing.Processor;
import javax.tools.JavaFileObject;

import org.neo4j.tooling.procedure.testutils.JavaFileObjectUtils;

import static com.google.common.truth.Truth.assert_;
import static com.google.testing.compile.JavaSourceSubjectFactory.javaSource;
import static com.google.testing.compile.JavaSourcesSubjectFactory.javaSources;
import static java.util.Arrays.asList;

public class UserFunctionProcessorTest extends ExtensionTestBase
{

    @Rule
    public CompilationRule compilation = new CompilationRule();

    private Processor processor = new UserFunctionProcessor();

    @Test
    public void fails_if_parameters_are_not_properly_annotated()
    {
        JavaFileObject function =
                JavaFileObjectUtils.INSTANCE.procedureSource( "invalid/missing_name/MissingNameUserFunction.java" );

        UnsuccessfulCompilationClause compilation =
                assert_().about( javaSource() ).that( function ).processedWith( processor() ).failsToCompile()
                        .withErrorCount( 2 );

        compilation.withErrorContaining( "@org.neo4j.procedure.Name usage error: missing on parameter <parameter>" )
                .in( function ).onLine( 28 );

        compilation.withErrorContaining( "@org.neo4j.procedure.Name usage error: missing on parameter <otherParam>" )
                .in( function ).onLine( 28 );
    }

    @Test
    public void fails_if_return_type_is_incorrect()
    {
        JavaFileObject function = JavaFileObjectUtils.INSTANCE
                .procedureSource( "invalid/bad_return_type/BadReturnTypeUserFunction.java" );

        assert_().about( javaSource() ).that( function ).processedWith( processor() ).failsToCompile().withErrorCount( 1 )
                .withErrorContaining(
                        "Unsupported return type <java.util.stream.Stream<java.lang.Long>> of function defined in " +
                "<org.neo4j.tooling.procedure.procedures.invalid.bad_return_type.BadReturnTypeUserFunction#wrongReturnTypeFunction>" )
                .in( function ).onLine( 36 );
    }

    @Test
    public void fails_if_function_primitive_input_type_is_not_supported()
    {
        JavaFileObject function = JavaFileObjectUtils.INSTANCE
                .procedureSource( "invalid/bad_proc_input_type/BadPrimitiveInputUserFunction.java" );

        assert_().about( javaSource() ).that( function ).processedWith( processor() ).failsToCompile().withErrorCount( 1 )
                .withErrorContaining(
                        "Unsupported parameter type <short> of procedure|function BadPrimitiveInputUserFunction#doSomething" )
                .in( function ).onLine( 32 );
    }

    @Test
    public void fails_if_function_generic_input_type_is_not_supported()
    {
        JavaFileObject function = JavaFileObjectUtils.INSTANCE
                .procedureSource( "invalid/bad_proc_input_type/BadGenericInputUserFunction.java" );

        UnsuccessfulCompilationClause compilation =
                assert_().about( javaSource() ).that( function ).processedWith( processor() ).failsToCompile()
                        .withErrorCount( 3 );

        compilation.withErrorContaining( "Unsupported parameter type " +
                "<java.util.List<java.util.List<java.util.Map<java.lang.String,java.lang.Thread>>>>" +
                " of procedure|function BadGenericInputUserFunction#doSomething" ).in( function ).onLine( 36 );

        compilation.withErrorContaining( "Unsupported parameter type " +
                "<java.util.Map<java.lang.String,java.util.List<java.util.concurrent.ExecutorService>>>" +
                " of procedure|function BadGenericInputUserFunction#doSomething2" ).in( function ).onLine( 42 );

        compilation.withErrorContaining(
                "Unsupported parameter type <java.util.Map> of procedure|function BadGenericInputUserFunction#doSomething3" )
                .in( function ).onLine( 48 );
    }

    @Test
    public void fails_if_duplicate_functions_are_declared()
    {
        JavaFileObject firstDuplicate =
                JavaFileObjectUtils.INSTANCE.procedureSource( "invalid/duplicated/UserFunction1.java" );
        JavaFileObject secondDuplicate =
                JavaFileObjectUtils.INSTANCE.procedureSource( "invalid/duplicated/UserFunction2.java" );

        assert_().about( javaSources() ).that( asList( firstDuplicate, secondDuplicate ) ).processedWith( processor() )
                .failsToCompile().withErrorCount( 2 ).withErrorContaining(
                "Procedure|function name <org.neo4j.tooling.procedure.procedures.invalid.duplicated.foobar> is " +
                        "already defined 2 times. It should be defined only once!" );
    }

    @Test
    public void succeeds_to_process_valid_stored_procedures()
    {
        assert_().about( javaSource() )
                .that( JavaFileObjectUtils.INSTANCE.procedureSource( "valid/UserFunctions.java" ) )
                .processedWith( processor() ).compilesWithoutError();

    }

    @Override
    Processor processor()
    {
        return processor;
    }
}
