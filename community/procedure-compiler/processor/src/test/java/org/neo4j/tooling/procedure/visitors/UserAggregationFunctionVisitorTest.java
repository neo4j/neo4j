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
package org.neo4j.tooling.procedure.visitors;

import com.google.testing.compile.CompilationRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.stream.Stream;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementVisitor;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;

import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.tooling.procedure.compilerutils.CustomNameExtractor;
import org.neo4j.tooling.procedure.compilerutils.TypeMirrorUtils;
import org.neo4j.tooling.procedure.messages.CompilationMessage;
import org.neo4j.tooling.procedure.testutils.ElementTestUtils;
import org.neo4j.tooling.procedure.visitors.examples.UserAggregationFunctionsExamples;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;

public class UserAggregationFunctionVisitorTest
{
    @Rule
    public CompilationRule compilationRule = new CompilationRule();
    private ElementTestUtils elementTestUtils;
    private ElementVisitor<Stream<CompilationMessage>,Void> visitor;

    @Before
    public void prepare()
    {
        Types types = compilationRule.getTypes();
        Elements elements = compilationRule.getElements();

        elementTestUtils = new ElementTestUtils( compilationRule );
        final TypeMirrorUtils typeMirrorUtils = new TypeMirrorUtils( types, elements );
        visitor = new UserAggregationFunctionVisitor(
                new FunctionVisitor<>( UserAggregationFunction.class, types, elements, typeMirrorUtils,
                        function -> CustomNameExtractor.getName( function::name, function::value ), false ), types );
    }

    @Test
    public void aggregation_functions_with_specified_name_cannot_be_in_root_namespace()
    {
        Element function = elementTestUtils.findMethodElement( UserAggregationFunctionsExamples.class, "functionWithName" );

        Stream<CompilationMessage> errors = visitor.visit( function );

        assertThat( errors ).hasSize( 1 ).extracting( CompilationMessage::getCategory, CompilationMessage::getElement,
                CompilationMessage::getContents ).contains( tuple( Diagnostic.Kind.ERROR, function,
                "Function <in_root_namespace> cannot be defined in the root namespace. Valid name example: com.acme.my_function" ) );
    }

    @Test
    public void aggregation_functions_with_specified_value_cannot_be_in_root_namespace()
    {
        Element function = elementTestUtils.findMethodElement( UserAggregationFunctionsExamples.class, "functionWithValue" );

        Stream<CompilationMessage> errors = visitor.visit( function );

        assertThat( errors ).hasSize( 1 ).extracting( CompilationMessage::getCategory, CompilationMessage::getElement,
                CompilationMessage::getContents ).contains( tuple( Diagnostic.Kind.ERROR, function,
                "Function <in_root_namespace_again> cannot be defined in the root namespace. Valid name example: com.acme.my_function" ) );
    }

    @Test
    public void aggregation_functions_in_non_root_namespace_are_valid()
    {
        Element function = elementTestUtils.findMethodElement( UserAggregationFunctionsExamples.class, "ok" );

        Stream<CompilationMessage> errors = visitor.visit( function );

        assertThat( errors ).isEmpty();
    }

    @Test
    public void aggregation_functions_with_unsupported_return_types_are_invalid()
    {
        Element function = elementTestUtils.findMethodElement( UserAggregationFunctionsExamples.class, "wrongReturnType" );

        Stream<CompilationMessage> errors = visitor.visit( function );

        assertThat( errors ).hasSize( 1 ).extracting( CompilationMessage::getCategory, CompilationMessage::getElement,
                CompilationMessage::getContents ).contains( tuple( Diagnostic.Kind.ERROR, function,
                "Unsupported return type <void> of aggregation function." ) );
    }

    @Test
    public void aggregation_functions_with_parameters_are_invalid()
    {
        Element function = elementTestUtils.findMethodElement( UserAggregationFunctionsExamples.class, "shouldNotHaveParameters" );

        Stream<CompilationMessage> errors = visitor.visit( function );

        assertThat( errors ).hasSize( 1 ).extracting( CompilationMessage::getCategory, CompilationMessage::getElement,
                CompilationMessage::getContents ).contains( tuple( Diagnostic.Kind.ERROR, function,
                "@UserAggregationFunction usage error: method should be public, non-static and without parameters." ) );
    }

    @Test
    public void aggregation_update_functions_with_unsupported_parameter_types_are_invalid()
    {
        Element function = elementTestUtils.findMethodElement( UserAggregationFunctionsExamples.class, "updateWithWrongParameterType" );

        Stream<CompilationMessage> errors = visitor.visit( function );

        assertThat( errors ).hasSize( 1 ).extracting( CompilationMessage::getCategory, CompilationMessage::getContents )
                .contains( tuple( Diagnostic.Kind.ERROR,
                        "Unsupported parameter type <java.lang.Thread> of procedure|function " +
                                "StringAggregatorWithWrongUpdateParameterType#doSomething" ) );
    }

    @Test
    public void aggregation_functions_with_non_annotated_parameters_are_invalid()
    {
        Element function =
                elementTestUtils.findMethodElement( UserAggregationFunctionsExamples.class, "missingParameterAnnotation" );

        Stream<CompilationMessage> errors = visitor.visit( function );

        assertThat( errors ).hasSize( 1 ).extracting( CompilationMessage::getCategory, CompilationMessage::getContents )
                .contains( tuple( Diagnostic.Kind.ERROR,
                        "@org.neo4j.procedure.Name usage error: missing on parameter <foo>" ) );
    }

    @Test
    public void aggregation_result_functions_with_unsupported_return_types_are_invalid()
    {
        Element function =
                elementTestUtils.findMethodElement( UserAggregationFunctionsExamples.class, "resultWithWrongReturnType" );

        Stream<CompilationMessage> errors = visitor.visit( function );

        assertThat( errors ).hasSize( 1 ).extracting( CompilationMessage::getCategory, CompilationMessage::getContents )
                .contains( tuple( Diagnostic.Kind.ERROR,
                "Unsupported return type <java.lang.Thread> of function defined in " +
                "<org.neo4j.tooling.procedure.visitors.examples.UserAggregationFunctionsExamples." +
                "StringAggregatorWithWrongResultReturnType#result>." ) );
    }

    @Test
    public void aggregation_result_functions_with_parameters_are_invalid()
    {
        Element function =
                elementTestUtils.findMethodElement( UserAggregationFunctionsExamples.class, "resultWithParams" );

        Stream<CompilationMessage> errors = visitor.visit( function );

        assertThat( errors ).hasSize( 1 ).extracting( CompilationMessage::getCategory, CompilationMessage::getContents )
                .contains( tuple( Diagnostic.Kind.ERROR,
                        "@UserAggregationUpdate usage error: method should be public, non-static and without parameters." ) );
    }
}
