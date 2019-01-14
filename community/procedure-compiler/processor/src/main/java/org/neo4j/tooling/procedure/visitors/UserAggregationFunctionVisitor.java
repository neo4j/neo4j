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

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementVisitor;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.ElementFilter;
import javax.lang.model.util.SimpleElementVisitor8;
import javax.lang.model.util.Types;

import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;
import org.neo4j.tooling.procedure.messages.AggregationError;
import org.neo4j.tooling.procedure.messages.CompilationMessage;

import static javax.lang.model.type.TypeKind.VOID;

public class UserAggregationFunctionVisitor extends SimpleElementVisitor8<Stream<CompilationMessage>,Void>
{

    private final FunctionVisitor<UserAggregationFunction> functionVisitor;
    private final Types types;
    private final ElementVisitor<CharSequence,Void> typeVisitor;

    public UserAggregationFunctionVisitor( FunctionVisitor<UserAggregationFunction> baseFunctionVisitor, Types types )
    {
        this.functionVisitor = baseFunctionVisitor;
        this.types = types;
        this.typeVisitor = new QualifiedTypeVisitor();
    }

    @Override
    public Stream<CompilationMessage> visitExecutable( ExecutableElement aggregationFunction, Void ignored )
    {
        return Stream.of( functionVisitor.validateEnclosingClass( aggregationFunction ),
                validateParameters( aggregationFunction, UserAggregationFunction.class ),
                functionVisitor.validateName( aggregationFunction ), validateAggregationType( aggregationFunction ) )
                .flatMap( Function.identity() );
    }

    private Stream<CompilationMessage> validateAggregationType( ExecutableElement aggregationFunction )
    {
        TypeMirror returnType = aggregationFunction.getReturnType();
        Element returnTypeElement = types.asElement( returnType );
        if ( returnTypeElement == null )
        {
            return Stream.of( new AggregationError( aggregationFunction,
                    "Unsupported return type <%s> of aggregation function.", returnType.toString(),
                    aggregationFunction.getEnclosingElement() ) );
        }

        List<ExecutableElement> updateMethods = methodsAnnotatedWith( returnTypeElement, UserAggregationUpdate.class );
        List<ExecutableElement> resultMethods = methodsAnnotatedWith( returnTypeElement, UserAggregationResult.class );

        return Stream.concat( validateAggregationUpdateMethod( aggregationFunction, returnTypeElement, updateMethods ),
                validateAggregationResultMethod( aggregationFunction, returnTypeElement, resultMethods ) );
    }

    private List<ExecutableElement> methodsAnnotatedWith( Element returnType,
            Class<? extends Annotation> annotationType )
    {
        return ElementFilter.methodsIn( returnType.getEnclosedElements() ).stream()
                .filter( m -> m.getAnnotation( annotationType ) != null ).collect( Collectors.toList() );
    }

    private Stream<CompilationMessage> validateAggregationUpdateMethod( ExecutableElement aggregationFunction, Element returnType,
            List<ExecutableElement> updateMethods )
    {
        if ( updateMethods.size() != 1 )
        {
            return Stream.of( missingAnnotation( aggregationFunction, returnType, updateMethods, UserAggregationUpdate.class ) );
        }

        Stream<CompilationMessage> errors = Stream.empty();

        ExecutableElement updateMethod = updateMethods.iterator().next();

        if ( !isValidUpdateSignature( updateMethod ) )
        {
            errors = Stream.of( new AggregationError( updateMethod,
                    "@%s usage error: method should be public, non-static and define 'void' as return type.",
                    UserAggregationUpdate.class.getSimpleName() ) );
        }
        return Stream.concat( errors, functionVisitor.validateParameters( updateMethod.getParameters() ) );
    }

    private Stream<CompilationMessage> validateAggregationResultMethod( ExecutableElement aggregationFunction, Element returnType,
            List<ExecutableElement> resultMethods )
    {
        if ( resultMethods.size() != 1 )
        {
            return Stream.of( missingAnnotation( aggregationFunction, returnType, resultMethods, UserAggregationResult.class ) );
        }

        ExecutableElement resultMethod = resultMethods.iterator().next();
        return Stream.concat( validateParameters( resultMethod, UserAggregationUpdate.class ),
                functionVisitor.validateReturnType( resultMethod ) );
    }

    private Stream<CompilationMessage> validateParameters( ExecutableElement resultMethod,
            Class<? extends Annotation> annotation )
    {
        if ( !isValidAggregationSignature( resultMethod ) )
        {
            return Stream.of( new AggregationError( resultMethod,
                    "@%s usage error: method should be public, non-static and without parameters.",
                    annotation.getSimpleName() ) );
        }
        return Stream.empty();
    }

    private AggregationError missingAnnotation( ExecutableElement aggregationFunction, Element returnType,
            List<ExecutableElement> updateMethods, Class<? extends Annotation> annotation )
    {
        return new AggregationError( aggregationFunction,
                "@%s usage error: expected aggregation type <%s> to define exactly 1 method with this annotation. %s.",
                annotation.getSimpleName(), typeVisitor.visit( returnType ), updateMethods.isEmpty() ? "Found none"
                                                                                                     : "Several methods found: " +
                                                                                                      methodNames( updateMethods ) );
    }

    private boolean isValidUpdateSignature( ExecutableElement updateMethod )
    {
        // note: parameters are checked subsequently
        return isPublicNonStatic( updateMethod.getModifiers() ) &&
                updateMethod.getReturnType().getKind().equals( VOID );
    }

    private boolean isValidAggregationSignature( ExecutableElement resultMethod )
    {
        // note: return type is checked subsequently
        return isPublicNonStatic( resultMethod.getModifiers() ) && resultMethod.getParameters().isEmpty();
    }

    private boolean isPublicNonStatic( Set<Modifier> modifiers )
    {
        return modifiers.contains( Modifier.PUBLIC ) && !modifiers.contains( Modifier.STATIC );
    }

    private String methodNames( List<ExecutableElement> updateMethods )
    {
        return updateMethods.stream().map( ExecutableElement::getSimpleName ).collect( Collectors.joining( "," ) );
    }
}
