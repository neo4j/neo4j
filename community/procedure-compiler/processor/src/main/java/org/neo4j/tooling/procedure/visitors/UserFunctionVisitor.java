/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.neo4j.tooling.procedure.compilerutils.TypeMirrorUtils;
import org.neo4j.tooling.procedure.messages.CompilationMessage;
import org.neo4j.tooling.procedure.messages.FunctionInRootNamespaceError;
import org.neo4j.tooling.procedure.messages.ReturnTypeError;
import org.neo4j.tooling.procedure.validators.AllowedTypesValidator;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;
import javax.lang.model.element.ElementVisitor;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.SimpleElementVisitor8;
import javax.lang.model.util.Types;

import org.neo4j.procedure.UserFunction;

public class UserFunctionVisitor extends SimpleElementVisitor8<Stream<CompilationMessage>,Void>
{

    private final ElementVisitor<Stream<CompilationMessage>,Void> parameterVisitor;
    private final Predicate<TypeMirror> allowedTypesValidator;
    private final Elements elements;

    public UserFunctionVisitor( Types types, Elements elements, TypeMirrorUtils typeMirrorUtils )
    {
        this.parameterVisitor = new ParameterVisitor( new ParameterTypeVisitor( types, typeMirrorUtils ) );
        this.allowedTypesValidator = new AllowedTypesValidator( typeMirrorUtils, types );
        this.elements = elements;
    }

    @Override
    public Stream<CompilationMessage> visitExecutable( ExecutableElement method, Void ignored )
    {
        return Stream
                .concat( Stream.concat( validateParameters( method.getParameters(), ignored ), validateName( method ) ),
                        validateReturnType( method ) );
    }

    private Stream<CompilationMessage> validateParameters( List<? extends VariableElement> parameters, Void ignored )
    {
        return parameters.stream().flatMap( var -> parameterVisitor.visit( var, ignored ) );
    }

    private Stream<CompilationMessage> validateName( ExecutableElement method )
    {
        UserFunction function = method.getAnnotation( UserFunction.class );
        String name = function.name();
        if ( !name.isEmpty() && isInRootNamespace( name ) )
        {
            return Stream.of( rootNamespaceError( method, name ) );
        }
        String value = function.value();
        if ( !value.isEmpty() && isInRootNamespace( value ) )
        {
            return Stream.of( rootNamespaceError( method, value ) );
        }
        PackageElement namespace = elements.getPackageOf( method );
        if ( namespace == null )
        {
            return Stream.of( rootNamespaceError( method ) );
        }
        return Stream.empty();
    }

    private Stream<CompilationMessage> validateReturnType( ExecutableElement method )
    {
        TypeMirror returnType = method.getReturnType();
        if ( !allowedTypesValidator.test( returnType ) )
        {
            return Stream.of( new ReturnTypeError( method,
                    "Unsupported return type <%s> of function defined in " + "<%s#%s>.", returnType,
                    method.getEnclosingElement(), method.getSimpleName() ) );
        }
        return Stream.empty();
    }

    private boolean isInRootNamespace( String name )
    {
        return !name.contains( "." ) || name.split( "\\." )[0].isEmpty();
    }

    private FunctionInRootNamespaceError rootNamespaceError( ExecutableElement method, String name )
    {
        return new FunctionInRootNamespaceError( method,
                "Function <%s> cannot be defined in the root namespace. Valid name example: com.acme.my_function",
                name );
    }

    private FunctionInRootNamespaceError rootNamespaceError( ExecutableElement method )
    {
        return new FunctionInRootNamespaceError( method,
                "Function defined in <%s#%s> cannot be defined in the root namespace. " +
                        "Valid name example: com.acme.my_function", method.getEnclosingElement().getSimpleName(),
                method.getSimpleName() );
    }

}
