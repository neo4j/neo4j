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
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.lang.model.element.ElementVisitor;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;

import org.neo4j.tooling.procedure.compilerutils.TypeMirrorUtils;
import org.neo4j.tooling.procedure.messages.CompilationMessage;
import org.neo4j.tooling.procedure.messages.FunctionInRootNamespaceError;
import org.neo4j.tooling.procedure.messages.ReturnTypeError;
import org.neo4j.tooling.procedure.validators.AllowedTypesValidator;

public class FunctionVisitor<T extends Annotation>
{

    private final ElementVisitor<Stream<CompilationMessage>,Void> parameterVisitor;
    private final Elements elements;
    private final ElementVisitor<Stream<CompilationMessage>,Void> classVisitor;
    private final Function<T,Optional<String>> customNameExtractor;
    private final Class<T> annotationType;
    private final AllowedTypesValidator allowedTypesValidator;

    public FunctionVisitor( Class<T> annotationType, Types types, Elements elements, TypeMirrorUtils typeMirrorUtils,
            Function<T,Optional<String>> customNameExtractor, boolean ignoresWarnings )
    {
        this.customNameExtractor = customNameExtractor;
        this.annotationType = annotationType;
        this.classVisitor = new ExtensionClassVisitor( types, elements, ignoresWarnings );
        this.parameterVisitor = new ParameterVisitor( new ParameterTypeVisitor( types, typeMirrorUtils ) );
        this.elements = elements;
        this.allowedTypesValidator = new AllowedTypesValidator( typeMirrorUtils, types );
    }

    public Stream<CompilationMessage> validateEnclosingClass( ExecutableElement method )
    {
        return classVisitor.visit( method.getEnclosingElement() );
    }

    public Stream<CompilationMessage> validateParameters( List<? extends VariableElement> parameters )
    {
        return parameters.stream().flatMap( parameterVisitor::visit );
    }

    public Stream<CompilationMessage> validateName( ExecutableElement method )
    {
        Optional<String> customName = customNameExtractor.apply( method.getAnnotation( annotationType ) );
        if ( customName.isPresent() )
        {
            if ( isInRootNamespace( customName.get() ) )
            {
                return Stream.of( rootNamespaceError( method, customName.get() ) );
            }
            return Stream.empty();
        }

        PackageElement namespace = elements.getPackageOf( method );
        if ( namespace == null )
        {
            return Stream.of( rootNamespaceError( method ) );
        }
        return Stream.empty();
    }

    public Stream<CompilationMessage> validateReturnType( ExecutableElement method )
    {
        TypeMirror returnType = method.getReturnType();
        if ( !allowedTypesValidator.test( returnType ) )
        {
            return Stream.of( new ReturnTypeError( method,
                    "Unsupported return type <%s> of function defined in <%s#%s>.", returnType,
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
