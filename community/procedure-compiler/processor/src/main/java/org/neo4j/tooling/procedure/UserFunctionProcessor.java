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
package org.neo4j.tooling.procedure;

import com.google.auto.service.AutoService;
import org.neo4j.tooling.procedure.compilerutils.TypeMirrorUtils;
import org.neo4j.tooling.procedure.messages.CompilationMessage;
import org.neo4j.tooling.procedure.messages.MessagePrinter;
import org.neo4j.tooling.procedure.validators.DuplicatedProcedureValidator;
import org.neo4j.tooling.procedure.visitors.UserFunctionVisitor;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementVisitor;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;

import org.neo4j.procedure.UserFunction;

@AutoService( Processor.class )
public class UserFunctionProcessor extends AbstractProcessor
{
    private static final Class<UserFunction> userFunctionType = UserFunction.class;
    private final Set<Element> visitedFunctions = new LinkedHashSet<>();

    private ElementVisitor<Stream<CompilationMessage>,Void> visitor;
    private MessagePrinter messagePrinter;
    private Function<Collection<Element>,Stream<CompilationMessage>> duplicationPredicate;

    public static Optional<String> getCustomName( UserFunction function )
    {
        String name = function.name();
        if ( !name.isEmpty() )
        {
            return Optional.of( name );
        }
        String value = function.value();
        if ( !value.isEmpty() )
        {
            return Optional.of( value );
        }
        return Optional.empty();
    }

    @Override
    public Set<String> getSupportedAnnotationTypes()
    {
        Set<String> types = new HashSet<>();
        types.add( userFunctionType.getName() );
        return types;
    }

    @Override
    public SourceVersion getSupportedSourceVersion()
    {
        return SourceVersion.RELEASE_8;
    }

    @Override
    public synchronized void init( ProcessingEnvironment processingEnv )
    {
        super.init( processingEnv );
        Types typeUtils = processingEnv.getTypeUtils();
        Elements elementUtils = processingEnv.getElementUtils();

        visitedFunctions.clear();
        messagePrinter = new MessagePrinter( processingEnv.getMessager() );
        visitor = new UserFunctionVisitor( typeUtils, elementUtils, new TypeMirrorUtils( typeUtils, elementUtils ) );
        duplicationPredicate = new DuplicatedProcedureValidator<>( elementUtils, userFunctionType,
                UserFunctionProcessor::getCustomName );
    }

    @Override
    public boolean process( Set<? extends TypeElement> annotations, RoundEnvironment roundEnv )
    {
        processElements( roundEnv );
        if ( roundEnv.processingOver() )
        {
            duplicationPredicate.apply( visitedFunctions ).forEach( messagePrinter::print );
        }
        return false;
    }

    private void processElements( RoundEnvironment roundEnv )
    {
        Set<? extends Element> functions = roundEnv.getElementsAnnotatedWith( userFunctionType );
        visitedFunctions.addAll( functions );
        functions.stream().flatMap( this::validate ).forEachOrdered( messagePrinter::print );
    }

    private Stream<CompilationMessage> validate( Element element )
    {
        return visitor.visit( element );
    }
}
