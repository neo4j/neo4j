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

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementVisitor;
import javax.lang.model.element.TypeElement;

import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;
import org.neo4j.procedure.UserFunction;
import org.neo4j.tooling.procedure.messages.CompilationMessage;
import org.neo4j.tooling.procedure.messages.MessagePrinter;
import org.neo4j.tooling.procedure.validators.DuplicatedExtensionValidator;

import static org.neo4j.tooling.procedure.CompilerOptions.IGNORE_CONTEXT_WARNINGS_OPTION;

/**
 * Base processor that processes {@link Element} annotated with {@code T}.
 * It also detects and reports duplicated elements (duplication can obviously be detected within a compilation unit and
 * not globally per Neo4j instance, as explained in {@link DuplicatedExtensionValidator}.
 *
 * @param <T> processed annotation type
 */
public class DuplicationAwareBaseProcessor<T extends Annotation> extends AbstractProcessor
{
    private final Set<Element> visitedElements = new LinkedHashSet<>();
    private final Class<T> supportedAnnotationType;
    private final Function<T,Optional<String>> customNameFunction;
    private final Function<ProcessingEnvironment,ElementVisitor<Stream<CompilationMessage>,Void>> visitorSupplier;

    private Function<Collection<Element>,Stream<CompilationMessage>> duplicationValidator;
    private ElementVisitor<Stream<CompilationMessage>,Void> visitor;
    private MessagePrinter messagePrinter;

    /**
     * Base initialization of Neo4j extension processor (where extension can be {@link Procedure}, {@link UserFunction},
     * {@link UserAggregationFunction}).
     *
     * @param supportedAnnotationType main annotation type supported by the processor. The main annotation may depend on
     * other annotations (e.g. {@link UserAggregationFunction} works with {@link UserAggregationResult} and
     * {@link UserAggregationUpdate}).
     * However, by design, these auxiliary annotations are processed by traversing the
     * element graph, rather than by standalone annotation processors.
     * @param customNameFunction function allowing to extract the custom simple name of the annotated element
     * @param visitorSupplier supplies the main {@link ElementVisitor} class in charge of traversing and validating the
     * annotated elements
     */
    public DuplicationAwareBaseProcessor( Class<T> supportedAnnotationType, Function<T,Optional<String>> customNameFunction,
            Function<ProcessingEnvironment,ElementVisitor<Stream<CompilationMessage>,Void>> visitorSupplier )
    {
        this.supportedAnnotationType = supportedAnnotationType;
        this.customNameFunction = customNameFunction;
        this.visitorSupplier = visitorSupplier;
    }

    @Override
    public synchronized void init( ProcessingEnvironment processingEnv )
    {
        super.init( processingEnv );

        messagePrinter = new MessagePrinter( processingEnv.getMessager() );
        duplicationValidator =
                new DuplicatedExtensionValidator<>( processingEnv.getElementUtils(), supportedAnnotationType, customNameFunction );
        visitor = visitorSupplier.apply( processingEnv );
    }

    @Override
    public Set<String> getSupportedOptions()
    {
        return Collections.singleton( IGNORE_CONTEXT_WARNINGS_OPTION );
    }

    @Override
    public Set<String> getSupportedAnnotationTypes()
    {
        return Collections.singleton( supportedAnnotationType.getName() );
    }

    @Override
    public SourceVersion getSupportedSourceVersion()
    {
        return SourceVersion.latestSupported();
    }

    @Override
    public boolean process( Set<? extends TypeElement> annotations, RoundEnvironment roundEnv )
    {
        processElements( roundEnv );
        if ( roundEnv.processingOver() )
        {
            duplicationValidator.apply( visitedElements ).forEach( messagePrinter::print );
        }
        return false;
    }

    private void processElements( RoundEnvironment roundEnv )
    {
        Set<? extends Element> functions = roundEnv.getElementsAnnotatedWith( supportedAnnotationType );
        visitedElements.addAll( functions );
        functions.stream().flatMap( this::validate ).forEachOrdered( messagePrinter::print );
    }

    private Stream<CompilationMessage> validate( Element element )
    {
        return visitor.visit( element );
    }
}
