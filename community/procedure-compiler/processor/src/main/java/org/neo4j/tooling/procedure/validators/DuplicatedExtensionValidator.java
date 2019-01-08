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
package org.neo4j.tooling.procedure.validators;

import org.neo4j.tooling.procedure.messages.CompilationMessage;
import org.neo4j.tooling.procedure.messages.DuplicatedProcedureError;
import org.neo4j.tooling.procedure.visitors.AnnotationTypeVisitor;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;
import javax.lang.model.util.Elements;

import org.neo4j.procedure.Procedure;

import static java.util.stream.Collectors.groupingBy;

/**
 * Validates that a given extension name is not declared by multiple elements annotated with the same annotation of type
 * {@code T}.
 * This validation is done within an annotation processor. This means that the detection is detected only per
 * compilation unit, not per Neo4j instance.
 *
 * Indeed, a Neo4j instance can aggregate several extension JARs and its duplication detection cannot be entirely
 * replaced by this.
 *
 * @param <T> annotation type
 */
public class DuplicatedExtensionValidator<T extends Annotation>
        implements Function<Collection<Element>,Stream<CompilationMessage>>
{

    private final Elements elements;
    private final Class<T> annotationType;
    private final Function<T,Optional<String>> customNameExtractor;

    public DuplicatedExtensionValidator( Elements elements, Class<T> annotationType,
            Function<T,Optional<String>> customNameExtractor )
    {
        this.elements = elements;
        this.annotationType = annotationType;
        this.customNameExtractor = customNameExtractor;
    }

    @Override
    public Stream<CompilationMessage> apply( Collection<Element> visitedProcedures )
    {
        return findDuplicates( visitedProcedures );
    }

    private Stream<CompilationMessage> findDuplicates( Collection<Element> visitedProcedures )
    {
        return indexByName( visitedProcedures ).filter( index -> index.getValue().size() > 1 )
                .flatMap( this::asErrors );
    }

    private Stream<Map.Entry<String,List<Element>>> indexByName( Collection<Element> visitedProcedures )
    {
        return visitedProcedures.stream().collect( groupingBy( this::getName ) ).entrySet().stream();
    }

    private String getName( Element procedure )
    {
        T annotation = procedure.getAnnotation( annotationType );
        Optional<String> customName = customNameExtractor.apply( annotation );
        return customName.orElse( defaultQualifiedName( procedure ) );
    }

    private String defaultQualifiedName( Element procedure )
    {
        return String.format( "%s.%s", elements.getPackageOf( procedure ).toString(), procedure.getSimpleName() );
    }

    private Stream<CompilationMessage> asErrors( Map.Entry<String,List<Element>> indexedProcedures )
    {
        String duplicatedName = indexedProcedures.getKey();
        return indexedProcedures.getValue().stream()
                .map( procedure -> asError( procedure, duplicatedName, indexedProcedures.getValue().size() ) );
    }

    private CompilationMessage asError( Element procedure, String duplicatedName, int duplicateCount )
    {
        return new DuplicatedProcedureError( procedure, getAnnotationMirror( procedure ),
                "Procedure|function name <%s> is already defined %s times. It should be defined only once!",
                duplicatedName, String.valueOf( duplicateCount ) );
    }

    private AnnotationMirror getAnnotationMirror( Element procedure )
    {
        return procedure.getAnnotationMirrors().stream().filter( this::isProcedureAnnotationType ).findFirst()
                .orElse( null );
    }

    private boolean isProcedureAnnotationType( AnnotationMirror mirror )
    {
        return new AnnotationTypeVisitor( Procedure.class ).visit( mirror.getAnnotationType().asElement() );
    }

}
