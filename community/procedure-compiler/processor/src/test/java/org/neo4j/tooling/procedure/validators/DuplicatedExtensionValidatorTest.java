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
package org.neo4j.tooling.procedure.validators;

import com.google.testing.compile.CompilationRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collection;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.Elements;
import javax.tools.Diagnostic;

import org.neo4j.procedure.Procedure;
import org.neo4j.tooling.procedure.compilerutils.CustomNameExtractor;
import org.neo4j.tooling.procedure.messages.CompilationMessage;
import org.neo4j.tooling.procedure.validators.examples.DefaultProcedureA;
import org.neo4j.tooling.procedure.validators.examples.DefaultProcedureB;
import org.neo4j.tooling.procedure.validators.examples.OverriddenProcedureB;
import org.neo4j.tooling.procedure.validators.examples.override.OverriddenProcedureA;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;

public class DuplicatedExtensionValidatorTest
{

    @Rule
    public CompilationRule compilation = new CompilationRule();

    private Elements elements;
    private Function<Collection<Element>,Stream<CompilationMessage>> validator;

    @Before
    public void prepare()
    {
        elements = compilation.getElements();
        validator = new DuplicatedExtensionValidator<>( elements, Procedure.class,
                proc -> CustomNameExtractor.getName( proc::name, proc::value ) );
    }

    @Test
    public void detects_duplicate_procedure_with_default_names()
    {
        Element procedureA = procedureMethod( DefaultProcedureA.class.getName() );
        Element procedureB = procedureMethod( DefaultProcedureB.class.getName() );
        Collection<Element> duplicates = asList( procedureA, procedureB );

        Stream<CompilationMessage> errors = validator.apply( duplicates );

        String procedureName = "org.neo4j.tooling.procedure.validators.examples.procedure";
        assertThat( errors ).extracting( CompilationMessage::getCategory, CompilationMessage::getElement,
                CompilationMessage::getContents ).containsExactlyInAnyOrder( tuple( Diagnostic.Kind.ERROR, procedureA,
                "Procedure|function name <" + procedureName + "> is already defined 2 times. It should be defined " +
                        "only once!" ), tuple( Diagnostic.Kind.ERROR, procedureB,
                "Procedure|function name <" + procedureName +
                        "> is already defined 2 times. It should be defined only once!" ) );
    }

    @Test
    public void detects_duplicate_procedure_with_overridden_names()
    {
        Element procedureA = procedureMethod( OverriddenProcedureA.class.getName() );
        Element procedureB = procedureMethod( OverriddenProcedureB.class.getName() );
        Collection<Element> duplicates = asList( procedureA, procedureB );

        Stream<CompilationMessage> errors = validator.apply( duplicates );

        assertThat( errors ).extracting( CompilationMessage::getCategory, CompilationMessage::getElement,
                CompilationMessage::getContents ).containsExactlyInAnyOrder( tuple( Diagnostic.Kind.ERROR, procedureA,
                "Procedure|function name <override> is already defined 2 times. It should be defined only once!" ),
                tuple( Diagnostic.Kind.ERROR, procedureB,
                        "Procedure|function name <override> is already defined 2 times. It should be defined only " +
                                "once!" ) );
    }

    @Test
    public void does_not_detect_duplicates_if_duplicate_procedure_has_custom_name()
    {
        Collection<Element> duplicates = asList( procedureMethod( DefaultProcedureA.class.getName() ),
                procedureMethod( OverriddenProcedureB.class.getName() ) );

        Stream<CompilationMessage> errors = validator.apply( duplicates );

        assertThat( errors ).isEmpty();
    }

    private Element procedureMethod( String name )
    {
        TypeElement typeElement = elements.getTypeElement( name );
        Collection<Element> procedures = findProcedures( typeElement );
        if ( procedures.size() != 1 )
        {
            throw new AssertionError( "Test procedure class should only have 1 defined procedure" );
        }
        return procedures.iterator().next();
    }

    private Collection<Element> findProcedures( TypeElement typeElement )
    {
        return typeElement.getEnclosedElements().stream()
                .filter( element -> element.getAnnotation( Procedure.class ) != null )
                .collect( Collectors.<Element>toList() );
    }

}
