/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.tooling.procedure.messages.CompilationMessage;
import org.neo4j.tooling.procedure.messages.ContextFieldWarning;
import org.neo4j.tooling.procedure.messages.FieldError;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.SimpleElementVisitor8;
import javax.lang.model.util.Types;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;

class ContextFieldVisitor extends SimpleElementVisitor8<Stream<CompilationMessage>,Void>
{
    private static final Set<Class<?>> SUPPORTED_TYPES =
            new LinkedHashSet<>( Arrays.asList( GraphDatabaseService.class, Log.class ) );

    private final Elements elements;
    private final Types types;
    private final boolean skipContextWarnings;

    public ContextFieldVisitor( Types types, Elements elements, boolean skipContextWarnings )
    {
        this.elements = elements;
        this.types = types;
        this.skipContextWarnings = skipContextWarnings;
    }

    private static String types( Set<Class<?>> supportedTypes )
    {
        return supportedTypes.stream().map( Class::getName ).collect( Collectors.joining( ">, <", "<", ">" ) );
    }

    @Override
    public Stream<CompilationMessage> visitVariable( VariableElement field, Void ignored )
    {
        return Stream.concat( validateModifiers( field ), validateInjectedTypes( field ) );
    }

    private Stream<CompilationMessage> validateModifiers( VariableElement field )
    {
        if ( !hasValidModifiers( field ) )
        {
            return Stream.of( new FieldError( field,
                    "@%s usage error: field %s#%s should be public, non-static and non-final", Context.class.getName(),
                    field.getEnclosingElement().getSimpleName(), field.getSimpleName() ) );
        }

        return Stream.empty();
    }

    private Stream<CompilationMessage> validateInjectedTypes( VariableElement field )
    {
        if ( skipContextWarnings )
        {
            return Stream.empty();
        }

        TypeMirror fieldType = field.asType();
        if ( !injectsAllowedTypes( fieldType ) )
        {
            return Stream
                    .of( new ContextFieldWarning( field, "@%s usage warning: found type: <%s>, expected one of: %s",
                            Context.class.getName(), fieldType.toString(), types( SUPPORTED_TYPES ) ) );
        }

        return Stream.empty();
    }

    private boolean injectsAllowedTypes( TypeMirror fieldType )
    {
        return supportedTypeMirrors( SUPPORTED_TYPES ).filter( t -> types.isSameType( t, fieldType ) ).findAny()
                .isPresent();
    }

    private boolean hasValidModifiers( VariableElement field )
    {
        Set<Modifier> modifiers = field.getModifiers();
        return modifiers.contains( Modifier.PUBLIC ) && !modifiers.contains( Modifier.STATIC ) &&
                !modifiers.contains( Modifier.FINAL );
    }

    private Stream<TypeMirror> supportedTypeMirrors( Set<Class<?>> supportedTypes )
    {
        return supportedTypes.stream().map( c -> elements.getTypeElement( c.getName() ).asType() );
    }
}
