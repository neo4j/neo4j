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

import java.util.Set;
import java.util.stream.Stream;
import javax.lang.model.element.ElementVisitor;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.VariableElement;
import javax.lang.model.util.Elements;
import javax.lang.model.util.SimpleElementVisitor8;
import javax.lang.model.util.Types;

import org.neo4j.procedure.Context;
import org.neo4j.tooling.procedure.messages.CompilationMessage;
import org.neo4j.tooling.procedure.messages.FieldError;

public class FieldVisitor extends SimpleElementVisitor8<Stream<CompilationMessage>,Void>
{

    private final ElementVisitor<Stream<CompilationMessage>,Void> contextFieldVisitor;

    public FieldVisitor( Types types, Elements elements, boolean ignoresWarnings )
    {
        contextFieldVisitor = new ContextFieldVisitor( types, elements, ignoresWarnings );
    }

    private static Stream<CompilationMessage> validateNonContextField( VariableElement field )
    {
        Set<Modifier> modifiers = field.getModifiers();
        if ( !modifiers.contains( Modifier.STATIC ) )
        {
            return Stream.of( new FieldError( field, "Field %s#%s should be static",
                    field.getEnclosingElement().getSimpleName(), field.getSimpleName() ) );
        }
        return Stream.empty();
    }

    @Override
    public Stream<CompilationMessage> visitVariable( VariableElement field, Void ignored )
    {
        if ( field.getAnnotation( Context.class ) != null )
        {
            return contextFieldVisitor.visitVariable( field, ignored );
        }
        return validateNonContextField( field );

    }

}
