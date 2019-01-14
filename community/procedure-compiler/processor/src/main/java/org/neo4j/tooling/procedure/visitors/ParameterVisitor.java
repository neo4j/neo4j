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

import java.util.List;
import java.util.stream.Stream;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeVisitor;
import javax.lang.model.util.SimpleElementVisitor8;

import org.neo4j.procedure.Name;
import org.neo4j.tooling.procedure.messages.CompilationMessage;
import org.neo4j.tooling.procedure.messages.ParameterMissingAnnotationError;
import org.neo4j.tooling.procedure.messages.ParameterTypeError;

class ParameterVisitor extends SimpleElementVisitor8<Stream<CompilationMessage>,Void>
{

    private final TypeVisitor<Boolean,Void> parameterTypeVisitor;

    ParameterVisitor( TypeVisitor<Boolean,Void> parameterTypeVisitor )
    {
        this.parameterTypeVisitor = parameterTypeVisitor;
    }

    @Override
    public Stream<CompilationMessage> visitVariable( VariableElement parameter, Void ignored )
    {
        Name annotation = parameter.getAnnotation( Name.class );
        if ( annotation == null )
        {
            return Stream.of( new ParameterMissingAnnotationError( parameter,
                    annotationMirror( parameter.getAnnotationMirrors() ), "@%s usage error: missing on parameter <%s>",
                    Name.class.getName(), nameOf( parameter ) ) );
        }

        if ( !parameterTypeVisitor.visit( parameter.asType() ) )
        {
            Element method = parameter.getEnclosingElement();
            return Stream.of( new ParameterTypeError( parameter,
                    "Unsupported parameter type <%s> of " + "procedure|function" + " %s#%s",
                    parameter.asType().toString(), method.getEnclosingElement().getSimpleName(),
                    method.getSimpleName() ) );
        }
        return Stream.empty();
    }

    private AnnotationMirror annotationMirror( List<? extends AnnotationMirror> mirrors )
    {
        AnnotationTypeVisitor nameVisitor = new AnnotationTypeVisitor( Name.class );
        return mirrors.stream().filter( mirror -> nameVisitor.visit( mirror.getAnnotationType().asElement() ) )
                .findFirst().orElse( null );
    }

    private String nameOf( VariableElement parameter )
    {
        return parameter.getSimpleName().toString();
    }
}
