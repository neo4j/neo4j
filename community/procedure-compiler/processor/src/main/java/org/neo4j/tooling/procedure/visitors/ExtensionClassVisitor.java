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

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.Elements;
import javax.lang.model.util.SimpleElementVisitor8;
import javax.lang.model.util.Types;

import org.neo4j.tooling.procedure.messages.CompilationMessage;
import org.neo4j.tooling.procedure.messages.ExtensionMissingPublicNoArgConstructor;

import static javax.lang.model.util.ElementFilter.constructorsIn;

public class ExtensionClassVisitor extends SimpleElementVisitor8<Stream<CompilationMessage>,Void>
{

    private final Set<TypeElement> visitedElements = new HashSet<>();
    private final FieldVisitor fieldVisitor;

    public ExtensionClassVisitor( Types types, Elements elements, boolean ignoresWarnings )
    {
        fieldVisitor = new FieldVisitor( types, elements, ignoresWarnings );
    }

    @Override
    public Stream<CompilationMessage> visitType( TypeElement extensionClass, Void ignored )
    {
        if ( isFirstVisit( extensionClass ) )
        {
            return Stream.concat( validateFields( extensionClass ), validateConstructor( extensionClass ) );
        }
        return Stream.empty();
    }

    /**
     * Check if the {@link TypeElement} is visited for the first time. A {@link TypeElement} will be visited once per
     * procedure it contains, but it only needs to be validated once.
     *
     * @param e The visited {@link TypeElement}
     * @return true for the first visit of the {@link TypeElement}, false afterwards
     */
    private boolean isFirstVisit( TypeElement e )
    {
        return visitedElements.add( e );
    }

    private Stream<CompilationMessage> validateFields( TypeElement e )
    {
        return e.getEnclosedElements().stream().flatMap( fieldVisitor::visit );
    }

    private Stream<CompilationMessage> validateConstructor( Element extensionClass )
    {
        Optional<ExecutableElement> publicNoArgConstructor =
                constructorsIn( extensionClass.getEnclosedElements() ).stream()
                        .filter( c -> c.getModifiers().contains( Modifier.PUBLIC ) )
                        .filter( c -> c.getParameters().isEmpty() ).findFirst();

        if ( !publicNoArgConstructor.isPresent() )
        {
            return Stream.of( new ExtensionMissingPublicNoArgConstructor( extensionClass,
                    "Extension class %s should contain a public no-arg constructor, none found.", extensionClass ) );
        }
        return Stream.empty();
    }
}
