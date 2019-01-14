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
package org.neo4j.tooling.procedure.testutils;

import com.google.testing.compile.CompilationRule;

import java.util.stream.Stream;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.ElementFilter;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;

import static javax.lang.model.util.ElementFilter.fieldsIn;

public class ElementTestUtils
{

    private final Elements elements;
    private final Types types;
    private final TypeMirrorTestUtils typeMirrorTestUtils;

    public ElementTestUtils( CompilationRule rule )
    {
        this( rule.getElements(), rule.getTypes(), new TypeMirrorTestUtils( rule ) );
    }

    private ElementTestUtils( Elements elements, Types types, TypeMirrorTestUtils typeMirrorTestUtils )
    {
        this.elements = elements;
        this.types = types;
        this.typeMirrorTestUtils = typeMirrorTestUtils;
    }

    public Stream<VariableElement> getFields( Class<?> type )
    {
        TypeElement procedure = elements.getTypeElement( type.getName() );

        return fieldsIn( procedure.getEnclosedElements() ).stream();
    }

    public Element findMethodElement( Class<?> type, String methodName )
    {
        TypeMirror mirror = typeMirrorTestUtils.typeOf( type );
        return ElementFilter.methodsIn( types.asElement( mirror ).getEnclosedElements() ).stream()
                .filter( method -> method.getSimpleName().contentEquals( methodName ) ).findFirst().orElseThrow(
                        () -> new AssertionError(
                                String.format( "Could not find method %s of class %s", methodName, type.getName() ) ) );
    }
}
