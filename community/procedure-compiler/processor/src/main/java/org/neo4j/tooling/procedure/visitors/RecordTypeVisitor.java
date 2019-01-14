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
import javax.lang.model.element.Element;
import javax.lang.model.element.Modifier;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.type.TypeVisitor;
import javax.lang.model.util.SimpleTypeVisitor8;
import javax.lang.model.util.Types;

import org.neo4j.tooling.procedure.compilerutils.TypeMirrorUtils;
import org.neo4j.tooling.procedure.messages.CompilationMessage;
import org.neo4j.tooling.procedure.messages.RecordTypeError;

import static javax.lang.model.element.Modifier.PUBLIC;
import static javax.lang.model.element.Modifier.STATIC;
import static javax.lang.model.util.ElementFilter.fieldsIn;

class RecordTypeVisitor extends SimpleTypeVisitor8<Stream<CompilationMessage>,Void>
{

    private final Types typeUtils;
    private final TypeVisitor<Boolean,Void> fieldTypeVisitor;

    RecordTypeVisitor( Types typeUtils, TypeMirrorUtils typeMirrors )
    {
        this.typeUtils = typeUtils;
        fieldTypeVisitor = new RecordFieldTypeVisitor( typeUtils, typeMirrors );
    }

    @Override
    public Stream<CompilationMessage> visitDeclared( DeclaredType returnType, Void ignored )
    {
        return returnType.getTypeArguments().stream().flatMap( this::validateRecord );
    }

    private Stream<CompilationMessage> validateRecord( TypeMirror recordType )
    {
        Element recordElement = typeUtils.asElement( recordType );
        return Stream.concat( validateFieldModifiers( recordElement ), validateFieldType( recordElement ) );
    }

    private Stream<CompilationMessage> validateFieldModifiers( Element recordElement )
    {
        return fieldsIn( recordElement.getEnclosedElements() ).stream().filter( element ->
        {
            Set<Modifier> modifiers = element.getModifiers();
            return !modifiers.contains( PUBLIC ) && !modifiers.contains( STATIC );
        } ).map( element -> new RecordTypeError( element, "Record definition error: field %s#%s must be public",
                recordElement.getSimpleName(), element.getSimpleName() ) );
    }

    private Stream<CompilationMessage> validateFieldType( Element recordElement )
    {
        return fieldsIn( recordElement.getEnclosedElements() ).stream()
                .filter( element -> !element.getModifiers().contains( STATIC ) )
                .filter( element -> !fieldTypeVisitor.visit( element.asType() ) )
                .map( element -> new RecordTypeError( element,
                        "Record definition error: type of field %s#%s is not supported", recordElement.getSimpleName(),
                        element.getSimpleName() ) );
    }

}
