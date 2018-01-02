/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.annotations;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;

@SupportedSourceVersion( SourceVersion.RELEASE_7 )
@SupportedAnnotationTypes( "org.neo4j.helpers.Service.Implementation" )
public class ServiceProcessor extends AnnotationProcessor
{
    @SuppressWarnings( "unchecked" )
    @Override
    protected void process( TypeElement annotationType, Element annotated, AnnotationMirror annotation,
            Map<? extends ExecutableElement, ? extends AnnotationValue> values ) throws IOException
    {
        for ( AnnotationValue o : (List<? extends AnnotationValue>) values.values().iterator().next().getValue() )
        {
            TypeMirror service = (TypeMirror) o.getValue();
            addTo( ( (TypeElement) annotated ).getQualifiedName().toString(), "META-INF", "services",
                    service.toString() );
        }
    }
}
