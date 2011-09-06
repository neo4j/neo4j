/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
import java.util.Map;

import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;

@SupportedSourceVersion( SourceVersion.RELEASE_6 )
@SupportedAnnotationTypes( "org.neo4j.kernel.impl.annotations.Documented" )
public class DocumentationProcessor extends AnnotationProcessor
{
    private static final String DEFAULT_VALUE;
    static
    {
        String defaultValue = "";
        try
        {
            defaultValue = (String) Documented.class.getMethod( "value" ).getDefaultValue();
        }
        catch ( Exception e )
        {
            // OK
        }
        DEFAULT_VALUE = defaultValue;
    }
    private CompilationManipulator manipulator = null;

    @Override
    public synchronized void init( ProcessingEnvironment processingEnv )
    {
        super.init( processingEnv );
        manipulator = CompilationManipulator.load( processingEnv );
    }

    @Override
    void process( TypeElement annotationType, Element annotated, AnnotationMirror annotation,
            Map<? extends ExecutableElement, ? extends AnnotationValue> values ) throws IOException
    {
        if ( values.size() != 1 )
        {
            warn( annotated, annotation, "Annotation values don't match the expectation" );
            return;
        }
        String value = (String) values.values().iterator().next().getValue();
        if ( DEFAULT_VALUE.equals( value ) || value == null )
        {
            if ( manipulator == null )
            {
                warn( annotated, annotation, "Cannot update annotation values for this compiler" );
                return;
            }
            String javadoc = processingEnv.getElementUtils().getDocComment( annotated );
            if ( javadoc == null )
            {
                warn( annotated, annotation, "Cannot extract JavaDoc documentation comment for " + annotated );
                // return no period, since that could mess up Title generation;
                javadoc = "Documentation not available";
            }
            if ( !manipulator.updateAnnotationValue( annotated, annotation, "value", javadoc ) )
            {
                warn( annotated, annotation, "Failed to update annotation value" );
            }
        }
    }
}
