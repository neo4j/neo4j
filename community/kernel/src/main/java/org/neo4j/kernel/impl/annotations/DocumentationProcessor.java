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
import java.io.Writer;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import javax.annotation.processing.SupportedAnnotationTypes;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;

@SupportedAnnotationTypes( "org.neo4j.kernel.impl.annotations.Documented" )
public class DocumentationProcessor extends AnnotationProcessor
{
    @Override
    void process( TypeElement annotation, Element annotated,
            Map<? extends ExecutableElement, ? extends AnnotationValue> values ) throws IOException
    {
        Element enclosing = annotated.getEnclosingElement();
        if ( enclosing instanceof TypeElement )
        {
            TypeElement type = (TypeElement) enclosing;
            System.out.println( Arrays.toString( annotated.getClass().getInterfaces() ) );
            System.out.println( "CLASS DOC: " + processingEnv.getElementUtils().getDocComment( type ) );
            String javadoc = processingEnv.getElementUtils().getDocComment( annotated );
            if ( javadoc == null )
                throw new IllegalStateException( "The field \"" + annotated + "\" should be documented." );
            Properties props = new Properties();
            props.setProperty( annotated.getSimpleName().toString(), javadoc );
            Writer writer = append( "META-INF", "documentation", type.getQualifiedName().toString() );
            try
            {
                props.store( writer, null );
            }
            finally
            {
                writer.close();
            }
        }
    }
}
