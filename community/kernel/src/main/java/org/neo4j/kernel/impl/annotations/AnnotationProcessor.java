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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic.Kind;
import javax.tools.FileObject;
import javax.tools.StandardLocation;

import static org.neo4j.io.fs.FileUtils.newFilePrintWriter;
import static org.neo4j.kernel.impl.util.Charsets.UTF_8;

public abstract class AnnotationProcessor extends AbstractProcessor
{
    @Override
    public boolean process( Set<? extends TypeElement> annotations, RoundEnvironment roundEnv )
    {
        for ( TypeElement type : annotations )
        {
            for ( Element annotated : roundEnv.getElementsAnnotatedWith( type ) )
            {
                for ( AnnotationMirror mirror : annotated.getAnnotationMirrors() )
                {
                    if ( mirror.getAnnotationType().asElement().equals( type ) )
                    {
                        try
                        {
                            process( type, annotated, mirror, processingEnv.getElementUtils()
                                    .getElementValuesWithDefaults( mirror ) );
                        }
                        catch ( Exception e )
                        {
                            e.printStackTrace();
                            processingEnv.getMessager().printMessage( Kind.ERROR, "Internal error: " + e.toString(),
                                    annotated, mirror );
                        }
                    }
                }
            }
        }
        return false;
    }

    protected final void warn( Element element, String message )
    {
        processingEnv.getMessager().printMessage( Kind.WARNING, message, element );
    }

    protected final void warn( Element element, AnnotationMirror annotation, String message )
    {
        processingEnv.getMessager().printMessage( Kind.WARNING, message, element, annotation );
    }

    protected final void error( Element element, String message )
    {
        processingEnv.getMessager().printMessage( Kind.ERROR, message, element );
    }

    protected final void error( Element element, AnnotationMirror annotation, String message )
    {
        processingEnv.getMessager().printMessage( Kind.ERROR, message, element, annotation );
    }

    protected abstract void process( TypeElement annotationType, Element annotated, AnnotationMirror annotation,
            Map<? extends ExecutableElement, ? extends AnnotationValue> values ) throws IOException;

    private static Pattern nl = Pattern.compile( "\n" );

    void addTo( String line, String... path ) throws IOException
    {
        FileObject fo = processingEnv.getFiler().getResource( StandardLocation.CLASS_OUTPUT, "", path( path ) );
        URI uri = fo.toUri();
        File file;
        try
        {
            file = new File( uri );
        }
        catch ( Exception e )
        {
            file = new File( uri.toString() );
        }
        if ( file.exists() )
        {
            for ( String previous : nl.split( fo.getCharContent( true ), 0 ) )
            {
                if ( line.equals( previous ) )
                {
                    return;
                }
            }
        }
        else
        {
            file.getParentFile().mkdirs();
        }

        newFilePrintWriter( file, UTF_8 ).append( line ).append( "\n" ).close();
    }

    private String path( String[] path )
    {
        StringBuilder filename = new StringBuilder();
        String sep = "";
        for ( String part : path )
        {
            filename.append( sep ).append( part );
            sep = "/";
        }
        return filename.toString();
    }
}
