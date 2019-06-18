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
package org.neo4j.annotations.service;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.api.multimap.MutableMultimap;
import org.eclipse.collections.impl.factory.Multimaps;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.FileObject;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static javax.tools.Diagnostic.Kind.ERROR;
import static javax.tools.Diagnostic.Kind.NOTE;
import static javax.tools.StandardLocation.CLASS_OUTPUT;
import static org.apache.commons.lang3.StringUtils.substringBefore;
import static org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace;
import static org.eclipse.collections.impl.set.mutable.UnifiedSet.newSetWith;

/**
 * Handles {@link Service} and {@link ServiceProvider} annotations. For each service type it collects associated service providers and creates
 * corresponding configuration file in {@code /META-INF/services/}.
 */
public class ServiceAnnotationProcessor extends AbstractProcessor
{
    private final MutableMultimap<TypeElement, TypeElement> serviceProviders = Multimaps.mutable.list.empty();
    private Types typeUtils;
    private Elements elementUtils;

    @Override
    public synchronized void init( ProcessingEnvironment processingEnv )
    {
        super.init( processingEnv );
        typeUtils = processingEnv.getTypeUtils();
        elementUtils = processingEnv.getElementUtils();
    }

    @Override
    public Set<String> getSupportedAnnotationTypes()
    {
        return newSetWith( ServiceProvider.class.getName() );
    }

    @Override
    public SourceVersion getSupportedSourceVersion()
    {
        return SourceVersion.latestSupported();
    }

    @Override
    public boolean process( Set<? extends TypeElement> annotations, RoundEnvironment roundEnv )
    {
        try
        {
            if ( roundEnv.processingOver() )
            {
                if ( !roundEnv.errorRaised() )
                {
                    generateConfigs();
                }
            }
            else
            {
                scan( roundEnv );
            }
        }
        catch ( Exception e )
        {
            error( "Service annotation processor failed", e );
        }
        return false;
    }

    private void scan( RoundEnvironment roundEnv )
    {
        final Set<TypeElement> elements = roundEnv.getElementsAnnotatedWith( ServiceProvider.class ).stream().map( TypeElement.class::cast ).collect( toSet() );
        info( "Processing service providers: " + elements.stream().map( Object::toString ).sorted( ).collect( toList() ) );
        for ( TypeElement serviceProvider : elements )
        {
            getImplementedService( serviceProvider ).ifPresent( service ->
            {
                info( format( "Service %s provided by %s", service, serviceProvider ) );
                serviceProviders.put( service, serviceProvider );
            } );
        }
    }

    private Optional<TypeElement> getImplementedService( TypeElement serviceProvider )
    {
        final Set<TypeMirror> types = getTypeWithSupertypes( serviceProvider.asType() );
        final List<TypeMirror> services = types.stream().filter( this::isService ).collect( toList() );

        if ( services.isEmpty() )
        {
            error( format( "Service provider %s neither has ascendants nor itself annotated with @Service)", serviceProvider ), serviceProvider );
            return Optional.empty();
        }

        if ( services.size() > 1 )
        {
            error( format( "Service provider %s has multiple ascendants annotated with @Service: %s", serviceProvider, services ), serviceProvider );
            return Optional.empty();
        }

        return Optional.of( (TypeElement) typeUtils.asElement( services.get( 0 ) ) );
    }

    private boolean isService( TypeMirror type )
    {
        return typeUtils.asElement( type ).getAnnotation( Service.class ) != null;
    }

    private Set<TypeMirror> getTypeWithSupertypes( TypeMirror type )
    {
        final Set<TypeMirror> allTypes = new HashSet<>();
        allTypes.add( type );
        final List<? extends TypeMirror> directSupertypes = typeUtils.directSupertypes( type );
        directSupertypes.forEach( directSupertype -> allTypes.addAll( getTypeWithSupertypes( directSupertype ) ) );
        return allTypes;
    }

    private void generateConfigs() throws IOException
    {
        for ( final TypeElement service : serviceProviders.keySet() )
        {
            final String path = "META-INF/services/" + elementUtils.getBinaryName( service ).toString();
            info( "Generating service config file: " + path );

            final SortedSet<String> oldProviders = loadIfExists( path );
            final SortedSet<String> newProviders = new TreeSet<>();

            serviceProviders.get( service ).forEach( providerType ->
            {
                final String providerName = elementUtils.getBinaryName( providerType ).toString();
                newProviders.add( providerName );
            } );

            if ( oldProviders.containsAll( newProviders ) )
            {
                info( "No new service providers found" );
                return;
            }
            newProviders.addAll( oldProviders );

            final FileObject file = processingEnv.getFiler().createResource( CLASS_OUTPUT, "", path );
            try ( Writer writer = file.openWriter();
                  BufferedWriter out = new BufferedWriter( writer ) )
            {
                info( "Writing service providers: " + newProviders );
                for ( final String provider : newProviders )
                {
                    out.write( provider );
                    out.write( "\n" );
                }
            }
        }
    }

    private SortedSet<String> loadIfExists( String path )
    {
        final SortedSet<String> result = new TreeSet<>();
        try
        {
            final FileObject file = processingEnv.getFiler().getResource( CLASS_OUTPUT, "", path );
            final List<String> lines = new ArrayList<>();
            try ( InputStream is = file.openInputStream();
                  BufferedReader in = new BufferedReader( new InputStreamReader( is ) ) )
            {
                String line;
                while ( (line = in.readLine()) != null )
                {
                    lines.add( line );
                }
            }
            lines.stream()
                    .map( s -> substringBefore( s, "#" ) )
                    .map( String::trim )
                    .filter( StringUtils::isNotEmpty )
                    .forEach( result::add );
            info( "Loaded existing providers: " + result );
        }
        catch ( IOException ignore )
        {
            info( "No existing providers loaded" );
        }
        return result;
    }

    private void info( String msg )
    {
        processingEnv.getMessager().printMessage( NOTE, msg );
    }

    private void error( String msg, Exception e )
    {
        processingEnv.getMessager().printMessage( ERROR, msg + ": " + getStackTrace( e ) );
    }

    private void error( String msg, Element element )
    {
        processingEnv.getMessager().printMessage( ERROR, msg, element );
    }
}
