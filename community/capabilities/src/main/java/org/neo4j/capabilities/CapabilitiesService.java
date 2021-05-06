/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.capabilities;

import org.apache.commons.lang3.reflect.FieldUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import org.neo4j.annotations.Description;
import org.neo4j.annotations.Public;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.service.Services;

import static java.lang.String.format;

public class CapabilitiesService extends LifecycleAdapter implements CapabilitiesRegistry
{
    private final Map<Name,CapabilityInstance<?>> capabilities;
    private final Collection<CapabilityProvider> capabilityProviders;

    CapabilitiesService( @Nonnull Collection<Class<? extends CapabilityDeclaration>> declarationClasses,
                         @Nonnull Collection<CapabilityProvider> capabilityProviders )
    {
        this.capabilities =
                getDeclaredCapabilities( Objects.requireNonNull( declarationClasses ) )
                        .entrySet()
                        .stream()
                        .collect( Collectors.toMap( Map.Entry::getKey, e -> new CapabilityInstance<>( e.getValue() ) ) );
        this.capabilityProviders = Objects.requireNonNull( capabilityProviders );
    }

    public Collection<Capability<?>> declaredCapabilities()
    {
        return capabilities.values().stream().map( CapabilityInstance::capability ).collect( Collectors.toList() );
    }

    @Override
    @SuppressWarnings( "unchecked" )
    public <T> T get( @Nonnull Capability<T> capability )
    {
        var instance = (CapabilityInstance<T>) capabilities.getOrDefault( capability.name(), null );
        if ( instance == null )
        {
            return null;
        }
        return instance.get();
    }

    @Override
    public Object get( @Nonnull Name name )
    {
        var instance = capabilities.getOrDefault( name, null );
        if ( instance == null )
        {
            return null;
        }
        return instance.get();
    }

    @Override
    @SuppressWarnings( "unchecked" )
    public <T> void set( @Nonnull Capability<T> capability, @Nonnull T value )
    {
        var instance = (CapabilityInstance<T>) capabilities.getOrDefault( capability.name(), null );
        if ( instance == null )
        {
            throw new IllegalArgumentException( String.format( "unknown capability %s", capability.name() ) );
        }
        instance.set( value );
    }

    @Override
    @SuppressWarnings( "unchecked" )
    public <T> void supply( @Nonnull Capability<T> capability, @Nonnull Supplier<T> dynamicValue )
    {
        var instance = (CapabilityInstance<T>) capabilities.getOrDefault( capability.name(), null );
        if ( instance == null )
        {
            throw new IllegalArgumentException( String.format( "unknown capability %s", capability.name() ) );
        }
        instance.supply( dynamicValue );
    }

    public Capabilities unmodifiable()
    {
        return new UnmodifiableCapabilities();
    }

    void processProviders()
    {
        capabilityProviders.forEach( p -> p.register( new NamespaceAwareCapabilityRegistry( p.namespace() ) ) );
    }

    @Override
    public void start() throws Exception
    {
        processProviders();
    }

    // A pure Capabilities implementation without allowing the caller to cast it into a CapabilityRegistry instance.
    private class UnmodifiableCapabilities implements Capabilities
    {
        @Override
        public <T> T get( @Nonnull Capability<T> capability )
        {
            return CapabilitiesService.this.get( capability );
        }

        @Override
        public Object get( @Nonnull Name name )
        {
            return CapabilitiesService.this.get( name );
        }
    }

    // A filtering CapabilitiesRegistry that prevents manipulation of out of namespace capabilities
    private class NamespaceAwareCapabilityRegistry implements CapabilitiesRegistry
    {
        private final String namespace;

        private NamespaceAwareCapabilityRegistry( @Nonnull String namespace )
        {
            this.namespace = Objects.requireNonNull( namespace );
        }

        @Override
        public <T> T get( @Nonnull Capability<T> capability )
        {
            return CapabilitiesService.this.get( capability );
        }

        @Override
        public Object get( @Nonnull Name name )
        {
            return CapabilitiesService.this.get( name );
        }

        @Override
        public <T> void set( @Nonnull Capability<T> capability, @Nonnull T value )
        {
            if ( !capability.name().isIn( namespace ) )
            {
                throw new IllegalArgumentException(
                        String.format( "provided capability %s is not in declared namespace %s", capability.name(), namespace ) );
            }

            CapabilitiesService.this.set( capability, value );
        }

        @Override
        public <T> void supply( @Nonnull Capability<T> capability, @Nonnull Supplier<T> dynamicValue )
        {
            if ( !capability.name().isIn( namespace ) )
            {
                throw new IllegalArgumentException(
                        String.format( "provided capability %s is not in declared namespace %s", capability.name(), namespace ) );
            }

            CapabilitiesService.this.supply( capability, dynamicValue );
        }
    }

    public static CapabilitiesService newCapabilities()
    {
        Collection<Class<? extends CapabilityDeclaration>> declarationClasses =
                Services.loadAll( CapabilityDeclaration.class ).stream().map( CapabilityDeclaration::getClass )
                        .collect( Collectors.toList() );
        Collection<CapabilityProvider> capabilityProviders = Services.loadAll( CapabilityProvider.class );
        return new CapabilitiesService( declarationClasses, capabilityProviders );
    }

    private static Map<Name,Capability<?>> getDeclaredCapabilities( Collection<Class<? extends CapabilityDeclaration>> declarationClasses )
    {
        var capabilities = new HashMap<Name,Capability<?>>();

        for ( var declarationClass : declarationClasses )
        {
            getDeclaredCapabilities( declarationClass )
                    .forEach( ( name, capability ) -> capabilities.merge( name, capability, ( oldValue, newValue ) ->
                    {
                        throw new UnsupportedOperationException( format( "duplicate capability %s", name ) );
                    } ) );
        }

        return capabilities;
    }

    private static Map<Name,Capability<?>> getDeclaredCapabilities( Class<?> declarationClass )
    {
        var capabilities = new HashMap<Name,Capability<?>>();

        Arrays.stream( FieldUtils.getAllFields( declarationClass ) )
              .filter( field -> field.getType().isAssignableFrom( Capability.class ) )
              .forEach( field ->
                        {
                            try
                            {
                                var capability = (Capability<?>) field.get( null );
                                if ( field.isAnnotationPresent( Description.class ) )
                                {
                                    capability.setDescription( field.getAnnotation( Description.class ).value() );
                                }
                                if ( field.isAnnotationPresent( Public.class ) )
                                {
                                    capability.setPublic();
                                }
                                capabilities.put( capability.name(), capability );
                            }
                            catch ( Exception e )
                            {
                                throw new RuntimeException( format( "%s %s, from %s is not accessible.", field.getType(), field.getName(),
                                                                    declarationClass.getSimpleName() ), e );
                            }
                        } );

        return capabilities;
    }
}
