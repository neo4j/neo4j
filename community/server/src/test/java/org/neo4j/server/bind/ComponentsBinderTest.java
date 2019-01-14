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
package org.neo4j.server.bind;

import org.glassfish.jersey.internal.PropertiesDelegate;
import org.glassfish.jersey.server.ApplicationHandler;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.jupiter.api.Test;

import java.net.URI;
import javax.ws.rs.core.SecurityContext;

import static javax.ws.rs.HttpMethod.GET;
import static javax.ws.rs.core.Response.Status;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class ComponentsBinderTest
{
    private final ComponentsBinder binder = new ComponentsBinder();

    @Test
    void shouldConfigureSingletonDependencyInjection()
    {
        configureSingletonInjection();
        testDependencyInjectionConfiguration();
    }

    @Test
    void shouldConfigureSupplierDependencyInjection()
    {
        configureSupplierInjection();
        testDependencyInjectionConfiguration();
    }

    @Test
    void shouldConfigureSupplierClassDependencyInjection()
    {
        configureSupplierClassInjection();
        testDependencyInjectionConfiguration();
    }

    @Test
    void shouldThrowWhenDependencyInjectionConfiguredAfterStartup()
    {
        configureSingletonInjection();

        testDependencyInjectionConfiguration();

        assertThrows( IllegalStateException.class, this::configureSingletonInjection );
        assertThrows( IllegalStateException.class, this::configureSingletonInjection );
        assertThrows( IllegalStateException.class, this::configureSupplierClassInjection );
    }

    private void configureSingletonInjection()
    {
        binder.addSingletonBinding( new DummyComponent(), DummyComponent.class );
    }

    private void configureSupplierInjection()
    {
        binder.addLazyBinding( DummyComponent::new, DummyComponent.class );
    }

    private void configureSupplierClassInjection()
    {
        binder.addLazyBinding( DummyComponentSupplier.class, DummyComponent.class );
    }

    private void testDependencyInjectionConfiguration()
    {
        ResourceConfig resourceConfig = new ResourceConfig()
                .register( binder )
                .register( DummyRestResource.class );

        ApplicationHandler handler = new ApplicationHandler( resourceConfig );

        ContainerRequest request = new ContainerRequest( URI.create( "http://neo4j.com/" ), URI.create( "http://neo4j.com/" ), GET,
                mock( SecurityContext.class ), mock( PropertiesDelegate.class ) );

        MemorizingContainerResponseWriter responseWriter = new MemorizingContainerResponseWriter();
        request.setWriter( responseWriter );

        handler.handle( request );

        assertEquals( Status.OK, responseWriter.getStatus() );
        assertEquals( DummyComponent.VALUE, responseWriter.getEntity() );
    }
}
