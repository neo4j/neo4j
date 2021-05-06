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

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.neo4j.annotations.Description;
import org.neo4j.annotations.Public;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.capabilities.Type.BOOLEAN;
import static org.neo4j.capabilities.Type.STRING;
import static org.neo4j.capabilities.Type.listOf;

class CapabilitiesServiceTest
{
    @Test
    void shouldAllowEmptyDeclarations()
    {
        var capabilities = new CapabilitiesService( Collections.emptyList(), Collections.emptyList() );

        assertThat( capabilities.declaredCapabilities() ).isEmpty();
    }

    @Test
    void shouldDiscoverGivenDeclarations()
    {
        var capabilities = new CapabilitiesService( List.of( TestCoreCapabilities.class, TestCypherCapabilities.class ), Collections.emptyList() );
        var declared = capabilities.declaredCapabilities();

        assertThat( declared ).containsExactlyInAnyOrder(
                TestCoreCapabilities.dbms_instance_version, TestCoreCapabilities.dbms_instance_internal_version,
                TestCypherCapabilities.dbms_cypher_version, TestCypherCapabilities.dbms_cypher_runtimes );

        assertThat( TestCoreCapabilities.dbms_instance_version.description() ).isEqualTo( "version of the instance" );
        assertThat( TestCoreCapabilities.dbms_instance_version.internal() ).isFalse();
        assertThat( TestCoreCapabilities.dbms_instance_internal_version.description() ).isEqualTo( "internal version of the instance" );
        assertThat( TestCoreCapabilities.dbms_instance_internal_version.internal() ).isTrue();
        assertThat( TestCypherCapabilities.dbms_cypher_version.description() ).isEqualTo( "cypher version" );
        assertThat( TestCypherCapabilities.dbms_cypher_version.internal() ).isFalse();
        assertThat( TestCypherCapabilities.dbms_cypher_runtimes.description() ).isEqualTo( "available cypher runtimes" );
        assertThat( TestCypherCapabilities.dbms_cypher_runtimes.internal() ).isFalse();
    }

    @Test
    void shouldThrowWhenDuplicateDeclarationsFound()
    {
        assertThatThrownBy( () -> new CapabilitiesService(
                List.of( TestCoreCapabilities.class, TestCypherCapabilities.class, TestDuplicateCapabilities.class ),
                Collections.emptyList() ) ).isInstanceOf( UnsupportedOperationException.class )
                                           .hasMessage( "duplicate capability dbms.cypher.version" );
    }

    @Test
    void shouldReturnAnUnmodifiableInstance()
    {
        var capabilities = new CapabilitiesService( List.of( TestCoreCapabilities.class ), Collections.emptyList() );
        var unmodifiable = capabilities.unmodifiable();

        assertThat( unmodifiable ).isInstanceOf( Capabilities.class );
        assertThat( unmodifiable ).isNotInstanceOf( CapabilitiesRegistry.class );
    }

    @Test
    void shouldSetAndGetStaticValues()
    {
        var capabilities = new CapabilitiesService( List.of( TestCoreCapabilities.class ), Collections.emptyList() );

        capabilities.set( TestCoreCapabilities.dbms_instance_version, "4.3.0" );
        capabilities.set( TestCoreCapabilities.dbms_instance_internal_version, "4.3.0-drop03" );

        assertThat( capabilities.get( TestCoreCapabilities.dbms_instance_version ) ).isEqualTo( "4.3.0" );
        assertThat( capabilities.get( TestCoreCapabilities.dbms_instance_internal_version ) ).isEqualTo( "4.3.0-drop03" );
        assertThat( capabilities.unmodifiable().get( TestCoreCapabilities.dbms_instance_version ) ).isEqualTo( "4.3.0" );
        assertThat( capabilities.unmodifiable().get( TestCoreCapabilities.dbms_instance_internal_version ) ).isEqualTo( "4.3.0-drop03" );
    }

    @Test
    void shouldSetAndGetDynamicValues()
    {
        var capabilities = new CapabilitiesService( List.of( TestCoreCapabilities.class ), Collections.emptyList() );

        capabilities.supply( TestCoreCapabilities.dbms_instance_version, () -> "4.3.0" );
        capabilities.supply( TestCoreCapabilities.dbms_instance_internal_version, () -> "4.3.0-drop03" );

        assertThat( capabilities.get( TestCoreCapabilities.dbms_instance_version ) ).isEqualTo( "4.3.0" );
        assertThat( capabilities.get( TestCoreCapabilities.dbms_instance_internal_version ) ).isEqualTo( "4.3.0-drop03" );
        assertThat( capabilities.unmodifiable().get( TestCoreCapabilities.dbms_instance_version ) ).isEqualTo( "4.3.0" );
        assertThat( capabilities.unmodifiable().get( TestCoreCapabilities.dbms_instance_internal_version ) ).isEqualTo( "4.3.0-drop03" );
    }

    @Test
    void shouldReturnNullForUnsetCapabilities()
    {
        var capabilities = new CapabilitiesService( List.of( TestCoreCapabilities.class ), Collections.emptyList() );

        assertThat( capabilities.get( TestCoreCapabilities.dbms_instance_version ) ).isNull();
    }

    @Test
    void shouldReturnNullForUnregisteredCapabilities()
    {
        var capabilities = new CapabilitiesService( List.of( TestCoreCapabilities.class ), Collections.emptyList() );

        assertThat( capabilities.get( Name.of( "dbms.instance.unregistered" ) ) ).isNull();
    }

    @Test
    void shouldThrowWhileSettingUnknownCapabilities()
    {
        var capabilities = new CapabilitiesService( List.of( TestCoreCapabilities.class ), Collections.emptyList() );

        assertThatThrownBy( () -> capabilities.set( TestCypherCapabilities.dbms_cypher_version, "4.3.0" ) ).isInstanceOf( IllegalArgumentException.class );
        assertThatThrownBy( () -> capabilities.supply( TestCypherCapabilities.dbms_cypher_version, () -> "4.3.0" ) )
                .isInstanceOf( IllegalArgumentException.class );
    }

    @Test
    void shouldProcessProviders()
    {
        var capabilities = new CapabilitiesService( List.of( TestProviderCapabilities.class ), List.of( new TestProviderA(), new TestProviderB() ) );

        capabilities.processProviders();

        assertThat( capabilities.get( TestProviderCapabilities.component_a_version ) ).isEqualTo( "5.2" );
        assertThat( capabilities.get( TestProviderCapabilities.component_a_authentication_supported ) ).isTrue();
        assertThat( capabilities.get( TestProviderCapabilities.component_b_version ) ).isEqualTo( "2.3" );
        assertThat( capabilities.get( TestProviderCapabilities.component_b_supported_versions ) ).containsExactly( "2.0", "2.1", "2.2" );
    }

    @Test
    void shouldThrowWhenNamespaceOutOfRange()
    {
        var capabilities =
                new CapabilitiesService( List.of( TestProviderCapabilities.class ), List.of( new TestProviderA(), new TestProviderNamespaceError() ) );

        assertThatThrownBy( capabilities::processProviders ).isInstanceOf( IllegalArgumentException.class ).hasMessage(
                "provided capability org.example.componentB.version is not in declared namespace org.example.component" );
    }

    private static class TestCoreCapabilities implements CapabilityDeclaration
    {
        @Public
        @Description( "version of the instance" )
        public static final Capability<String> dbms_instance_version = new Capability<>( Name.of( "dbms.instance.version" ), STRING );

        @Description( "internal version of the instance" )
        public static final Capability<String> dbms_instance_internal_version = new Capability<>( Name.of( "dbms.instance.internal_version" ), STRING );
    }

    private static class TestCypherCapabilities implements CapabilityDeclaration
    {
        @Public
        @Description( "available cypher runtimes" )
        public static final Capability<Collection<String>> dbms_cypher_runtimes = new Capability<>( Name.of( "dbms.cypher.runtimes" ), listOf( STRING ) );

        @Public
        @Description( "cypher version" )
        public static final Capability<String> dbms_cypher_version = new Capability<>( Name.of( "dbms.cypher.version" ), STRING );
    }

    private static class TestDuplicateCapabilities implements CapabilityDeclaration
    {
        @Public
        @Description( "duplicate cypher version" )
        @SuppressWarnings( "unused" )
        public static final Capability<String> dbms_cypher_version = new Capability<>( Name.of( "dbms.cypher.version" ), STRING );
    }

    private static class TestProviderCapabilities implements CapabilityDeclaration
    {
        @Public
        @Description( "component a version" )
        public static final Capability<String> component_a_version = new Capability<>( Name.of( "org.example.componentA.version" ), STRING );

        @Public
        @Description( "component a supports authentication" )
        public static final Capability<Boolean> component_a_authentication_supported =
                new Capability<>( Name.of( "org.example.componentA.supports_authentication" ), BOOLEAN );

        @Public
        @Description( "component b version" )
        public static final Capability<String> component_b_version = new Capability<>( Name.of( "org.example.componentB.version" ), STRING );

        @Public
        @Description( "component b supported list of versions" )
        public static final Capability<Collection<String>> component_b_supported_versions =
                new Capability<>( Name.of( "org.example.componentB.supported_versions" ), listOf( STRING ) );
    }

    private static class TestProviderA implements CapabilityProvider
    {

        @Override
        public String namespace()
        {
            return "org.example.componentA";
        }

        @Override
        public void register( CapabilitiesRegistry registry )
        {
            registry.set( TestProviderCapabilities.component_a_version, "5.2" );
            registry.supply( TestProviderCapabilities.component_a_authentication_supported, () -> true );
        }
    }

    private static class TestProviderB implements CapabilityProvider
    {

        @Override
        public String namespace()
        {
            return "org.example.componentB";
        }

        @Override
        public void register( CapabilitiesRegistry registry )
        {
            registry.set( TestProviderCapabilities.component_b_version, "2.3" );
            registry.supply( TestProviderCapabilities.component_b_supported_versions, () -> List.of( "2.0", "2.1", "2.2" ) );
        }
    }

    private static class TestProviderNamespaceError implements CapabilityProvider
    {

        @Override
        public String namespace()
        {
            return "org.example.component";
        }

        @Override
        public void register( CapabilitiesRegistry registry )
        {
            registry.set( TestProviderCapabilities.component_b_version, "2.3" );
            registry.supply( TestProviderCapabilities.component_b_supported_versions, () -> List.of( "2.0", "2.1", "2.2" ) );
        }
    }
}
