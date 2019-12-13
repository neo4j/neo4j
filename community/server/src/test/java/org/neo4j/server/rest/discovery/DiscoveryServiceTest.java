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
package org.neo4j.server.rest.discovery;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Answers;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.server.NeoWebServer;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.rest.repr.formats.JsonFormat;
import org.neo4j.test.server.EntityOutputFormat;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.server.rest.discovery.CommunityDiscoverableURIs.communityDiscoverableURIs;

@RunWith( Parameterized.class )
public class DiscoveryServiceTest
{
    @Parameterized.Parameters( name = "{0}" )
    public static Iterable<Object[]> data()
    {
        List<Object[]> cases = new ArrayList<>();

        // Default config
        cases.add( new Object[]{"http://localhost:7474", "http://localhost:7474", null, null, "bolt://localhost:7687"} );
        cases.add( new Object[]{"https://localhost:7473", "https://localhost:7473", null, null, "bolt://localhost:7687"} );
        cases.add( new Object[]{"http://www.example.com", "http://www.example.com", null, null, "bolt://www.example.com:7687"} );

        // Default config + default listen address 0.0.0.0
        cases.add( new Object[]{"http://localhost:7474 - 0.0.0.0", "http://localhost:7474", null, overrideWithDefaultListenAddress( "0.0.0.0" ),
                "bolt://localhost:7687"} );
        cases.add( new Object[]{"https://localhost:7473 - 0.0.0.0", "https://localhost:7473", null, overrideWithDefaultListenAddress( "0.0.0.0" ),
                "bolt://localhost:7687"} );
        cases.add( new Object[]{"http://www.example.com - 0.0.0.0", "http://www.example.com", null, overrideWithDefaultListenAddress( "0.0.0.0" ),
                "bolt://www.example.com:7687"} );

        // Default config + default listen address ::
        cases.add(
                new Object[]{"http://localhost:7474 - ::", "http://localhost:7474", null, overrideWithDefaultListenAddress( "::" ), "bolt://localhost:7687"} );
        cases.add( new Object[]{"https://localhost:7473 - ::", "https://localhost:7473", null, overrideWithDefaultListenAddress( "::" ),
                "bolt://localhost:7687"} );
        cases.add( new Object[]{"http://www.example.com - ::", "http://www.example.com", null, overrideWithDefaultListenAddress( "::" ),
                "bolt://www.example.com:7687"} );

        // Default config + bolt listen address [::]:8888
        cases.add( new Object[]{"http://localhost:7474 - [::]:8888", "http://localhost:7474", null,
                combineConfigOverriders( overrideWithDefaultListenAddress( "::" ), overrideWithListenAddress( "::", 8888 ) ), "bolt://localhost:8888"} );
        cases.add( new Object[]{"https://localhost:7473 - [::]:8888", "https://localhost:7473", null,
                combineConfigOverriders( overrideWithDefaultListenAddress( "::" ), overrideWithListenAddress( "::", 8888 ) ), "bolt://localhost:8888"} );
        cases.add( new Object[]{"http://www.example.com - [::]:8888", "http://www.example.com", null,
                combineConfigOverriders( overrideWithDefaultListenAddress( "::" ), overrideWithListenAddress( "::", 8888 ) ), "bolt://www.example.com:8888"} );

        // Default config + advertised address
        cases.add(
                new Object[]{"http://www.example.com (advertised 1)", "http://www.example.com", null, overrideWithAdvertisedAddress( "www.example.com", 8898 ),
                        "bolt://www.example.com:8898"} );
        cases.add(
                new Object[]{"http://www.example.com (advertised 2)", "http://www.example.com", null, overrideWithAdvertisedAddress( "www2.example.com", 7576 ),
                        "bolt://www2.example.com:7576"} );

        // Default config + advertised address with port 0
        cases.add( new Object[]{"http://www.example.com (advertised 3)", "http://www.example.com", register( "bolt", "localhost", 9999 ),
                overrideWithAdvertisedAddress( "www2.example.com", 0 ), "bolt://www2.example.com:9999"} );

        // Default config + discoverable address
        cases.add( new Object[]{"http://www.example.com (discoverable 1)", "http://www.example.com", null,
                overrideWithDiscoverable( "bolt://www.notanexample.com:7777" ), "bolt://www.notanexample.com:7777"} );
        cases.add( new Object[]{"http://www.example.com (discoverable 2)", "http://www.example.com", null,
                overrideWithDiscoverable( "something://www.notanexample.com:7777" ), "something://www.notanexample.com:7777"} );

        // Default config + discoverable address + advertised address
        cases.add( new Object[]{"http://www.example.com (discoverable and advertised 1)", "http://www.example.com", null,
                combineConfigOverriders( overrideWithDiscoverable( "bolt://www.notanexample.com:7777" ),
                        overrideWithAdvertisedAddress( "www.notanexample2.com", 8888 ) ), "bolt://www.notanexample.com:7777"} );
        cases.add( new Object[]{"http://www.example.com (discoverable and advertised 2)", "http://www.example.com", null,
                combineConfigOverriders( overrideWithAdvertisedAddress( "www.notanexample2.com", 8888 ),
                        overrideWithDiscoverable( "bolt://www.notanexample.com:7777" ) ), "bolt://www.notanexample.com:7777"} );

        return cases;
    }

    private final NeoWebServer neoWebServer = mock( NeoWebServer.class, Answers.RETURNS_DEEP_STUBS );
    private final ConnectorPortRegister portRegistry = mock( ConnectorPortRegister.class );

    private URI baseUri;
    private URI dbUri;
    private Consumer<ConnectorPortRegister> portRegistryOverrider;
    private Consumer<Config.Builder> configOverrider;

    private String expectedDatabaseUri;
    private String expectedBoltUri;

    public DiscoveryServiceTest( String description, String baseUri, Consumer<ConnectorPortRegister> portRegistryOverrider,
            Consumer<Config.Builder> configOverrider, String expectedBoltUri ) throws Throwable
    {
        this.baseUri = new URI( baseUri );
        this.dbUri = new URI( "/db" );
        this.portRegistryOverrider = portRegistryOverrider;
        this.configOverrider = configOverrider;

        this.expectedDatabaseUri = this.baseUri.resolve( this.dbUri ).toString();
        this.expectedBoltUri = expectedBoltUri;
    }

    @Before
    public void setUp()
    {
        if ( portRegistryOverrider != null )
        {
            portRegistryOverrider.accept( portRegistry );
        }
        else
        {
            when( portRegistry.getLocalAddress( "bolt" ) ).thenReturn( new HostnamePort( "localhost", 7687 ) );
        }

        DependencyResolver dependencyResolver = mock( DependencyResolver.class );
        when( dependencyResolver.resolveDependency( ConnectorPortRegister.class ) ).thenReturn( portRegistry );
    }

    private Config mockConfig()
    {
        Config.Builder builder = Config.newBuilder()
            .set( GraphDatabaseSettings.auth_enabled, false )
            .set( BoltConnector.enabled, true )
            .set( ServerSettings.db_api_path, dbUri );

        if ( configOverrider != null )
        {
            configOverrider.accept( builder );
        }

        return builder.build();
    }

    private DiscoveryService testDiscoveryService()
    {
        Config config = mockConfig();
        return new DiscoveryService( config, new EntityOutputFormat( new JsonFormat(), baseUri ),
                communityDiscoverableURIs( config, portRegistry ), mock( ServerVersionAndEdition.class ) );
    }

    @Test
    public void shouldReturnValidJSON() throws Exception
    {
        Response response = testDiscoveryService().getDiscoveryDocument( uriInfo( baseUri ) );
        String json = new String( (byte[]) response.getEntity() );

        assertNotNull( json );
        assertThat( json.length(), is( greaterThan( 0 ) ) );
        assertThat( json, is( not( "\"\"" ) ) );
        assertThat( json, is( not( "null" ) ) );
    }

    private UriInfo uriInfo( URI baseUri )
    {
        UriInfo uriInfo = mock( UriInfo.class );
        when( uriInfo.getBaseUri() ).thenReturn( baseUri );
        return uriInfo;
    }

    @Test
    public void shouldReturnBoltDirectURI() throws Exception
    {
        Response response = testDiscoveryService().getDiscoveryDocument( uriInfo( baseUri ) );
        String json = new String( (byte[]) response.getEntity() );
        assertThat( json, containsString( "\"bolt_direct\" : \"" + expectedBoltUri ) );
    }

    @Test
    public void shouldReturnTxURI() throws Exception
    {
        Response response = testDiscoveryService().getDiscoveryDocument( uriInfo( baseUri ) );
        String json = new String( (byte[]) response.getEntity() );
        assertThat( json, containsString( "\"transaction\" : \"" + expectedDatabaseUri + "/" ) );
    }

    @Test
    public void shouldNotReturnManagementURI() throws Exception
    {
        Response response = testDiscoveryService().getDiscoveryDocument( uriInfo( baseUri ) );
        String json = new String( (byte[]) response.getEntity() );
        assertThat( json, not( containsString( "\"management\"" ) ) );
    }

    @Test
    public void shouldReturnRedirectToAbsoluteAPIUsingOutputFormat() throws Exception
    {
        Config config = Config.defaults( ServerSettings.browser_path, URI.create( "/browser/" ) );

        String baseUri = "http://www.example.com:5435";
        DiscoveryService ds = new DiscoveryService( config, new EntityOutputFormat( new JsonFormat(), new URI( baseUri ) ),
                communityDiscoverableURIs( config, null ), mock( ServerVersionAndEdition.class ) );

        Response response = ds.redirectToBrowser();

        assertThat( response.getMetadata().getFirst( "Location" ), is( new URI( "http://www.example" + ".com:5435/browser/" ) ) );
    }

    private static Consumer<ConnectorPortRegister> register( String connector, String host, int port )
    {
        return register -> when( register.getLocalAddress( connector ) ).thenReturn( new HostnamePort( host, port ) );
    }

    private static Consumer<Config.Builder> overrideWithAdvertisedAddress( String host, int port )
    {
        return builder -> builder.set( BoltConnector.advertised_address, new SocketAddress( host, port ) );
    }

    private static Consumer<Config.Builder> overrideWithListenAddress( String host, int port )
    {
        return builder ->
        {
            builder.set( BoltConnector.listen_address, new SocketAddress( host, port ) );
            builder.setDefault( BoltConnector.advertised_address, new SocketAddress( port ) );
        };
    }

    private static Consumer<Config.Builder> overrideWithDefaultListenAddress( String host )
    {
        return builder -> builder.set( GraphDatabaseSettings.default_listen_address, new SocketAddress( host ) );
    }

    private static Consumer<Config.Builder> overrideWithDiscoverable( String uri )
    {
        return builder -> builder.set( ServerSettings.bolt_discoverable_address, URI.create( uri ) );
    }

    @SafeVarargs
    private static Consumer<Config.Builder> combineConfigOverriders( Consumer<Config.Builder>... overriders )
    {
        return config ->
        {
            for ( Consumer<Config.Builder> overrider : overriders )
            {
                overrider.accept( config );
            }
        };
    }
}
