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
package org.neo4j.server.rest;

import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.HttpConnector;
import org.neo4j.kernel.configuration.HttpConnector.Encryption;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.internal.KernelData;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.NeoServer;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.WrappedDatabase;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.rest.management.JmxService;
import org.neo4j.server.rest.management.RootService;
import org.neo4j.server.rest.management.VersionAndEditionService;
import org.neo4j.server.rest.management.console.ConsoleService;
import org.neo4j.server.rest.management.console.ConsoleSessionFactory;
import org.neo4j.server.rest.management.console.ScriptSession;
import org.neo4j.server.rest.management.console.ShellSession;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.formats.JsonFormat;
import org.neo4j.shell.ShellSettings;
import org.neo4j.string.UTF8;
import org.neo4j.test.TestData;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.server.EntityOutputFormat;
import org.neo4j.test.server.ExclusiveServerTestBase;
import org.neo4j.test.server.HTTP;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static java.lang.System.lineSeparator;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.test.rule.SuppressOutput.suppressAll;

public class ManageNodeIT extends AbstractRestFunctionalDocTestBase
{
    private static final long NON_EXISTENT_NODE_ID = 999999;
    private static String NODE_URI_PATTERN = "^.*/node/[0-9]+$";

    private static FunctionalTestHelper functionalTestHelper;
    private static GraphDbHelper helper;

    @BeforeClass
    public static void setupServer()
    {
        functionalTestHelper = new FunctionalTestHelper( server() );
        helper = functionalTestHelper.getGraphDbHelper();
    }

    @Test
    public void create_node()
    {
        JaxRsResponse response = gen.get()
                .expectedStatus( 201 )
                .expectedHeader( "Location" )
                .post( functionalTestHelper.nodeUri() )
                .response();
        assertTrue( response.getLocation()
                .toString()
                .matches( NODE_URI_PATTERN ) );
    }

    @Test
    public void create_node_with_properties()
    {
        JaxRsResponse response = gen.get()
                .payload( "{\"foo\" : \"bar\"}" )
                .expectedStatus( 201 )
                .expectedHeader( "Location" )
                .expectedHeader( "Content-Length" )
                .post( functionalTestHelper.nodeUri() )
                .response();
        assertTrue( response.getLocation()
                .toString()
                .matches( NODE_URI_PATTERN ) );
    }

    @Test
    public void create_node_with_array_properties()
    {
        String response = gen.get()
                .payload( "{\"foo\" : [1,2,3]}" )
                .expectedStatus( 201 )
                .expectedHeader( "Location" )
                .expectedHeader( "Content-Length" )
                .post( functionalTestHelper.nodeUri() )
                .response().getEntity();
        assertThat( response, containsString( "[ 1, 2, 3 ]" ) );
    }

    @Documented( "Property values can not be null.\n" +
                 "\n" +
                 "This example shows the response you get when trying to set a property to +null+." )
    @Test
    public void shouldGet400WhenSupplyingNullValueForAProperty()
    {
        gen.get()
                .payload( "{\"foo\":null}" )
                .expectedStatus( 400 )
                .post( functionalTestHelper.nodeUri() );
    }

    @Test
    public void shouldGet400WhenCreatingNodeMalformedProperties()
    {
        JaxRsResponse response = sendCreateRequestToServer("this:::isNot::JSON}");
        assertEquals( 400, response.getStatus() );
    }

    @Test
    public void shouldGet400WhenCreatingNodeUnsupportedNestedPropertyValues()
    {
        JaxRsResponse response = sendCreateRequestToServer("{\"foo\" : {\"bar\" : \"baz\"}}");
        assertEquals( 400, response.getStatus() );
    }

    private JaxRsResponse sendCreateRequestToServer( final String json )
    {
        return RestRequest.req().post( functionalTestHelper.dataUri() + "node/" , json );
    }

    private JaxRsResponse sendCreateRequestToServer()
    {
        return RestRequest.req().post( functionalTestHelper.dataUri() + "node/" , null, MediaType.APPLICATION_JSON_TYPE );
    }

    @Test
    public void shouldGetValidLocationHeaderWhenCreatingNode()
    {
        JaxRsResponse response = sendCreateRequestToServer();
        assertNotNull( response.getLocation() );
        assertTrue( response.getLocation()
                .toString()
                .startsWith( functionalTestHelper.dataUri() + "node/" ) );
    }

    @Test
    public void shouldGetASingleContentLengthHeaderWhenCreatingANode()
    {
        JaxRsResponse response = sendCreateRequestToServer();
        List<String> contentLentgthHeaders = response.getHeaders()
                .get( "Content-Length" );
        assertNotNull( contentLentgthHeaders );
        assertEquals( 1, contentLentgthHeaders.size() );
    }

    @Test
    public void shouldBeJSONContentTypeOnResponse()
    {
        JaxRsResponse response = sendCreateRequestToServer();
        assertThat( response.getType().toString(), containsString( MediaType.APPLICATION_JSON ) );
    }

    @Test
    public void shouldGetValidNodeRepresentationWhenCreatingNode() throws Exception
    {
        JaxRsResponse response = sendCreateRequestToServer();
        String entity = response.getEntity();

        Map<String, Object> map = JsonHelper.jsonToMap( entity );

        assertNotNull( map );
        assertTrue( map.containsKey( "self" ) );

    }

    @Documented( "Delete node." )
    @Test
    public void shouldRespondWith204WhenNodeDeleted()
    {
        long node = helper.createNode();
        gen.get().expectedStatus( 204 )
                .delete( functionalTestHelper.dataUri() + "node/" + node );
    }

    @Test
    public void shouldRespondWith404AndSensibleEntityBodyWhenNodeToBeDeletedCannotBeFound() throws Exception
    {
        JaxRsResponse response = sendDeleteRequestToServer(NON_EXISTENT_NODE_ID);
        assertEquals( 404, response.getStatus() );

        Map<String, Object> jsonMap = JsonHelper.jsonToMap( response.getEntity() );
        assertThat( jsonMap, hasKey( "message" ) );
        assertNotNull( jsonMap.get( "message" ) );
    }

    @Documented( "Nodes with relationships cannot be deleted.\n" +
                 "\n" +
                 "The relationships on a node has to be deleted before the node can be\n" +
                 "deleted.\n" +
                 " \n" +
                 "TIP: You can use `DETACH DELETE` in Cypher to delete nodes and their relationships in one go." )
    @Test
    public void shouldRespondWith409AndSensibleEntityBodyWhenNodeCannotBeDeleted() throws Exception
    {
        long id = helper.createNode();
        helper.createRelationship( "LOVES", id, helper.createNode() );
        JaxRsResponse response = sendDeleteRequestToServer(id);
        assertEquals( 409, response.getStatus() );
        Map<String, Object> jsonMap = JsonHelper.jsonToMap( response.getEntity() );
        assertThat( jsonMap, hasKey( "message" ) );
        assertNotNull( jsonMap.get( "message" ) );

        gen.get().expectedStatus( 409 )
                .delete( functionalTestHelper.dataUri() + "node/" + id );
    }

    @Test
    public void shouldRespondWith400IfInvalidJsonSentAsNodePropertiesDuringNodeCreation()
    {
        String mangledJsonArray = "{\"myprop\":[1,2,\"three\"]}";
        JaxRsResponse response = sendCreateRequestToServer(mangledJsonArray);
        assertEquals( 400, response.getStatus() );
        assertEquals( "text/plain", response.getType()
                .toString() );
        assertThat( response.getEntity(), containsString( mangledJsonArray ) );
    }

    @Test
    public void shouldRespondWith400IfInvalidJsonSentAsNodeProperty()
    {
        URI nodeLocation = sendCreateRequestToServer().getLocation();

        String mangledJsonArray = "[1,2,\"three\"]";
        JaxRsResponse response = RestRequest.req().put(nodeLocation.toString() + "/properties/myprop", mangledJsonArray);
        assertEquals(400, response.getStatus());
        assertEquals("text/plain", response.getType()
                .toString());
        assertThat( response.getEntity(), containsString(mangledJsonArray));
        response.close();
    }

    @Test
    public void shouldRespondWith400IfInvalidJsonSentAsNodeProperties()
    {
        URI nodeLocation = sendCreateRequestToServer().getLocation();

        String mangledJsonProperties = "{\"a\":\"b\", \"c\":[1,2,\"three\"]}";
        JaxRsResponse response = RestRequest.req().put(nodeLocation.toString() + "/properties", mangledJsonProperties);
        assertEquals(400, response.getStatus());
        assertEquals("text/plain", response.getType()
                .toString());
        assertThat( response.getEntity(), containsString(mangledJsonProperties));
        response.close();
    }

    private JaxRsResponse sendDeleteRequestToServer( final long id )
    {
        return RestRequest.req().delete(functionalTestHelper.dataUri() + "node/" + id);
    }

    /*
        Note that when running this test from within an IDE, the version field will be an empty string. This is because the
        code that generates the version identifier is written by Maven as part of the build process(!). The tests will pass
        both in the IDE (where the empty string will be correctly compared).
         */
    public static class CommunityVersionAndEditionServiceDocIT extends ExclusiveServerTestBase
    {
        private static NeoServer server;
        private static FunctionalTestHelper functionalTestHelper;

        @ClassRule
        public static TemporaryFolder staticFolder = new TemporaryFolder();

        @Rule
        public TestData<RESTRequestGenerator> gen = TestData.producedThrough( RESTRequestGenerator.PRODUCER );
        private static FakeClock clock;

        @BeforeClass
        public static void setupServer() throws Exception
        {
            clock = Clocks.fakeClock();
            server = CommunityServerBuilder.server()
                    .usingDataDir( staticFolder.getRoot().getAbsolutePath() )
                    .withClock( clock )
                    .build();

            suppressAll().call( (Callable<Void>) () ->
            {
                server.start();
                return null;
            } );
            functionalTestHelper = new FunctionalTestHelper( server );
        }

        @Before
        public void setupTheDatabase()
        {
            // do nothing, we don't care about the database contents here
        }

        @AfterClass
        public static void stopServer() throws Exception
        {
            suppressAll().call( (Callable<Void>) () ->
            {
                server.stop();
                return null;
            } );
        }

        @Test
        public void shouldReportCommunityEdition() throws Exception
        {
            // Given
            String releaseVersion = server.getDatabase().getGraph().getDependencyResolver().resolveDependency( KernelData
                    .class ).version().getReleaseVersion();

            // When
            HTTP.Response res =
                    HTTP.GET( functionalTestHelper.managementUri() + "/" + VersionAndEditionService.SERVER_PATH );

            // Then
            assertEquals( 200, res.status() );
            assertThat( res.get( "edition" ).asText(), equalTo( "community" ) );
            assertThat( res.get( "version" ).asText(), equalTo( releaseVersion ) );
        }
    }

    public static class ConfigureEnabledManagementConsolesDocIT extends ExclusiveServerTestBase
    {
        private NeoServer server;

        @After
        public void stopTheServer()
        {
            server.stop();
        }

        @Test
        public void shouldBeAbleToExplicitlySetConsolesToEnabled() throws Exception
        {
            server = CommunityServerBuilder.server().withProperty( ServerSettings.console_module_engines.name(), "" )
                    .usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                    .build();
            server.start();

            assertThat( exec( "ls", "shell" ).getStatus(), is( 400 ) );
        }

        @Test
        public void shellConsoleShouldBeEnabledByDefault() throws Exception
        {
            server = CommunityServerBuilder.server().usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() ).build();
            server.start();

            assertThat( exec( "ls", "shell" ).getStatus(), is( 200 ) );
        }

        private JaxRsResponse exec( String command, String engine )
        {
            return RestRequest.req().post( server.baseUri() + "db/manage/server/console", "{" +
                    "\"engine\":\"" + engine + "\"," +
                    "\"command\":\"" + command + "\\n\"}" );
        }
    }

    public static class ConsoleServiceDocTest
    {
        private final URI uri = URI.create( "http://peteriscool.com:6666/" );

        @Test
        public void correctRepresentation()
        {
            ConsoleService consoleService = new ConsoleService( new ShellOnlyConsoleSessionFactory(), mock( Database.class ),
                    NullLogProvider.getInstance(), new OutputFormat( new JsonFormat(), uri, null ) );

            Response consoleResponse = consoleService.getServiceDefinition();

            assertEquals( 200, consoleResponse.getStatus() );
            String response = decode( consoleResponse );
            MatcherAssert.assertThat( response, containsString( "resources" ) );
            MatcherAssert.assertThat( response, containsString( uri.toString() ) );
        }

        @Test
        public void advertisesAvailableConsoleEngines()
        {
            ConsoleService consoleServiceWithJustShellEngine = new ConsoleService( new ShellOnlyConsoleSessionFactory(),
                    mock( Database.class ), NullLogProvider.getInstance(), new OutputFormat( new JsonFormat(), uri, null ) );

            String response = decode( consoleServiceWithJustShellEngine.getServiceDefinition());

            MatcherAssert.assertThat( response, containsString( "\"engines\" : [ \"shell\" ]" ) );

        }

        private String decode( final Response response )
        {
            return UTF8.decode( (byte[]) response.getEntity() );
        }

        private static class ShellOnlyConsoleSessionFactory implements ConsoleSessionFactory
        {
            @Override
            public ScriptSession createSession( String engineName, Database database, LogProvider logProvider )
            {
                return null;
            }

            @Override
            public Iterable<String> supportedEngines()
            {
                return Collections.singletonList( "shell" );
            }
        }
    }

    public static class JmxServiceDocTest
    {
        public JmxService jmxService;
        private final URI uri = URI.create( "http://peteriscool.com:6666/" );

        @Test
        public void correctRepresentation()
        {
            Response resp = jmxService.getServiceDefinition();

            assertEquals( 200, resp.getStatus() );

            String json = UTF8.decode( (byte[]) resp.getEntity() );
            MatcherAssert.assertThat( json, containsString( "resources" ) );
            MatcherAssert.assertThat( json, containsString( uri.toString() ) );
            MatcherAssert.assertThat( json, containsString( "jmx/domain/{domain}/{objectName}" ) );
        }

        @Test
        public void shouldListDomainsCorrectly()
        {
            Response resp = jmxService.listDomains();

            assertEquals( 200, resp.getStatus() );
        }

        @Test
        public void testwork()
        {
            jmxService.queryBeans( "[\"*:*\"]" );
        }

        @Before
        public void setUp()
        {
            this.jmxService = new JmxService( new OutputFormat( new JsonFormat(), uri, null ), null );
        }

    }

    public static class Neo4jShellConsoleSessionDocTest implements ConsoleSessionFactory
    {
        private ConsoleService consoleService;
        private Database database;
        private final URI uri = URI.create( "http://peteriscool.com:6666/" );

        @Before
        public void setUp()
        {
            this.database = new WrappedDatabase( (GraphDatabaseFacade) new TestGraphDatabaseFactory().
                    newImpermanentDatabaseBuilder().
                    setConfig( ShellSettings.remote_shell_enabled, Settings.TRUE ).
                    newGraphDatabase() );
            this.consoleService = new ConsoleService(
                    this,
                    database,
                    NullLogProvider.getInstance(),
                    new OutputFormat( new JsonFormat(), uri, null ) );
        }

        @After
        public void shutdownDatabase()
        {
            this.database.getGraph().shutdown();
        }

        @Override
        public ScriptSession createSession( String engineName, Database database, LogProvider logProvider )
        {
            return new ShellSession( database.getGraph() );
        }

        @Test
        public void doesntMangleNewlines() throws Exception
        {
            Response response = consoleService.exec( new JsonFormat(),
                    "{ \"command\" : \"create (n) return n;\", \"engine\":\"shell\" }" );

            assertEquals( 200, response.getStatus() );
            String result = decode( response ).get( 0 );

            String expected = "+-----------+" + lineSeparator()
                    + "| n         |" + lineSeparator()
                    + "+-----------+" + lineSeparator()
                    + "| Node[0]{} |" + lineSeparator()
                    + "+-----------+" + lineSeparator()
                    + "1 row";

            MatcherAssert.assertThat( result, containsString( expected ) );
        }

        private List<String> decode( final Response response ) throws JsonParseException
        {
            return (List<String>) JsonHelper.readJson( UTF8.decode( (byte[]) response.getEntity() ) );
        }

        @Override
        public Iterable<String> supportedEngines()
        {
            return new ArrayList<String>()
            {{
                add( "shell" );
            }};
        }
    }

    public static class RootServiceDocTest
    {
        @Test
        public void shouldAdvertiseServicesWhenAsked() throws Exception
        {
            UriInfo uriInfo = mock( UriInfo.class );
            URI uri = new URI( "http://example.org:7474/" );
            when( uriInfo.getBaseUri() ).thenReturn( uri );

            RootService svc = new RootService( new CommunityNeoServer( Config.defaults( stringMap(
                    new HttpConnector( "http", Encryption.NONE ).type.name(), "HTTP",
                    new HttpConnector( "http", Encryption.NONE ).enabled.name(), "true"
            ) ),
                    GraphDatabaseDependencies.newDependencies().userLogProvider( NullLogProvider.getInstance() )
                            .monitors( new Monitors() ),
                    NullLogProvider.getInstance() ) );

            EntityOutputFormat output = new EntityOutputFormat( new JsonFormat(), null, null );
            Response serviceDefinition = svc.getServiceDefinition( uriInfo, output );

            assertEquals( 200, serviceDefinition.getStatus() );
            Map<String, Object> result = (Map<String, Object>) output.getResultAsMap().get( "services" );

            assertThat( result.get( "console" )
                    .toString(), containsString( String.format( "%sserver/console", uri.toString() ) ) );
            assertThat( result.get( "jmx" )
                    .toString(), containsString( String.format( "%sserver/jmx", uri.toString() ) ) );
        }
    }

    public static class VersionAndEditionServiceTest
    {
        @Test
        public void shouldReturnReadableStringForServiceName()
        {
            // given
            VersionAndEditionService service = new VersionAndEditionService( mock( CommunityNeoServer.class ) );

            // when
            String serviceName = service.getName();
            // then
            assertEquals( "version", serviceName );
        }

        @Test
        public void shouldReturnSensiblePathWhereServiceIsHosted()
        {
            // given
            VersionAndEditionService service = new VersionAndEditionService( mock( CommunityNeoServer.class ) );

            // when
            String serverPath = service.getServerPath();

            // then
            assertEquals( "server/version", serverPath );
        }
    }
}
