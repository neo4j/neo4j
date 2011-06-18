package org.neo4j.server.rest.web;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.server.database.Database;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.TraverserReturnType;
import org.neo4j.server.rest.repr.formats.JsonFormat;

public class PagingTraversalTest
{

    private static final String BASE_URI = "http://neo4j.org:7474/";
    private RestfulGraphDatabase service;
    private Database database;
    private EntityOutputFormat output;
    private String databasePath;
    private GraphDbHelper helper;

    @Before
    public void startDatabase() throws IOException
    {
        databasePath = ServerTestUtils.createTempDir()
                .getAbsolutePath();
        database = new Database( ServerTestUtils.EMBEDDED_GRAPH_DATABASE_FACTORY, databasePath );
        helper = new GraphDbHelper( database );
        output = new EntityOutputFormat( new JsonFormat(), URI.create( BASE_URI ), null );
        service = new RestfulGraphDatabase( uriInfo(), database, new JsonFormat(), output );
    }

    @After
    public void shutdownDatabase() throws IOException
    {
        this.database.shutdown();
        org.apache.commons.io.FileUtils.forceDelete( new File( databasePath ) );
    }

    @Test
    public void shouldLodgeAPagingTraverserAndTraverseTheFirstPageBeforeRespondingWith201()
    {
        Response response = createAPagedTraverser();
        assertEquals( 201, response.getStatus() );
        String responseUri = response.getMetadata()
                .get( "Location" )
                .get( 0 )
                .toString();
        assertThat( responseUri, containsString( BASE_URI + "traversers" ) );
        assertThat( responseUri, containsString( "?returnType=node" ) );
        assertNotNull( response.getEntity() );
        System.out.println(response.getEntity().toString());
        assertThat( response.getEntity()
                .toString(), containsString( "\"name\" : \"19\"" ) );
    }

    @Test
    public void givenAPageTraversalHasBeenCreatedShouldYieldNextPageAndRespondWith200() throws Exception
    {
        Response response = createAPagedTraverser();
        String locationUri = response.getMetadata()
                .get( "Location" )
                .get( 0 )
                .toString();
        String traverserId = parseTraverserIdFromLocationUri( locationUri );
        
        response = service.pagedTraverse( traverserId, TraverserReturnType.node );

        
        
        assertEquals( 200, response.getStatus() );
        assertNotNull( response.getEntity() );
        assertThat( response.getEntity()
                .toString(), not(containsString( "\"name\" : \"19\"" ) ));
        assertThat( response.getEntity()
                .toString(), containsString( "\"name\" : \"91\"" ) );
    }



    @Test
    public void shouldTraverseAsFarAsPossibleForAGivenRangeWhenAtEndOfTraversalAndRespondWith200()
    {
        fail( "not implemented" );
    }

    @Test
    public void shouldRespondWith204IfTraversalResultsInZeroNodes()
    {
        fail( "not implemented" );
    }

    @Test
    public void shouldRespondWith400WhenTraversingRangeThatHasAlreadyBeenTraversed()
    {
        fail( "not implemented" );
    }

    @Test
    public void shouldRespondWith404WhenNoSuchTraversalRegistered()
    {
        fail( "not implemented" );
    }

    @Test
    public void shouldRespondWith210WhenTraversalHasExpired()
    {
        fail( "not implemented" );
    }

    private UriInfo uriInfo()
    {
        UriInfo mockUriInfo = mock( UriInfo.class );
        try
        {
            when( mockUriInfo.getBaseUri() ).thenReturn( new URI( BASE_URI ) );
        }
        catch ( URISyntaxException e )
        {
            throw new RuntimeException( e );
        }

        return mockUriInfo;
    }

    private Response createAPagedTraverser()
    {
        long startNodeId = createListOfNodes( 1000 );
        String description = "{"
                             + "\"prune evaluator\":{\"language\":\"builtin\",\"name\":\"none\"},"
                             + "\"return filter\":{\"language\":\"javascript\",\"body\":\"position.endNode().getProperty('name').contains('9');\"},"
                             + "\"order\":\"depth first\","
                             + "\"relationships\":{\"type\":\"PRECEDES\",\"direction\":\"out\"}" + "}";

        Response response = service.createPagedTraverser( startNodeId, 10, TraverserReturnType.node, description );
        return response;
    }

    private long createListOfNodes( int numberOfNodes )
    {
        Transaction tx = database.graph.beginTx();
        try
        {
            long zerothNode = helper.createNode( MapUtil.map( "name", String.valueOf( 0 ) ) );
            long previousNodeId = zerothNode;
            for ( int i = 1; i < numberOfNodes; i++ )
            {
                long currentNodeId = helper.createNode( MapUtil.map( "name", String.valueOf( i ) ) );
                database.graph.getNodeById( previousNodeId )
                        .createRelationshipTo( database.graph.getNodeById( currentNodeId ),
                                DynamicRelationshipType.withName( "PRECEDES" ) );
            }

            tx.success();
            return zerothNode;
        }
        finally
        {
            tx.finish();
        }
    }
    
    private String parseTraverserIdFromLocationUri( String locationUri )
    {
        return locationUri.substring( locationUri.lastIndexOf( "/" ) + 1, locationUri.lastIndexOf( "?" ) );
    }
}
