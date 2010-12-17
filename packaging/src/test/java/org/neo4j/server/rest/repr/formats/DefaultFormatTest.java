package org.neo4j.server.rest.repr.formats;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.DefaultFormat;
import org.neo4j.server.rest.repr.MediaTypeNotSupportedException;

import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Map;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class DefaultFormatTest
{
    private DefaultFormat input;

    @Before
    public void setUp() throws Exception
    {
        JsonFormat inner = new JsonFormat();
        ArrayList<MediaType> supported = new ArrayList<MediaType>();
        MediaType requested = MediaType.APPLICATION_JSON_TYPE;
        input = new DefaultFormat( inner, supported, requested );
    }

    @Test
    public void canReadEmptyMap() throws Exception
    {
        Map<String, Object> map = input.readMap( "{}" );
        assertNotNull( map );
        assertTrue( "map is not empty", map.isEmpty() );
    }

    @Test
    public void canReadMapWithTwoValues() throws Exception
    {
        Map<String, Object> map = input.readMap( "{\"key1\":\"value1\",     \"key2\":\"value11\"}" );
        assertNotNull( map );
        assertThat( map, hasEntry( "key1", (Object) "value1" ) );
        assertThat( map, hasEntry( "key2", (Object) "value11" ) );
        assertTrue( "map contained extra values", map.size() == 2 );
    }

    @Test
    public void canReadMapWithNestedMap() throws Exception
    {
        Map<String, Object> map = input.readMap( "{\"nested\": {\"key\": \"valuable\"}}" );
        assertNotNull( map );
        assertThat( map, hasKey( "nested" ) );
        assertTrue( "map contained extra values", map.size() == 1 );
        Object nested = map.get( "nested" );
        assertThat( nested, instanceOf( Map.class ) );
        @SuppressWarnings( "unchecked" ) Map<String, String> nestedMap = (Map<String, String>) nested;
        assertThat( nestedMap, hasEntry( "key", "valuable" ) );
    }

    @Test(expected = MediaTypeNotSupportedException.class)
    public void failsWithTheCorrectExceptionWhenGettingTheWrongInput() throws BadInputException
    {
        input.readValue( "<xml />" );
    }


    @Test(expected = MediaTypeNotSupportedException.class)
    public void failsWithTheCorrectExceptionWhenGettingTheWrongInput2() throws BadInputException
    {
        input.readMap( "<xml />" );
    }


    @Test(expected = MediaTypeNotSupportedException.class)
    public void failsWithTheCorrectExceptionWhenGettingTheWrongInput3() throws BadInputException
    {
        input.readUri( "<xml />" );
    }
}
