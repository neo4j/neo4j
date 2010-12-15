package org.neo4j.server.rest.repr.formats;

import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class UrlFormFormatTest
{
    @Test
    public void shouldParseEmptyMap() throws Exception
    {
        UrlFormFormat format = new UrlFormFormat();
        Map<String,Object> map = format.readMap( "" );

        assertThat(map.size(), is(0));
    }

    @Test
    public void canParseSingleKeyMap() throws Exception
    {
        UrlFormFormat format = new UrlFormFormat();
        Map<String,Object> map = format.readMap( "var=A" );

        assertThat(map.size(), is(1));
        assertThat((String)map.get( "var" ), is("A"));
    }

        @Test
    public void canParseListsInMaps() throws Exception
    {
        UrlFormFormat format = new UrlFormFormat();
        Map<String,Object> map = format.readMap( "var=A&var=B" );

        assertThat(map.size(), is(1));
        assertThat(((List<String>)map.get( "var" )).get(0), is("A"));
        assertThat(((List<String>)map.get( "var" )).get(1), is("B"));
    }
}
