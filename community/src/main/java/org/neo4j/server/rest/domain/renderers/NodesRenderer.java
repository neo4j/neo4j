package org.neo4j.server.rest.domain.renderers;

import org.neo4j.server.rest.domain.HtmlHelper;
import org.neo4j.server.rest.domain.Representation;

import java.util.LinkedHashMap;
import java.util.Map;

public class NodesRenderer extends HtmlRenderer
{
    public String render(
            Representation... oneOrManyRepresentations )
    {
        StringBuilder builder = HtmlHelper.start( "Index hits", null );
        if ( oneOrManyRepresentations.length == 0 )
        {
            HtmlHelper.appendMessage( builder, "No index hits" );
            return HtmlHelper.end( builder );
        } else
        {
            for ( Representation rep : oneOrManyRepresentations )
            {
                Map<Object, Object> map = new LinkedHashMap<Object, Object>();
                Map<?, ?> serialized = (Map<?, ?>)rep.serialize();
                RepresentationUtil.transfer( serialized, map, "self",
                        "data" );
                HtmlHelper.append( builder, map, HtmlHelper.ObjectType.NODE );
            }
            return HtmlHelper.end( builder );
        }
    }
}
