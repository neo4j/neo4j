package org.neo4j.server.rest.domain.renderers;

import org.neo4j.server.rest.domain.HtmlHelper;
import org.neo4j.server.rest.domain.Representation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

public class RelationshipsRenderer extends HtmlRenderer
{
    public String render(
            Representation... oneOrManyRepresentations )
    {
        if ( oneOrManyRepresentations.length == 0 )
        {
            StringBuilder builder = HtmlHelper.start(
                    HtmlHelper.ObjectType.RELATIONSHIP, null );
            HtmlHelper.appendMessage( builder, "No relationships found" );
            return HtmlHelper.end( builder );
        } else
        {
            Collection<Object> list = new ArrayList<Object>();
            for ( Representation rep : oneOrManyRepresentations )
            {
                Map<?, ?> serialized = (Map<?, ?>)rep.serialize();
                Map<Object, Object> map = new LinkedHashMap<Object, Object>();
                RepresentationUtil.transfer( serialized, map, "self",
                        "type", "data", "start", "end" );
                list.add( map );
            }
            return HtmlHelper.from( list, HtmlHelper.ObjectType.RELATIONSHIP );
        }
    }
}
