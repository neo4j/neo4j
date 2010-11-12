package org.neo4j.server.rest.domain.renderers;

import org.neo4j.server.rest.domain.HtmlHelper;
import org.neo4j.server.rest.domain.Representation;

import java.util.HashMap;
import java.util.Map;

public class RootRenderer extends HtmlRenderer
{
    public String render(
            Representation... oneOrManyRepresentations )
    {
        Map<Object, Object> map = new HashMap<Object, Object>();
        Map<?, ?> serialized = (Map<?, ?>)oneOrManyRepresentations[ 0 ].serialize();
        RepresentationUtil.transfer( serialized, map, "index",
                "reference node" );
        return HtmlHelper.from( map, HtmlHelper.ObjectType.ROOT );
    }
}
