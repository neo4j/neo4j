package org.neo4j.server.rest.domain.renderers;

import org.neo4j.server.rest.domain.HtmlHelper;
import org.neo4j.server.rest.domain.Representation;

import java.util.LinkedHashMap;
import java.util.Map;

public class RelationshipRenderer extends HtmlRenderer
{
    public String render(
            Representation... oneOrManyRepresentations )
    {
        Map<?, ?> serialized = (Map<?, ?>)oneOrManyRepresentations[ 0 ].serialize();
        Map<Object, Object> map = new LinkedHashMap<Object, Object>();
        RepresentationUtil.transfer( serialized, map, "type", "data",
                "start", "end" );
        return HtmlHelper.from( map, HtmlHelper.ObjectType.RELATIONSHIP );
    }
}
