package org.neo4j.server.rest.domain.renderers;

import org.neo4j.server.rest.domain.HtmlHelper;
import org.neo4j.server.rest.domain.Representation;

import java.util.List;
import java.util.Map;

public class IndexRootRenderer extends HtmlRenderer
{
    public String render(
            Representation... oneOrManyRepresentations )
    {
        Map<?, ?> serialized = (Map<?, ?>)oneOrManyRepresentations[ 0 ].serialize();
        String javascript = "";
        StringBuilder builder = HtmlHelper.start( HtmlHelper.ObjectType.INDEX_ROOT,
                javascript );
        int counter = 0;
        for ( String objectType : new String[]{"node", "relationship"} )
        {
            List<?> list = (List<?>)serialized.get( objectType );
            if ( list == null )
            {
                continue;
            }
            builder.append( "<ul>" );
            for ( Object indexMapObject : list )
            {
                builder.append( "<li>" );
                Map<?, ?> indexMap = (Map<?, ?>)indexMapObject;
                String keyId = "key_" + counter;
                String valueId = "value_" + counter;
                builder.append( "<form action='javascript:neo4jHtmlBrowse.search(\"" ).append( indexMap.get( "template" ) ).append( "\",\"" ).append( keyId ).append( "\",\"" ).append( valueId ).append( "\");'><fieldset><legend>" ).append( indexMap.get( "type" ) ).append( "</legend>\n" );
                builder.append( "<label for='" ).append( keyId ).append( "'>Key</label><input id='" ).append( keyId ).append( "'>\n" );
                builder.append( "<label for='" ).append( valueId ).append( "'>Value</label><input id='" ).append( valueId ).append( "'>\n" );
                builder.append( "<button>Search</button>\n" );
                builder.append( "</fieldset></form>\n" );
                builder.append( "</li>\n" );
                counter++;
            }
            builder.append( "</ul>" );
        }
        return HtmlHelper.end( builder );
    }
}
