package org.neo4j.server.rest.domain.renderers;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.server.database.DatabaseBlockedException;
import org.neo4j.server.rest.domain.HtmlHelper;
import org.neo4j.server.rest.domain.Representation;

import java.util.Collections;
import java.util.Map;

/**
 * For one node, for a list of node (from an index lookup use another
 * renderer, because we maybe don't want the relationship form to be
 * displayed for every single node... it'd be quite bloated.
 */
public class NodeRenderer extends HtmlRenderer
{
    public Iterable<RelationshipType> relationshipTypes;

    public NodeRenderer( Iterable<RelationshipType> relationshipTypes )
    {
        this.relationshipTypes = relationshipTypes;
    }

    @Override
    public String render( Representation... oneOrManyRepresentations )
    {
        Representation rep = oneOrManyRepresentations[ 0 ];
        Map<?, ?> serialized = (Map<?, ?>)rep.serialize();
        String javascript = "";
        StringBuilder builder = HtmlHelper.start( HtmlHelper.ObjectType.NODE,
                javascript );
        HtmlHelper.append(
                builder,
                Collections.singletonMap( "data", serialized.get( "data" ) ),
                HtmlHelper.ObjectType.NODE );
        builder.append( "<form action='javascript:neo4jHtmlBrowse.getRelationships();'><fieldset><legend>Get relationships</legend>\n" );
        builder.append( "<label for='direction'>with direction</label>\n"
                + "<select id='direction'>" );
        builder.append( "<option value='" ).append( serialized.get( "all typed relationships" ) ).append( "'>all</option>" );
        builder.append( "<option value='" ).append( serialized.get( "incoming typed relationships" ) ).append( "'>in</option>" );
        builder.append( "<option value='" ).append( serialized.get( "outgoing typed relationships" ) ).append( "'>out</option>" );
        builder.append( "</select>\n" );
        builder.append( "<label for='types'>for type(s)</label><select id='types' multiple='multiple'>" );

        try
        {
            for ( RelationshipType type : relationshipTypes )
            {
                builder.append( "<option selected='selected' value='" ).append( type.name() ).append( "'>" ).append( type.name() ).append( "</option>" );
            }
        }
        catch ( DatabaseBlockedException e )
        {
            throw new RuntimeException(
                    "Unable to render, database is blocked, see nested exception.",
                    e );
        }
        builder.append( "</select>\n" );
        builder.append( "<button>Get</button>\n" );
        builder.append( "</fieldset></form>\n" );

        return HtmlHelper.end( builder );
    }
}
