package org.neo4j.server.rest.domain.renderers;

import org.neo4j.server.rest.domain.Representation;

import javax.ws.rs.core.MediaType;

import static org.neo4j.server.rest.domain.renderers.JsonRenderers.asString;

public abstract class HtmlRenderer implements Renderer
{
    public abstract String render( Representation... oneOrManyRepresentations );

    public MediaType getMediaType()
    {
        return MediaType.TEXT_HTML_TYPE;
    }

    public String renderException( String subjectOrNull, Exception exception )
    {
        StringBuilder entity = new StringBuilder( "<html>" );
        entity.append( "<head><title>Error</title></head><body>" );
        if ( subjectOrNull != null )
        {
            entity.append( "<p><pre>" ).append( subjectOrNull ).append( "</pre></p>" );
        }
        entity.append( "<p><pre>" ).append( asString( exception ) ).append( "</pre></p>" );
        entity.append( "</body></html>" );
        return entity.toString();
    }
}
