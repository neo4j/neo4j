/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

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
