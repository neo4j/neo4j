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

package org.neo4j.server.rest.domain;

import static org.neo4j.server.rest.domain.JsonRenderers.asString;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.server.NeoServer;
import org.neo4j.server.database.DatabaseBlockedException;
import org.neo4j.server.rest.domain.HtmlHelper.ObjectType;

public enum HtmlRenderers implements Renderer
{
    ROOT
    {
        @Override
        public String render( Representation... oneOrManyRepresentations )
        {
            Map<Object, Object> map = new HashMap<Object, Object>();
            Map<?, ?> serialized = (Map<?, ?>) oneOrManyRepresentations[0].serialize();
            RepresentationUtil.transfer( serialized, map, "index",
                    "reference node" );
            return HtmlHelper.from( map, ObjectType.ROOT );
        }
    },

    /**
     * For one node, for a list of node (from an index lookup use another
     * renderer, because we maybe don't want the relationship form to be
     * displayed for every single node... it'd be quite bloated.
     */
    NODE
    {
        @Override
        public String render( Representation... oneOrManyRepresentations )
        {
            Representation rep = oneOrManyRepresentations[0];
            Map<?, ?> serialized = (Map<?, ?>) rep.serialize();
            String javascript = "";
            StringBuilder builder = HtmlHelper.start( ObjectType.NODE,
                    javascript );
            HtmlHelper.append(
                    builder,
                    Collections.singletonMap( "data", serialized.get( "data" ) ),
                    ObjectType.NODE );
            builder.append( "<form action='javascript:neo4jHtmlBrowse.getRelationships();'><fieldset><legend>Get relationships</legend>\n" );
            builder.append( "<label for='direction'>with direction</label>\n"
                            + "<select id='direction'>" );
            builder.append( "<option value='"
                            + serialized.get( "all typed relationships" )
                            + "'>all</option>" );
            builder.append( "<option value='"
                            + serialized.get( "incoming typed relationships" )
                            + "'>in</option>" );
            builder.append( "<option value='"
                            + serialized.get( "outgoing typed relationships" )
                            + "'>out</option>" );
            builder.append( "</select>\n" );
            builder.append( "<label for='types'>for type(s)</label><select id='types' multiple='multiple'>" );

            try
            {
                for ( RelationshipType type : NeoServer.server().database().db.getRelationshipTypes() )
                {
                    builder.append( "<option selected='selected' value='"
                                    + type.name() + "'>" + type.name()
                                    + "</option>" );
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
    },

    RELATIONSHIPS
    {
        @Override
        public String render( Representation... oneOrManyRepresentations )
        {
            if ( oneOrManyRepresentations.length == 0 )
            {
                StringBuilder builder = HtmlHelper.start(
                        ObjectType.RELATIONSHIP, null );
                HtmlHelper.appendMessage( builder, "No relationships found" );
                return HtmlHelper.end( builder );
            }
            else
            {
                Collection<Object> list = new ArrayList<Object>();
                for ( Representation rep : oneOrManyRepresentations )
                {
                    Map<?, ?> serialized = (Map<?, ?>) rep.serialize();
                    Map<Object, Object> map = new LinkedHashMap<Object, Object>();
                    RepresentationUtil.transfer( serialized, map, "self",
                            "type", "data", "start", "end" );
                    list.add( map );
                }
                return HtmlHelper.from( list, ObjectType.RELATIONSHIP );
            }
        }
    },

    RELATIONSHIP
    {
        @Override
        public String render( Representation... oneOrManyRepresentations )
        {
            Map<?, ?> serialized = (Map<?, ?>) oneOrManyRepresentations[0].serialize();
            Map<Object, Object> map = new LinkedHashMap<Object, Object>();
            RepresentationUtil.transfer( serialized, map, "type", "data",
                    "start", "end" );
            return HtmlHelper.from( map, ObjectType.RELATIONSHIP );
        }
    },

    INDEX_ROOT
    {
        @Override
        public String render( Representation... oneOrManyRepresentations )
        {
            Map<?, ?> serialized = (Map<?, ?>) oneOrManyRepresentations[0].serialize();
            String javascript = "";
            StringBuilder builder = HtmlHelper.start( ObjectType.INDEX_ROOT,
                    javascript );
            int counter = 0;
            for ( String objectType : new String[] { "node", "relationship" } )
            {
                List<?> list = (List<?>) serialized.get( objectType );
                if ( list == null )
                {
                    continue;
                }
                builder.append( "<ul>" );
                for ( Object indexMapObject : list )
                {
                    builder.append( "<li>" );
                    Map<?, ?> indexMap = (Map<?, ?>) indexMapObject;
                    String keyId = "key_" + counter;
                    String valueId = "value_" + counter;
                    builder.append( "<form action='javascript:neo4jHtmlBrowse.search(\""
                                    + indexMap.get( "template" )
                                    + "\",\""
                                    + keyId
                                    + "\",\""
                                    + valueId
                                    + "\");'><fieldset><legend>"
                                    + indexMap.get( "type" ) + "</legend>\n" );
                    builder.append( "<label for='" + keyId
                                    + "'>Key</label><input id='" + keyId
                                    + "'>\n" );
                    builder.append( "<label for='" + valueId
                                    + "'>Value</label><input id='" + valueId
                                    + "'>\n" );
                    builder.append( "<button>Search</button>\n" );
                    builder.append( "</fieldset></form>\n" );
                    builder.append( "</li>\n" );
                    counter++;
                }
                builder.append( "</ul>" );
            }
            return HtmlHelper.end( builder );
        }
    },

    NODES
    {
        @Override
        public String render( Representation... oneOrManyRepresentations )
        {
            StringBuilder builder = HtmlHelper.start( "Index hits", null );
            if ( oneOrManyRepresentations.length == 0 )
            {
                HtmlHelper.appendMessage( builder, "No index hits" );
                return HtmlHelper.end( builder );
            }
            else
            {
                for ( Representation rep : oneOrManyRepresentations )
                {
                    Map<Object, Object> map = new LinkedHashMap<Object, Object>();
                    Map<?, ?> serialized = (Map<?, ?>) rep.serialize();
                    RepresentationUtil.transfer( serialized, map, "self",
                            "data" );
                    HtmlHelper.append( builder, map, ObjectType.NODE );
                }
                return HtmlHelper.end( builder );
            }
        }
    }

    ;
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
            entity.append( "<p><pre>" + subjectOrNull + "</pre></p>" );
        }
        entity.append( "<p><pre>" + asString( exception ) + "</pre></p>" );
        entity.append( "</body></html>" );
        return entity.toString();
    }
}
