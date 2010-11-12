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
