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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexRootRepresentation implements Representation
{
    private final URI baseUri;

    public IndexRootRepresentation( URI baseUri )
    {
        this.baseUri = baseUri;
    }
    
    public URI selfUri()
    {
        try
        {
            return new URI( baseUri.toString() + "index" );
        }
        catch ( URISyntaxException e )
        {
            throw new RuntimeException( e );
        }
    }

    public Map<String, Object> serialize()
    {
        Map<String, Object> map = new HashMap<String, Object>();
        // TODO Discover index/fulltext-index, also add relationship index
        
        List<Map<String, Object>> list = new ArrayList<Map<String,Object>>();
        list.add( new IndexRepresentation( baseUri, "node",
                IndexRepresentation.TYPE_LOOKUP ).serialize() );
        map.put( "node", list );
        return map;
    }
}
