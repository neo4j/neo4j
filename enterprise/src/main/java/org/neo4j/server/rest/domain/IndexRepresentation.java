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
import java.util.HashMap;
import java.util.Map;

public class IndexRepresentation implements Representation
{
    public static final String TYPE_LOOKUP = "lookup";
    public static final String TYPE_FULLTEXT = "fulltext";
    
    private final URI baseUri;
    private final String name;
    private final String type;

    public IndexRepresentation( URI baseUri, String name, String type )
    {
        this.baseUri = baseUri;
        this.name = name;
        this.type = type;
    }
    
    public Map<String, Object> serialize()
    {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put( "template", baseUri.toString() + "index/" + name + "/{key}/{value}" );
        map.put( "type", type );
        return map;
    }
}
