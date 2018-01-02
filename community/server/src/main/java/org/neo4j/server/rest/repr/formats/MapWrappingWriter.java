/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.rest.repr.formats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.server.rest.repr.ListWriter;
import org.neo4j.server.rest.repr.MappingWriter;

public class MapWrappingWriter extends MappingWriter
{
    final Map<String, Object> data;
    final boolean interactive;

    public MapWrappingWriter( Map<String, Object> data )
    {
        this( data, false );
    }

    public MapWrappingWriter( Map<String, Object> data, boolean interactive )
    {
        this.data = data;
        this.interactive = interactive;
    }

    @Override
    final protected boolean isInteractive()
    {
        return interactive;
    }

    @Override
    protected ListWriter newList( String type, String key )
    {
        List<Object> list = new ArrayList<Object>();
        data.put( key, list );
        return new ListWrappingWriter( list, interactive );
    }

    @Override
    protected MappingWriter newMapping( String type, String key )
    {
        Map<String, Object> map = new HashMap<String, Object>();
        data.put( key, map );
        return new MapWrappingWriter( map, interactive );
    }

    @Override
    protected void writeValue( String type, String key, Object value )
    {
        data.put( key, value );
    }

    @Override
    protected void done()
    {
    }
}
