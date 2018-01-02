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
package org.neo4j.visualization.graphviz.color;

import java.util.Collection;
import java.util.Map;
import org.neo4j.graphdb.Node;

/**
 * Wrap a {@code Map<Object, Color>} to expose it as a {@code ColorMapper<Node>} from a property
 * value.
 */
public class SimpleNodeColorMapper implements ColorMapper<Node>
{
    private final Map<Object, Color> mappings;
    private final String propertyKey;

    /**
     * Map from property values to colors.
     * 
     * @param propertyKey the key to the value we will map
     * @param mappings property values to color mappings
     */
    public SimpleNodeColorMapper( String propertyKey,
            Map<Object, Color> mappings )
    {
        this.propertyKey = propertyKey;
        this.mappings = mappings;
    }

    @Override
    public Color getColor( Node entity )
    {
        return mappings.get( entity.getProperty( propertyKey, null ) );
    }

    @Override
    public Collection<Color> getColors()
    {
        return mappings.values();
    }
}
