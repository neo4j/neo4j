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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A color mapper matching keys to colors, producing string representations of
 * the colors.
 * 
 * The mapper uses the colors from {@link Color}, in the order they are defined
 * there. When running out of colors, it's starts over from the first one again.
 */
public class DefaultColorMapping<E>
{
    private final List<String> availableColors = new ArrayList<String>();
    private int usedAvailableColors = 0;
    private final Map<E, String> colorMappings = new HashMap<E, String>();

    /**
     * Map colors using the full set of colors in {@link Color}.
     */
    public DefaultColorMapping()
    {
        this( Collections.<Color>emptyList() );
    }

    /**
     * Map colors from {@link Color} while excluding the reserved colors.
     * 
     * Both the dark and light variation of the colors are excluded, even though
     * only the dark variation will be used by the reserved mapping.
     * 
     * @param reservedColors colors this mapper shouldn't use
     */
    public DefaultColorMapping( Collection<Color> reservedColors )
    {
        Color[] existingColors = Color.values();
        // add the dark colors first, then the lighter ones
        for ( Color color : existingColors )
        {
            if ( !reservedColors.contains( color ) )
            {
                availableColors.add( color.dark );
            }
        }
        for ( Color color : existingColors )
        {
            if ( !reservedColors.contains( color ) )
            {
                availableColors.add( color.light );
            }
        }
    }

    /**
     * Get the color for a key as a string.
     * 
     * @param key the key
     * @return the color as a String
     */
    protected String getColor( E key )
    {
        String color = colorMappings.get( key );
        if ( color == null )
        {
            color = availableColors.get( usedAvailableColors
                                         % availableColors.size() );
            usedAvailableColors++;
            colorMappings.put( key, color );
        }
        return color;
    }

    /**
     * Get the color string value for a reserved color.
     * 
     * @param color
     * @return
     */
    protected String getColor( Color color )
    {
        return color.dark;
    }
}
