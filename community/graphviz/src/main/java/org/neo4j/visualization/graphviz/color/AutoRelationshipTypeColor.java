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

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.visualization.graphviz.StyleParameter.RelationshipTypeColor;

/**
 * Color relationships automatically based on the type.
 */
public class AutoRelationshipTypeColor extends RelationshipTypeColor
{
    private final DefaultColorMapping<String> colors;
    private ColorMapper<RelationshipType> rtcm = null;

    /**
     * Use default color mappings.
     */
    public AutoRelationshipTypeColor()
    {
        this.colors = new DefaultColorMapping<String>();
    }

    /**
     * Reserve and map colors for relationship types. Any non-mapped
     * relationship types will be automatically mapped to non-reserved colors.
     * 
     * @param rtcm relationship type to color mapper
     */
    public AutoRelationshipTypeColor( ColorMapper<RelationshipType> rtcm )
    {
        this.rtcm = rtcm;
        this.colors = new DefaultColorMapping<String>( rtcm.getColors() );
    }

    @Override
    protected String getColor( RelationshipType type )
    {
        if ( rtcm != null )
        {
            Color color = rtcm.getColor( type );
            if ( color != null )
            {
                return colors.getColor( color );
            }
        }
        return colors.getColor( type.name() );
    }
}
