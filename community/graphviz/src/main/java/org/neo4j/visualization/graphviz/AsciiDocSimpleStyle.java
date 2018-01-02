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
package org.neo4j.visualization.graphviz;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.visualization.graphviz.color.AutoNodeColor;
import org.neo4j.visualization.graphviz.color.AutoRelationshipTypeColor;
import org.neo4j.visualization.graphviz.color.ColorMapper;
import org.neo4j.visualization.graphviz.color.SimpleNodeColorMapper;
import org.neo4j.visualization.graphviz.color.SimpleRelationshipTypeColorMapper;

/**
 * Simplified style with no node labels and no property type names in the
 * output. Look at {@link SimpleNodeColorMapper} and
 * {@link SimpleRelationshipTypeColorMapper} for easy ways to add predefined
 * colors, consumable by static factory methods of this class. For more control,
 * provide {@link NodeStyle} and {@link RelationshipStyle} implementations.
 * 
 * @author anders
 */
public class AsciiDocSimpleStyle extends AsciiDocStyle
{
    private AsciiDocSimpleStyle()
    {
        this( false, true );
    }

    private AsciiDocSimpleStyle( NodeStyle nodeStyle,
            RelationshipStyle edgeStyle )
    {
        super( nodeStyle, edgeStyle );
    }

    private AsciiDocSimpleStyle( boolean autoColoredNodes,
            boolean autoColoredRelationships )
    {
        super( new SimpleNodeStyle( defaultNodeConfig( autoColoredNodes ) ),
                new DefaultRelationshipStyle(
                        defaultRelationshipConfig( autoColoredRelationships ) ) );
    }

    private AsciiDocSimpleStyle(
            ColorMapper<RelationshipType> relationshipTypeColors,
            boolean autoColoredNodes )
    {
        super(
                new SimpleNodeStyle( defaultNodeConfig( autoColoredNodes ) ),
                new DefaultRelationshipStyle(
                        new DefaultStyleConfiguration(
                                AsciiDocStyle.SIMPLE_PROPERTY_STYLE,
                                new AutoRelationshipTypeColor(
                                        relationshipTypeColors ) ) ) );
    }

    private AsciiDocSimpleStyle( boolean autoColoredRelationships,
            ColorMapper<Node> nodeColors )
    {
        super( new SimpleNodeStyle( new DefaultStyleConfiguration(
                new AutoNodeColor( nodeColors ) ) ),
                new DefaultRelationshipStyle(
                        defaultRelationshipConfig( autoColoredRelationships ) ) );
    }

    private AsciiDocSimpleStyle( ColorMapper<Node> nodeColors,
            ColorMapper<RelationshipType> relationshipTypeColors )
    {
        super(
                new SimpleNodeStyle( new DefaultStyleConfiguration(
                        new AutoNodeColor( nodeColors ) ) ),
                new DefaultRelationshipStyle(
                        new DefaultStyleConfiguration(
                                AsciiDocStyle.SIMPLE_PROPERTY_STYLE,
                                new AutoRelationshipTypeColor(
                                        relationshipTypeColors ) ) ) );
    }

    private static DefaultStyleConfiguration defaultRelationshipConfig(
            boolean autoColoredRelationships )
    {
        if ( autoColoredRelationships )
        {
            return new DefaultStyleConfiguration(
                    AsciiDocStyle.SIMPLE_PROPERTY_STYLE,
                    new AutoRelationshipTypeColor() );
        }
        else
        {
            return AsciiDocStyle.PLAIN_STYLE;
        }
    }

    private static DefaultStyleConfiguration defaultNodeConfig(
            boolean autoColoredNodes )
    {
        if ( autoColoredNodes )
        {
            return new DefaultStyleConfiguration(
                    AsciiDocStyle.SIMPLE_PROPERTY_STYLE,
                    new AutoNodeColor() );
        }
        else
        {
            return AsciiDocStyle.PLAIN_STYLE;
        }
    }

    public static AsciiDocSimpleStyle withoutColors()
    {
        return new AsciiDocSimpleStyle( false, false );
    }

    public static AsciiDocSimpleStyle withAutomaticRelationshipTypeColors()
    {
        return new AsciiDocSimpleStyle( false, true );
    }

    public static AsciiDocSimpleStyle withAutomaticNodeColors()
    {
        return new AsciiDocSimpleStyle( true, false );
    }

    public static AsciiDocSimpleStyle withAutomaticNodeAndRelationshipTypeColors()
    {
        return new AsciiDocSimpleStyle( true, true );
    }

    public static AsciiDocSimpleStyle withPredefinedRelationshipTypeColors(
            ColorMapper<RelationshipType> relationshipTypeColors )
    {
        return new AsciiDocSimpleStyle( relationshipTypeColors, false );
    }

    public static AsciiDocSimpleStyle withAudomaticNodeAndPredefinedRelationshipTypeColors(
            ColorMapper<RelationshipType> relationshipTypeColors )
    {
        return new AsciiDocSimpleStyle( relationshipTypeColors, true );
    }

    public static AsciiDocSimpleStyle withPredefinedNodeColors(
            ColorMapper<Node> nodeColors )
    {
        return new AsciiDocSimpleStyle( false, nodeColors );
    }

    public static AsciiDocSimpleStyle withPredefinedNodeColorsAndAutomaticRelationshipTypeColors(
            ColorMapper<Node> nodeColors )
    {
        return new AsciiDocSimpleStyle( true, nodeColors );
    }

    public static AsciiDocSimpleStyle withPredefinedNodeAndRelationshipTypeColors(
            ColorMapper<Node> nodeColors,
            ColorMapper<RelationshipType> relationshipTypeColors )
    {
        return new AsciiDocSimpleStyle( nodeColors, relationshipTypeColors );
    }

    public static AsciiDocSimpleStyle withPredefinedNodeAndRelationshipStyles(
            NodeStyle nodeStyle, RelationshipStyle edgeStyle )
    {
        return new AsciiDocSimpleStyle( nodeStyle, edgeStyle );
    }
}
