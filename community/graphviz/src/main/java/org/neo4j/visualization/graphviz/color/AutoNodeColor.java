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

import java.util.HashSet;
import java.util.Set;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.visualization.graphviz.StyleParameter;

/**
 * Color nodes automatically based on relationships. Use NodeColorConfig to set
 * different modes.
 */
public class AutoNodeColor extends StyleParameter.NodeColor
{
    private final DefaultColorMapping<Set<String>> colors;
    private Direction[] directions;
    private boolean differentiateOnDirection;
    private boolean differentiateOnDirectionOnly = false;
    private ColorMapper<Node> ncm = null;

    public AutoNodeColor()
    {
        NodeColorConfig.DEFAULT.configure( this );
        this.colors = new DefaultColorMapping<Set<String>>();
    }

    public AutoNodeColor( ColorMapper<Node> ncm )
    {
        NodeColorConfig.DEFAULT.configure( this );
        this.colors = new DefaultColorMapping<Set<String>>( ncm.getColors() );
        this.ncm = ncm;
    }

    public AutoNodeColor( NodeColorConfig config )
    {
        config.configure( this );
        this.colors = new DefaultColorMapping<Set<String>>();
    }

    public AutoNodeColor( NodeColorConfig config, ColorMapper<Node> ncm )
    {
        config.configure( this );
        this.colors = new DefaultColorMapping<Set<String>>( ncm.getColors() );
        this.ncm = ncm;
    }

    @Override
    protected String getColor( Node node )
    {
        if ( ncm != null )
        {
            Color color = ncm.getColor( node );
            if ( color != null )
            {
                return colors.getColor( color );
            }
        }
        Set<String> relationshipTypeAndDirections = new HashSet<String>();
        for ( Direction direction : directions )
        {
            if ( differentiateOnDirectionOnly )
            {
                if ( node.hasRelationship( direction ) )
                {
                    relationshipTypeAndDirections.add( direction.name() );
                }
            }
            else
            {
                for ( Relationship relationship : node.getRelationships( direction ) )
                {
                    String key = relationship.getType()
                            .name();
                    if ( differentiateOnDirection )
                    {
                        key += direction.name();
                    }
                    relationshipTypeAndDirections.add( key );
                }
            }
        }
        return colors.getColor( relationshipTypeAndDirections );
    }

    public enum NodeColorConfig
    {
        /**
         * Alias for BOTH_IGNORE_DIRECTION.
         */
        DEFAULT()
        {
            @Override
            void configure( AutoNodeColor style )
            {
                BOTH_IGNORE_DIRECTION.configure( style );
            }
        },
        /**
         * Differentiate on relationship type and direction.
         */
        BOTH()
        {
            @Override
            void configure( AutoNodeColor style )
            {
                style.directions = new Direction[] { Direction.INCOMING,
                        Direction.OUTGOING };
                style.differentiateOnDirection = true;
            }
        },
        /**
         * Differentiate on relationship type, ignore direction.
         */
        BOTH_IGNORE_DIRECTION()
        {
            @Override
            void configure( AutoNodeColor style )
            {
                style.directions = new Direction[] { Direction.INCOMING,
                        Direction.OUTGOING };
                style.differentiateOnDirection = false;
            }
        },
        /**
         * Only look at incoming relationships.
         */
        INCOMING()
        {
            @Override
            void configure( AutoNodeColor style )
            {
                style.directions = new Direction[] { Direction.INCOMING };
                style.differentiateOnDirection = false;
            }
        },
        /**
         * Only look at outgoing relationships.
         */
        OUTGOING()
        {
            @Override
            void configure( AutoNodeColor style )
            {
                style.directions = new Direction[] { Direction.OUTGOING };
                style.differentiateOnDirection = false;
            }
        },
        /**
         * Differentiate only on if a node has incoming or outgoing
         * relationships.
         */
        DIRECTION()
        {
            @Override
            void configure( AutoNodeColor style )
            {
                style.directions = new Direction[] { Direction.INCOMING,
                        Direction.OUTGOING };
                style.differentiateOnDirectionOnly = true;
            }
        };

        abstract void configure( AutoNodeColor style );
    }
}
