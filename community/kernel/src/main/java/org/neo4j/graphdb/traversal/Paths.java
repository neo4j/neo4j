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
package org.neo4j.graphdb.traversal;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

/**
 * Utilities for {@link org.neo4j.graphdb.Path} objects.
 */
public class Paths
{
    /**
     * Provides hooks to help build a string representation of a {@link org.neo4j.graphdb.Path}.
     * @param <T> the type of {@link org.neo4j.graphdb.Path}.
     */
    public interface PathDescriptor<T extends Path>
    {
        /**
         * Returns a string representation of a {@link org.neo4j.graphdb.Node}.
         * @param path the {@link Path} we're building a string representation
         * from.
         * @param node the {@link org.neo4j.graphdb.Node} to return a string representation of.
         * @return a string representation of a {@link org.neo4j.graphdb.Node}.
         */
        String nodeRepresentation( T path, Node node );

        /**
         * Returns a string representation of a {@link org.neo4j.graphdb.Relationship}.
         * @param path the {@link Path} we're building a string representation
         * from.
         * @param from the previous {@link Node} in the path.
         * @param relationship the {@link org.neo4j.graphdb.Relationship} to return a string
         * representation of.
         * @return a string representation of a {@link org.neo4j.graphdb.Relationship}.
         */
        String relationshipRepresentation( T path, Node from,
                                           Relationship relationship );
    }

    /**
     * The default {@link PathDescriptor} used in common toString()
     * representations in classes implementing {@link Path}.
     * @param <T> the type of {@link Path}.
     */
    public static class DefaultPathDescriptor<T extends Path> implements PathDescriptor<T>
    {
        @Override
        public String nodeRepresentation( Path path, Node node )
        {
            return "(" + node.getId() + ")";
        }

        @Override
        public String relationshipRepresentation( Path path,
                                                  Node from, Relationship relationship )
        {
            String prefix = "--", suffix = "--";
            if ( from.equals( relationship.getEndNode() ) )
            {
                prefix = "<--";
            }
            else
            {
                suffix = "-->";
            }
            return prefix + "[" + relationship.getType().name() + "," +
                    relationship.getId() + "]" + suffix;
        }
    }

    /**
     * Method for building a string representation of a {@link Path}, using
     * the given {@code builder}.
     * @param <T> the type of {@link Path}.
     * @param path the {@link Path} to build a string representation of.
     * @param builder the {@link PathDescriptor} to get
     * {@link Node} and {@link Relationship} representations from.
     * @return a string representation of a {@link Path}.
     */
    public static <T extends Path> String pathToString( T path, PathDescriptor<T> builder )
    {
        Node current = path.startNode();
        StringBuilder result = new StringBuilder();
        for ( Relationship rel : path.relationships() )
        {
            result.append( builder.nodeRepresentation( path, current ) );
            result.append( builder.relationshipRepresentation( path, current, rel ) );
            current = rel.getOtherNode( current );
        }
        if ( null != current ) {
            result.append(builder.nodeRepresentation(path, current));
        }
        return result.toString();
    }

    /**
     * TODO: This method re-binds nodes and relationships. It should not.
     *
     * Returns the default string representation of a {@link Path}. It uses
     * the {@link DefaultPathDescriptor} to get representations.
     * @param path the {@link Path} to build a string representation of.
     * @return the default string representation of a {@link Path}.
     */
    public static String defaultPathToString( Path path )
    {
        return pathToString( path, new DefaultPathDescriptor<>() );
    }

    /**
     * Returns a quite simple string representation of a {@link Path}. It
     * doesn't print relationship types or ids, just directions.
     * @param path the {@link Path} to build a string representation of.
     * @return a quite simple representation of a {@link Path}.
     */
    public static String simplePathToString( Path path )
    {
        return pathToString( path, new DefaultPathDescriptor<Path>()
        {
            @Override
            public String relationshipRepresentation( Path path, Node from,
                                                      Relationship relationship )
            {
                return relationship.getStartNode().equals( from ) ? "-->" : "<--";
            }
        } );
    }

    /**
     * Returns a quite simple string representation of a {@link Path}. It
     * doesn't print relationship types or ids, just directions. it uses the
     * {@code nodePropertyKey} to try to display that property value as in the
     * node representation instead of the node id. If that property doesn't
     * exist, the id is used.
     * @param path the {@link Path} to build a string representation of.
     * @param nodePropertyKey the key of the property value to display
     * @return a quite simple representation of a {@link Path}.
     */
    public static String simplePathToString( Path path, final String nodePropertyKey )
    {
        return pathToString( path, new DefaultPathDescriptor<Path>()
        {
            @Override
            public String nodeRepresentation( Path path, Node node )
            {
                return "(" + node.getProperty( nodePropertyKey, node.getId() ) + ")";
            }

            @Override
            public String relationshipRepresentation( Path path, Node from,
                                                      Relationship relationship )
            {
                return relationship.getStartNode().equals( from ) ? "-->" : "<--";
            }
        } );
    }

    /**
     * Create a new {@link Paths.PathDescriptor} that prints values of listed property keys
     * and id of nodes and relationships if configured so.
     * @param nodeId            true if node id should be included.
     * @param relId             true if relationship id should be included.
     * @param propertyKeys      all property keys that should be included.
     * @param <T>               the type of the {@link Path}
     * @return                  a new {@link Paths.PathDescriptor}
     */
    public static <T extends Path> PathDescriptor<T> descriptorForIdAndProperties( final boolean nodeId,
    final boolean relId, final String... propertyKeys )
    {
        return new Paths.PathDescriptor<T>()
        {
            @Override
            public String nodeRepresentation( T path, Node node )
            {
                String representation = representation( node );
                return "(" + ( nodeId ? node.getId(): "" ) +
                       ( nodeId && !representation.equals( "" ) ? "," : "" ) +
                       representation + ")";
            }

            private String representation( PropertyContainer entity )
            {
                StringBuilder builder = new StringBuilder();
                for ( String key : propertyKeys )
                {
                    Object value = entity.getProperty( key, null );
                    if ( value != null )
                    {
                        if ( builder.length() > 0 )
                        {
                            builder.append( "," );
                        }
                        builder.append( value );
                    }
                }
                return builder.toString();
            }

            @Override
            public String relationshipRepresentation( T path, Node from, Relationship relationship )
            {
                Direction direction = relationship.getEndNode().equals( from ) ? Direction.INCOMING : Direction.OUTGOING;
                StringBuilder builder = new StringBuilder();
                if ( direction.equals( Direction.INCOMING ) )
                {
                    builder.append( "<" );
                }
                builder.append( "--[" + ( relId ? relationship.getId(): "" ) );
                String representation = representation( relationship );
                if ( relId && !representation.equals( "" ) )
                {
                    builder.append( "," );
                }
                builder.append( representation );
                builder.append( "]--" );

                if ( direction.equals( Direction.OUTGOING ) )
                {
                    builder.append( ">" );
                }
                return builder.toString();
            }
        };
    }
}
