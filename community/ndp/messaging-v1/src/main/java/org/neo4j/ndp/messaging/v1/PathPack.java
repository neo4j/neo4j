/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.ndp.messaging.v1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongIntKeyValueArray;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.ndp.messaging.v1.infrastructure.ValueNode;
import org.neo4j.ndp.messaging.v1.infrastructure.ValuePath;
import org.neo4j.ndp.messaging.v1.infrastructure.ValueUnboundRelationship;

public class PathPack
{
    private static final int STRUCT_FIELD_COUNT = 5;
    private static final int NO_SUCH_ID = -1;

    public static class Packer
    {
        private static final int INITIAL_PATH_CAPACITY = 500;

        private final PrimitiveLongIntKeyValueArray nodes =
                new PrimitiveLongIntKeyValueArray( INITIAL_PATH_CAPACITY + 1 );
        private final PrimitiveLongIntKeyValueArray relationships =
                new PrimitiveLongIntKeyValueArray( INITIAL_PATH_CAPACITY );

        private void packNodes( Neo4jPack.Packer packer, Path path )
                throws IOException
        {
            nodes.reset( path.length() + 1 );
            for ( Node node : path.nodes() )
            {
                nodes.putIfAbsent( node.getId(), nodes.size() );
            }
            int size = nodes.size();
            packer.packListHeader( size );
            if ( size > 0 )
            {
                Iterator<Node> iterator = path.nodes().iterator();
                Node node = iterator.next();
                for ( long id : nodes.keys() )
                {
                    while ( node.getId() != id )
                    {
                        node = iterator.next();
                    }
                    ValueNode.pack( packer, node );
                    if ( iterator.hasNext() )
                    {
                        node = iterator.next();
                    }
                }
            }
        }

        private void packRelationships( Neo4jPack.Packer packer, Path path )
                throws IOException
        {
            relationships.reset( path.length() );
            for ( Relationship rel : path.relationships() )
            {
                // relationship indexes are one-based
                relationships.putIfAbsent( rel.getId(), relationships.size() + 1 );
            }
            int size = relationships.size();
            packer.packListHeader( size );
            if ( size > 0 )
            {
                Iterator<Relationship> iterator = path.relationships().iterator();
                Relationship rel = iterator.next();
                for ( long id : relationships.keys() )
                {
                    while ( rel.getId() != id )
                    {
                        rel = iterator.next();
                    }
                    ValueUnboundRelationship.pack( packer, ValueUnboundRelationship.unbind( rel ) );
                    if ( iterator.hasNext() )
                    {
                        rel = iterator.next();
                    }
                }
            }
        }

        public void pack( Neo4jPack.Packer packer, Path path ) throws IOException
        {
            packer.packStructHeader( 3, Neo4jPack.PATH );

            packNodes( packer, path );
            packRelationships( packer, path );

            Node node = null;
            packer.packListHeader( 2 * path.length() );
            int i = 0;
            for ( PropertyContainer entity : path )
            {
                if ( i % 2 == 0 )
                {
                    node = (Node) entity;
                    if ( i > 0 )
                    {
                        int index = nodes.getOrDefault( node.getId(), NO_SUCH_ID );
                        packer.pack( index );
                    }
                }
                else
                {
                    Relationship relationship = (Relationship) entity;
                    int index = relationships.getOrDefault( relationship.getId(), NO_SUCH_ID );
                    if ( node != null && node.getId() == relationship.getStartNode().getId() )
                    {
                        packer.pack( index );
                    }
                    else
                    {
                        packer.pack( -index );
                    }
                }
                i += 1;
            }
        }

    }

    public static class Unpacker
    {

        public ValuePath unpack( Neo4jPack.Unpacker unpacker )
                throws IOException
        {
            assert unpacker.unpackStructHeader() == STRUCT_FIELD_COUNT;
            assert unpacker.unpackStructSignature() == Neo4jPack.PATH;
            return unpackFields( unpacker );
        }

        public ValuePath unpackFields( Neo4jPack.Unpacker unpacker ) throws IOException
        {
            List<Node> nodes = unpackNodes( unpacker );
            List<Relationship> relationships = unpackRelationships( unpacker );
            List<Integer> sequence = unpackSequence( unpacker );
            return new ValuePath( nodes, relationships, sequence );
        }

        private List<Node> unpackNodes( Neo4jPack.Unpacker unpacker )
                throws IOException
        {
            int count = (int) unpacker.unpackListHeader();
            if ( count > 0 )
            {
                List<Node> items = new ArrayList<>( count );
                for ( int i = 0; i < count; i++ )
                {
                    items.add( ValueNode.unpack( unpacker ) );
                }
                return items;
            }
            else
            {
                return Collections.emptyList();
            }
        }

        private List<Relationship> unpackRelationships( Neo4jPack.Unpacker unpacker )
                throws IOException
        {
            int count = (int) unpacker.unpackListHeader();
            if ( count > 0 )
            {
                List<Relationship> items = new ArrayList<>( count );
                for ( int i = 0; i < count; i++ )
                {
                    items.add( ValueUnboundRelationship.unpack( unpacker ) );
                }
                return items;
            }
            else
            {
                return Collections.emptyList();
            }
        }

        private List<Integer> unpackSequence( Neo4jPack.Unpacker unpacker )
                throws IOException
        {
            int count = (int) unpacker.unpackListHeader();
            if ( count > 0 )
            {
                List<Integer> items = new ArrayList<>( count );
                for ( int i = 0; i < count; i++ )
                {
                    items.add( unpacker.unpackInteger() );
                }
                return items;
            }
            else
            {
                return Collections.emptyList();
            }
        }

    }

}
