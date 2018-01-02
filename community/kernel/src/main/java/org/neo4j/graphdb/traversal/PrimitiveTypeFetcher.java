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

import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;

enum PrimitiveTypeFetcher
{
    NODE
    {
        @Override
        long getId( Path source )
        {
            return source.endNode().getId();
        }

        @Override
        boolean idEquals( Path source, long idToCompare )
        {
            return getId( source ) == idToCompare;
        }
        
        @Override
        boolean containsDuplicates( Path source )
        {
            Set<Node> nodes = new HashSet<Node>();
            for ( Node node : source.reverseNodes() )
                if ( !nodes.add( node ) )
                    return true;
            return false;
        }
    },
    RELATIONSHIP
    {
        @Override
        long getId( Path source )
        {
            return source.lastRelationship().getId();
        }

        @Override
        boolean idEquals( Path source, long idToCompare )
        {
            Relationship relationship = source.lastRelationship();
            return relationship != null && relationship.getId() == idToCompare;
        }

        @Override
        boolean containsDuplicates( Path source )
        {
            Set<Relationship> relationships = new HashSet<Relationship>();
            for ( Relationship relationship : source.reverseRelationships() )
                if ( !relationships.add( relationship ) )
                    return true;
            return false;
        }
    };
    
    abstract long getId( Path path );

    abstract boolean idEquals( Path path, long idToCompare );
    
    abstract boolean containsDuplicates( Path path );
}
