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
package org.neo4j.visualization;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.walk.Visitor;
import org.neo4j.walk.Walker;

public interface SubgraphMapper
{
    String getSubgraphFor( Node node );

    public abstract class SubgraphMappingWalker extends Walker
    {
        private final SubgraphMapper mapper;

        protected SubgraphMappingWalker( SubgraphMapper mapper )
        {
            this.mapper = mapper;
        }

        private Map<String, Collection<Node>> subgraphs;
        private Collection<Node> generic;
        private Collection<Relationship> relationships;

        private synchronized void initialize()
        {
            if ( subgraphs != null ) return;
            Map<Node, String> mappings = new HashMap<Node, String>();
            subgraphs = new HashMap<String, Collection<Node>>();
            generic = new ArrayList<Node>();
            for ( Node node : nodes() )
            {
                if ( mappings.containsKey( node ) ) continue;
                String subgraph = subgraphFor( node );
                if ( !subgraphs.containsKey( subgraph ) && subgraph != null )
                {
                    subgraphs.put( subgraph, new ArrayList<Node>() );
                }
                mappings.put( node, subgraph );
            }
            relationships = new ArrayList<Relationship>();
            for ( Relationship rel : relationships() )
            {
                if ( mappings.containsKey( rel.getStartNode() )
                     && mappings.containsKey( rel.getEndNode() ) )
                {
                    relationships.add( rel );
                }
            }
            for ( Map.Entry<Node, String> e : mappings.entrySet() )
            {
                ( e.getValue() == null ? generic : subgraphs.get( e.getValue() ) ).add( e.getKey() );
            }
        }

        private String subgraphFor( Node node )
        {
            return mapper == null ? null : mapper.getSubgraphFor( node );
        }

        @Override
        public final <R, E extends Throwable> R accept( Visitor<R, E> visitor ) throws E
        {
            initialize();
            for ( Map.Entry<String, Collection<Node>> subgraph : subgraphs.entrySet() )
            {
                Visitor<R, E> subVisitor = visitor.visitSubgraph( subgraph.getKey() );
                for ( Node node : subgraph.getValue() )
                {
                    subVisitor.visitNode( node );
                }
                subVisitor.done();
            }
            for ( Node node : generic )
            {
                visitor.visitNode( node );
            }
            for ( Relationship relationship : relationships )
            {
                visitor.visitRelationship( relationship );
            }
            return visitor.done();
        }

        protected abstract Iterable<Node> nodes();

        protected abstract Iterable<Relationship> relationships();
    }
}
