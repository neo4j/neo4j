/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.test;

import static org.neo4j.test.GraphDescription.PropType.STRING;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.AutoIndexer;

public class GraphDescription implements GraphDefinition
{
    @Target( { ElementType.METHOD, ElementType.TYPE } )
    @Retention( RetentionPolicy.RUNTIME )
    public @interface Graph
    {
        String[] value() default {};

        NODE[] nodes() default {};

        REL[] relationships() default {};
        
        boolean autoIndexNodes() default false;
        
        boolean autoIndexRelationships() default false;
    }

    @Target( {} )
    public @interface NODE
    {
        String name();

        PROP[] properties() default {};

        boolean setNameProperty() default false;
    }

    @Target( {} )
    public @interface REL
    {
        String name() default "";

        String type();

        String start();

        String end();

        PROP[] properties() default {};

        boolean setNameProperty() default false;
    }

    @Target( {} )
    public @interface PROP
    {
        String key();

        String value();

        PropType type() default STRING;
    }

    @SuppressWarnings( "boxing" )
    public enum PropType
    {
        STRING
        {
            @Override
            String convert( String value )
            {
                return value;
            }
        },
        INTEGER
        {
            @Override
            Long convert( String value )
            {
                return Long.parseLong( value );
            }
        },
        DOUBLE
        {
            @Override
            Double convert( String value )
            {
                return Double.parseDouble( value );
            }
        },
        BOOLEAN
        {
            @Override
            Boolean convert( String value )
            {
                return Boolean.parseBoolean( value );
            }
        };
        abstract Object convert( String value );
    }

    public static TestData.Producer<Map<String, Node>> createGraphFor( final GraphHolder holder, final boolean destroy )
    {
        return new TestData.Producer<Map<String, Node>>()
        {
            @Override
            public Map<String, Node> create( GraphDefinition graph, String title, String documentation )
            {
                return graph.create( holder.graphdb() );
            }

            @Override
            public void destroy( Map<String, Node> product, boolean successful )
            {
                if ( destroy )
                {
                    GraphDescription.destroy( product );
                }
            }
        };
    }

    @Override
    public Map<String, Node> create( GraphDatabaseService graphdb )
    {
        Map<String, Node> result = new HashMap<String, Node>();
        Transaction tx = graphdb.beginTx();
        try
        {
            graphdb.index().getRelationshipAutoIndexer().setEnabled( autoIndexRelationships );
            for ( NODE def : nodes )
            {
                result.put( def.name(),
                        init( graphdb.createNode(), def.setNameProperty() ? def.name() : null, def.properties(), graphdb.index().getNodeAutoIndexer(), autoIndexNodes ));
            }
            for ( REL def : rels )
            {
                init( result.get( def.start() ).createRelationshipTo( result.get( def.end() ),
                        DynamicRelationshipType.withName( def.type() ) ), def.setNameProperty() ? def.name() : null,
                        def.properties(), graphdb.index().getRelationshipAutoIndexer(), autoIndexRelationships );
            }
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        return result;
    }

    private static <T extends PropertyContainer> T init( T entity, String name, PROP[] properties, AutoIndexer<T> autoindex, boolean auto )
    {
        autoindex.setEnabled( auto );
        for ( PROP prop : properties )
        {
            if(auto)
            {
                autoindex.startAutoIndexingProperty( prop.key() );
            }
            entity.setProperty( prop.key(), prop.type().convert( prop.value() ) );
        }
        if ( name != null )
        {
            if(auto)
            {
                autoindex.startAutoIndexingProperty( "name" );
            }
            entity.setProperty( "name", name );
        }
        
        return entity;
    }

    private static final PROP[] NO_PROPS = {};
    private static final NODE[] NO_NODES = {};
    private static final REL[] NO_RELS = {};
    private static final GraphDescription EMPTY = new GraphDescription( NO_NODES, NO_RELS, false, false )
    {
        @Override
        public Map<String, Node> create( GraphDatabaseService graphdb )
        {
            // don't bother with creating a transaction
            return new HashMap<String, Node>();
        }
    };
    private final NODE[] nodes;
    private final REL[] rels;
    private final boolean autoIndexRelationships;
    private final boolean autoIndexNodes;

    public static GraphDescription create( String... definition )
    {
        Map<String, NODE> nodes = new HashMap<String, NODE>();
        List<REL> relationships = new ArrayList<REL>();
        parse( definition, nodes, relationships );
        return new GraphDescription( nodes.values().toArray( NO_NODES ), relationships.toArray( NO_RELS ), false, false );
    }

    public static void destroy( Map<String, Node> nodes )
    {
        if ( nodes.isEmpty() ) return;
        GraphDatabaseService db = nodes.values().iterator().next().getGraphDatabase();
        Transaction tx = db.beginTx();
        try
        {
            for ( Node node : db.getAllNodes() )
            {
                for ( Relationship rel : node.getRelationships() )
                    rel.delete();
                node.delete();
            }
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    static GraphDescription create( Graph graph )
    {
        if ( graph == null )
        {
            return EMPTY;
        }
        Map<String, NODE> nodes = new HashMap<String, NODE>();
        for ( NODE node : graph.nodes() )
        {
            if ( nodes.put( defined( node.name() ), node ) != null )
            {
                throw new IllegalArgumentException( "Node \"" + node.name() + "\" defined more than once" );
            }
        }
        Map<String, REL> rels = new HashMap<String, REL>();
        List<REL> relationships = new ArrayList<REL>();
        for ( REL rel : graph.relationships() )
        {
            createIfAbsent( nodes, rel.start() );
            createIfAbsent( nodes, rel.end() );
            String name = rel.name();
            if ( !name.equals( "" ) )
            {
                if ( rels.put( name, rel ) != null )
                {
                    throw new IllegalArgumentException( "Relationship \"" + name + "\" defined more than once" );
                }
            }
            relationships.add( rel );
        }
        parse( graph.value(), nodes, relationships );
        return new GraphDescription( nodes.values().toArray( NO_NODES ), relationships.toArray( NO_RELS ), graph.autoIndexNodes(), graph.autoIndexRelationships() );
    }

    private static void createIfAbsent( Map<String, NODE> nodes, String name )
    {
        if ( !nodes.containsKey( name ) ) nodes.put( name, new DefaultNode( name ) );
    }

    private static void parse( String[] description, Map<String, NODE> nodes, List<REL> relationships )
    {
        for ( String part : description )
        {
            for ( String line : part.split( "\n" ) )
            {
                String[] components = line.split( " " );
                if ( components.length != 3 )
                {
                    throw new IllegalArgumentException( "syntax error: \"" + line + "\"" );
                }
                createIfAbsent( nodes, defined( components[0] ) );
                createIfAbsent( nodes, defined( components[2] ) );
                relationships.add( new DefaultRel( components[0], components[1], components[2] ) );
            }
        }
    }

    private GraphDescription( NODE[] nodes, REL[] rels, boolean autoIndexNodes, boolean autoIndexRelationships )
    {
        this.nodes = nodes;
        this.rels = rels;
        this.autoIndexNodes = autoIndexNodes;
        this.autoIndexRelationships = autoIndexRelationships;
    }

    static String defined( String name )
    {
        if ( name == null || name.equals( "" ) )
        {
            throw new IllegalArgumentException( "Node name not provided" );
        }
        return name;
    }

    private static class Default
    {
        private final String name;

        Default( String name )
        {
            this.name = "".equals( name ) ? null : name;
        }

        public String name()
        {
            return name;
        }

        public Class<? extends Annotation> annotationType()
        {
            throw new UnsupportedOperationException( "this is not a real annotation" );
        }

        public PROP[] properties()
        {
            return NO_PROPS;
        }

        public boolean setNameProperty()
        {
            return true;
        }
    }

    private static class DefaultNode extends Default implements NODE
    {
        DefaultNode( String name )
        {
            super( name );
        }
    }

    private static class DefaultRel extends Default implements REL
    {
        private final String start, type, end;

        DefaultRel( String start, String type, String end )
        {
            super( null );
            this.type = type;
            this.start = defined( start );
            this.end = defined( end );
        }

        @Override
        public String start()
        {
            return start;
        }

        @Override
        public String end()
        {
            return end;
        }

        @Override
        public String type()
        {
            return type;
        }
    }
}
