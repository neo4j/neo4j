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
package org.neo4j.test;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.tooling.GlobalGraphOperations;

import static java.util.Arrays.asList;
import static java.util.Arrays.copyOfRange;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.test.GraphDescription.PropType.ERROR;
import static org.neo4j.test.GraphDescription.PropType.STRING;

public class GraphDescription implements GraphDefinition
{
    @Inherited
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
        
        LABEL[] labels() default {};

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

        PropType componentType() default ERROR;
    }
    
    @Target( {} )
    public @interface LABEL
    {
        String value();
    }

    @SuppressWarnings( "boxing" )
    public enum PropType
    {

        ARRAY
        {
            @Override Object convert( PropType componentType, String value )
            {
                String[] items  = value.split( " *, *" );
                Object[] result = (Object[]) Array.newInstance( componentType.componentClass(), items.length );
                for ( int i =0 ; i < items.length; i++ )
                {
                    result[i] = componentType.convert( items[i] );
                }
                return result;
            }
        },
        STRING
        {
            @Override
            String convert( String value )
            {
                return value;
            }

            @Override Class<?> componentClass()
            {
                return String.class;
            }
        },
        INTEGER
        {
            @Override
            Long convert( String value )
            {
                return Long.parseLong( value );
            }

            @Override Class<?> componentClass()
            {
                return Long.class;
            }
        },
        DOUBLE
        {
            @Override
            Double convert( String value )
            {
                return Double.parseDouble( value );
            }

            @Override Class<?> componentClass()
            {
                return Double.class;
            }
        },
        BOOLEAN
        {
            @Override
            Boolean convert( String value )
            {
                return Boolean.parseBoolean( value );
            }

            @Override Class<?> componentClass()
            {
                return Boolean.class;
            }
        },

        ERROR{
        };

        Class<?> componentClass() {
            throw new UnsupportedOperationException( "Not implemented for property type" + name() );
        }

        Object convert( String value ) {
            throw new UnsupportedOperationException( "Not implemented for property type"  + name() );
        }

        Object convert( PropType componentType, String value ) {
            throw new UnsupportedOperationException( "Not implemented for property type"  + name() );
        }
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
        Map<String, Node> result = new HashMap<>();
        try ( Transaction tx = graphdb.beginTx() )
        {
            graphdb.index().getRelationshipAutoIndexer().setEnabled( autoIndexRelationships );
            for ( NODE def : nodes )
            {
                Node node = init( graphdb.createNode(), def.setNameProperty() ? def.name() : null, def.properties(), graphdb.index().getNodeAutoIndexer(), autoIndexNodes );
                for ( LABEL label : def.labels() )
                {
                    node.addLabel( label( label.value() ) );
                }
                result.put( def.name(), node );
            }
            for ( REL def : rels )
            {
                init( result.get( def.start() ).createRelationshipTo( result.get( def.end() ),
                        DynamicRelationshipType.withName( def.type() ) ), def.setNameProperty() ? def.name() : null,
                        def.properties(), graphdb.index().getRelationshipAutoIndexer(), autoIndexRelationships );
            }
            tx.success();
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
            PropType tpe = prop.type();
            switch(tpe) {
                case ARRAY:
                    entity.setProperty( prop.key(), tpe.convert( prop.componentType(), prop.value() ) );
                    break;
                default:
                    entity.setProperty( prop.key(), prop.type().convert( prop.value() ) );
            }
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
            return new HashMap<>();
        }
    };
    private final NODE[] nodes;
    private final REL[] rels;
    private final boolean autoIndexRelationships;
    private final boolean autoIndexNodes;

    public static GraphDescription create( String... definition )
    {
        Map<String, NODE> nodes = new HashMap<>();
        List<REL> relationships = new ArrayList<>();
        parse( definition, nodes, relationships );
        return new GraphDescription( nodes.values().toArray( NO_NODES ), relationships.toArray( NO_RELS ), false, false );
    }

    public static void destroy( Map<String, Node> nodes )
    {
        if ( nodes.isEmpty() ) return;
        GraphDatabaseService db = nodes.values().iterator().next().getGraphDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            for ( Node node : GlobalGraphOperations.at( db ).getAllNodes() )
            {
                for ( Relationship rel : node.getRelationships() )
                    rel.delete();
                node.delete();
            }
            tx.success();
        }
    }

    public static GraphDescription create( Graph graph )
    {
        if ( graph == null )
        {
            return EMPTY;
        }
        Map<String, NODE> nodes = new HashMap<>();
        for ( NODE node : graph.nodes() )
        {
            if ( nodes.put( defined( node.name() ), node ) != null )
            {
                throw new IllegalArgumentException( "Node \"" + node.name() + "\" defined more than once" );
            }
        }
        Map<String, REL> rels = new HashMap<>();
        List<REL> relationships = new ArrayList<>();
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

    private static void createIfAbsent( Map<String, NODE> nodes, String name, String ... labels )
    {
        if ( !nodes.containsKey( name ) )
        {
            nodes.put( name, new DefaultNode( name, labels ) );
        }
        else
        {
            NODE preexistingNode = nodes.get( name );
            // Join with any new labels
            HashSet<String> joinedLabels = new HashSet<>( asList( labels ) );
            for ( LABEL label : preexistingNode.labels() )
            {
                joinedLabels.add( label.value() );
            }

            String[] labelNameArray = joinedLabels.toArray(new String[joinedLabels.size()]);
            nodes.put( name, new NodeWithAddedLabels(preexistingNode, labelNameArray));
        }
    }

    private static String parseAndCreateNodeIfAbsent( Map<String, NODE> nodes, String descriptionToParse )
    {
        String[] parts = descriptionToParse.split( ":" );
        if ( parts.length == 0 )
        {
            throw new IllegalArgumentException( "Empty node names are not allowed." );
        }

        createIfAbsent( nodes, parts[0], copyOfRange( parts, 1, parts.length ) );
        return parts[0];

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

                String startName = parseAndCreateNodeIfAbsent( nodes, defined( components[0] ) );
                String endName = parseAndCreateNodeIfAbsent( nodes, defined( components[2] ) );
                relationships.add( new DefaultRel( startName, components[1], endName ) );
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
        private final LABEL[] labels;

        DefaultNode( String name, String[] labelNames )
        {
            super( name );
            labels = new LABEL[labelNames.length];
            for(int i=0;i<labelNames.length;i++)
            {
                labels[i] = new DefaultLabel(labelNames[i]);
            }
        }

        @Override
        public LABEL[] labels()
        {
            return labels;
        }
    }

    /*
     * Used if the user has defined the same node twice, with different labels, this combines
     * all labels the user has added.
     */
    private static class NodeWithAddedLabels implements NODE
    {
        private final LABEL[] labels;
        private final NODE inner;

        NodeWithAddedLabels( NODE inner, String[] labelNames )
        {
            this.inner = inner;
            labels = new LABEL[labelNames.length];
            for(int i=0;i<labelNames.length;i++)
            {
                labels[i] = new DefaultLabel(labelNames[i]);
            }
        }

        @Override
        public String name()
        {
            return inner.name();
        }

        @Override
        public PROP[] properties()
        {
            return inner.properties();
        }

        @Override
        public LABEL[] labels()
        {
            return labels;
        }

        @Override
        public boolean setNameProperty()
        {
            return inner.setNameProperty();
        }

        @Override
        public Class<? extends Annotation> annotationType()
        {
            return inner.annotationType();
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

    private static class DefaultLabel implements LABEL
    {

        private final String name;

        public DefaultLabel( String name )
        {

            this.name = name;
        }

        @Override
        public Class<? extends Annotation> annotationType()
        {
            throw new UnsupportedOperationException( "this is not a real annotation" );
        }

        @Override
        public String value()
        {
            return name;
        }
    }


}
