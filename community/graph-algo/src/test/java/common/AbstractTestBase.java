/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package common;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.tooling.GlobalGraphOperations;

public abstract class AbstractTestBase
{
    public interface GraphDatabaseWrapper
    {
        GraphDatabaseService wrap( GraphDatabaseService graphdb );
    }

    private static GraphDatabaseService graphdb;

    @BeforeClass
    public static final void beforeSuite()
    {
        graphdb = new ImpermanentGraphDatabase();
    }

    @AfterClass
    public static final void afterSuite()
    {
        graphdb.shutdown();
        graphdb = null;
    }

    protected static final void wrapGraphDatabase( GraphDatabaseWrapper wrapper )
    {
        graphdb = wrapper.wrap( graphdb );
    }

    protected static final Node referenceNode()
    {
        return graphdb.getReferenceNode();
    }

    protected static final Node getNode( long id )
    {
        return graphdb.getNodeById( id );
    }

    protected static final Transaction beginTx()
    {
        return graphdb.beginTx();
    }

    protected static void removeAllNodes( boolean removeReference )
    {
        Transaction tx = graphdb.beginTx();
        try
        {
            Node reference = removeReference ? null
                    : graphdb.getReferenceNode();
            for ( Node node : GlobalGraphOperations.at( graphdb ).getAllNodes() )
            {
                for ( Relationship rel : node.getRelationships() )
                {
                    rel.delete();
                }
                if ( !node.equals( reference ) )
                {
                    node.delete();
                }
            }
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    protected static void createGraph( String... description )
    {
        createGraph( new GraphDescription( description ) );
    }

    protected static Node createGraph( GraphDefinition graph )
    {
        Transaction tx = graphdb.beginTx();
        try
        {
            Node root = graph.create( graphdb );
            tx.success();
            return root;
        }
        finally
        {
            tx.finish();
        }
    }

    protected Node getNodeWithName( String name )
    {
        for ( Node node : GlobalGraphOperations.at( graphdb ).getAllNodes() )
        {
            String nodeName = (String) node.getProperty( "name", null );
            if ( nodeName != null && nodeName.equals( name ) )
            {
                return node;
            }
        }
        return null;
    }

    private static void deleteFileOrDirectory( File file )
    {
        if ( !file.exists() )
        {
            return;
        }

        if ( file.isDirectory() )
        {
            for ( File child : file.listFiles() )
            {
                deleteFileOrDirectory( child );
            }
        }
        else
        {
            file.delete();
        }
    }

    protected static final Representation<PropertyContainer> NAME_PROPERTY_REPRESENTATION = new PropertyRepresentation(
            "name" );

    protected static final Representation<Relationship> RELATIONSHIP_TYPE_REPRESENTATION = new Representation<Relationship>()
    {
        public String represent( Relationship item )
        {
            return item.getType().name();
        }
    };

    protected interface Representation<T>
    {
        String represent( T item );
    }

    protected static final class PropertyRepresentation implements
            Representation<PropertyContainer>
    {
        public PropertyRepresentation( String key )
        {
            this.key = key;
        }

        private final String key;

        public String represent( PropertyContainer item )
        {
            return (String) item.getProperty( key );
        }
    }

    protected static final class RelationshipRepresentation implements
            Representation<Relationship>
    {
        private final Representation<? super Node> nodes;
        private final Representation<? super Relationship> rel;

        public RelationshipRepresentation( Representation<? super Node> nodes )
        {
            this( nodes, RELATIONSHIP_TYPE_REPRESENTATION );
        }

        public RelationshipRepresentation( Representation<? super Node> nodes,
                Representation<? super Relationship> rel )
        {
            this.nodes = nodes;
            this.rel = rel;
        }

        public String represent( Relationship item )
        {
            return nodes.represent( item.getStartNode() ) + " "
                   + rel.represent( item ) + " "
                   + nodes.represent( item.getEndNode() );
        }
    }

    protected static final class NodePathRepresentation implements
            Representation<Path>
    {
        private final Representation<? super Node> nodes;

        public NodePathRepresentation( Representation<? super Node> nodes )
        {
            this.nodes = nodes;

        }

        public String represent( Path item )
        {
            StringBuilder builder = new StringBuilder();
            for ( Node node : item.nodes() )
            {
                builder.append( builder.length() > 0 ? "," : "" );
                builder.append( nodes.represent( node ) );
            }
            return builder.toString();
        }
    }

    protected static <T> void expect( Iterable<? extends T> items,
            Representation<T> representation, String... expected )
    {
        expect( items, representation, new HashSet<String>(
                Arrays.asList( expected ) ) );
    }

    protected static <T> void expect( Iterable<? extends T> items,
            Representation<T> representation, Set<String> expected )
    {
        Transaction tx = beginTx();
        try
        {
            for ( T item : items )
            {
                String repr = representation.represent( item );
                assertTrue( repr + " not expected ", expected.remove( repr ) );
            }
            tx.success();
        }
        finally
        {
            tx.finish();
        }

        if ( !expected.isEmpty() )
        {
            fail( "The exepected elements " + expected + " were not returned." );
        }
    }

    protected static void expectNodes( Traverser traverser, String... nodes )
    {
        expect( traverser.nodes(), NAME_PROPERTY_REPRESENTATION, nodes );
    }

    protected static void expectRelationships( Traverser traverser,
            String... relationships )
    {
        expect( traverser.relationships(), new RelationshipRepresentation(
                NAME_PROPERTY_REPRESENTATION ), relationships );
    }

    protected static void expectPaths( Traverser traverser, String... paths )
    {
        expectPaths( traverser, new HashSet<String>( Arrays.asList( paths ) ) );
    }

    protected static void expectPaths( Traverser traverser, Set<String> expected )
    {
        expect( traverser, new NodePathRepresentation(
                NAME_PROPERTY_REPRESENTATION ), expected );
    }

//    private static String relationshipRepresentation( Relationship relationship )
//    {
//        return relationship.getStartNode().getProperty( "name" ) + " "
//               + relationship.getType().name() + " "
//               + relationship.getEndNode().getProperty( "name" );
//    }
//
//    private static String nodePathRepresention( Path path )
//    {
//        StringBuilder builder = new StringBuilder();
//        for ( Node node : path.nodes() )
//        {
//            builder.append( builder.length() > 0 ? "," : "" );
//            builder.append( node.getProperty( "name" ) );
//        }
//        return builder.toString();
//    }
}
