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
package org.neo4j.server.rest.web;

import com.sun.jersey.api.core.HttpContext;
import org.apache.lucene.search.Sort;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.function.Function;
import org.neo4j.function.Predicate;
import org.neo4j.function.Predicates;
import org.neo4j.graphalgo.CommonEvaluators;
import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Expander;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.ReadableIndex;
import org.neo4j.graphdb.index.UniqueFactory;
import org.neo4j.graphdb.index.UniqueFactory.UniqueEntity;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.index.lucene.QueryContext;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.Traversal;
import org.neo4j.server.database.InjectableProvider;
import org.neo4j.server.rest.domain.EndNodeNotFoundException;
import org.neo4j.server.rest.domain.PropertySettingStrategy;
import org.neo4j.server.rest.domain.RelationshipExpanderBuilder;
import org.neo4j.server.rest.domain.StartNodeNotFoundException;
import org.neo4j.server.rest.domain.TraversalDescriptionBuilder;
import org.neo4j.server.rest.domain.TraverserReturnType;
import org.neo4j.server.rest.paging.Lease;
import org.neo4j.server.rest.paging.LeaseManager;
import org.neo4j.server.rest.paging.PagedTraverser;
import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.ConstraintDefinitionRepresentation;
import org.neo4j.server.rest.repr.DatabaseRepresentation;
import org.neo4j.server.rest.repr.IndexDefinitionRepresentation;
import org.neo4j.server.rest.repr.IndexRepresentation;
import org.neo4j.server.rest.repr.IndexedEntityRepresentation;
import org.neo4j.server.rest.repr.InvalidArgumentsException;
import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.server.rest.repr.NodeIndexRepresentation;
import org.neo4j.server.rest.repr.NodeIndexRootRepresentation;
import org.neo4j.server.rest.repr.NodeRepresentation;
import org.neo4j.server.rest.repr.PathRepresentation;
import org.neo4j.server.rest.repr.PropertiesRepresentation;
import org.neo4j.server.rest.repr.RelationshipIndexRepresentation;
import org.neo4j.server.rest.repr.RelationshipIndexRootRepresentation;
import org.neo4j.server.rest.repr.RelationshipRepresentation;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.server.rest.repr.RepresentationType;
import org.neo4j.server.rest.repr.ScoredNodeRepresentation;
import org.neo4j.server.rest.repr.ScoredRelationshipRepresentation;
import org.neo4j.server.rest.repr.ValueRepresentation;
import org.neo4j.server.rest.repr.WeightedPathRepresentation;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.IteratorUtil.asList;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.single;
import static org.neo4j.helpers.collection.IteratorUtil.singleOrNull;
import static org.neo4j.server.rest.repr.RepresentationType.CONSTRAINT_DEFINITION;

public class DatabaseActions
{
    public static final String SCORE_ORDER = "score";
    public static final String RELEVANCE_ORDER = "relevance";
    public static final String INDEX_ORDER = "index";
    private final GraphDatabaseAPI graphDb;
    private final LeaseManager leases;

    private final TraversalDescriptionBuilder traversalDescriptionBuilder;
    private final PropertySettingStrategy propertySetter;

    public static class Provider extends InjectableProvider<DatabaseActions>
    {
        private final DatabaseActions database;

        public Provider( DatabaseActions database )
        {
            super( DatabaseActions.class );
            this.database = database;
        }

        @Override
        public DatabaseActions getValue( HttpContext c )
        {
            return database;
        }
    }

    private final Function<ConstraintDefinition, Representation> CONSTRAINT_DEF_TO_REPRESENTATION =
            new Function<ConstraintDefinition, Representation>()
            {
                @Override
                public Representation apply( ConstraintDefinition from )
                {
                    return new ConstraintDefinitionRepresentation( from );
                }
            };

    public DatabaseActions( LeaseManager leaseManager, boolean enableScriptSandboxing, GraphDatabaseAPI graphDatabaseAPI )
    {
        this.leases = leaseManager;
        this.graphDb = graphDatabaseAPI;
        this.traversalDescriptionBuilder = new TraversalDescriptionBuilder( enableScriptSandboxing );
        this.propertySetter = new PropertySettingStrategy( graphDb );
    }

    public DatabaseActions( LeaseManager leaseManager, GraphDatabaseAPI graphDatabaseAPI )
    {
        this( leaseManager, true, graphDatabaseAPI );
    }

    private Node node( long id ) throws NodeNotFoundException
    {
        try
        {
            return graphDb.getNodeById( id );
        }
        catch ( NotFoundException e )
        {
            throw new NodeNotFoundException( String.format(
                    "Cannot find node with id [%d] in database.", id ), e );
        }
    }

    private Relationship relationship( long id )
            throws RelationshipNotFoundException
    {
        try
        {
            return graphDb.getRelationshipById( id );
        }
        catch ( NotFoundException e )
        {
            throw new RelationshipNotFoundException( e );
        }
    }

    // API

    public DatabaseRepresentation root()
    {
        return new DatabaseRepresentation();
    }

    // Nodes

    public NodeRepresentation createNode( Map<String, Object> properties, Label... labels )
            throws PropertyValueException
    {
        Node node = graphDb.createNode();
        propertySetter.setProperties( node, properties );
        if ( labels != null )
        {
            for ( Label label : labels )
            {
                node.addLabel( label );
            }
        }
        return new NodeRepresentation( node );
    }

    public NodeRepresentation getNode( long nodeId )
            throws NodeNotFoundException
    {
        return new NodeRepresentation( node( nodeId ) );
    }

    public void deleteNode( long nodeId ) throws NodeNotFoundException, ConstraintViolationException
    {
        Node node = node( nodeId );

        if ( node.hasRelationship() )
        {
            throw new ConstraintViolationException(
                    String.format(
                            "The node with id %d cannot be deleted. Check that the node is orphaned before deletion.",
                            nodeId ) );
        }

        node.delete();
    }

    // Property keys

    public Representation getAllPropertyKeys()
    {
        Collection<ValueRepresentation> propKeys = asSet( map( new Function<String, ValueRepresentation>()
        {
            @Override
            public ValueRepresentation apply( String key )
            {
                return ValueRepresentation.string( key );
            }
        }, GlobalGraphOperations.at( graphDb ).getAllPropertyKeys() ) );

        return new ListRepresentation( RepresentationType.STRING, propKeys );
    }

    // Node properties

    public Representation getNodeProperty( long nodeId, String key )
            throws NodeNotFoundException, NoSuchPropertyException
    {
        Node node = node( nodeId );
        try
        {
            return PropertiesRepresentation.value( node.getProperty( key ) );
        }
        catch ( NotFoundException e )
        {
            throw new NoSuchPropertyException( node, key );
        }
    }

    public void setNodeProperty( long nodeId, String key, Object value )
            throws PropertyValueException, NodeNotFoundException
    {
        Node node = node( nodeId );
        propertySetter.setProperty( node, key, value );
    }

    public void removeNodeProperty( long nodeId, String key ) throws NodeNotFoundException, NoSuchPropertyException
    {
        Node node = node( nodeId );
        if ( node.removeProperty( key ) == null )
        {
            throw new NoSuchPropertyException( node, key );
        }
    }

    public PropertiesRepresentation getAllNodeProperties( long nodeId )
            throws NodeNotFoundException
    {
        return new PropertiesRepresentation( node( nodeId ) );
    }

    public void setAllNodeProperties( long nodeId,
                                      Map<String, Object> properties ) throws PropertyValueException,
            NodeNotFoundException
    {
        propertySetter.setAllProperties( node( nodeId ), properties );
    }

    public void removeAllNodeProperties( long nodeId )
            throws NodeNotFoundException, PropertyValueException
    {
        propertySetter.setAllProperties( node( nodeId ), null );
    }

    // Labels

    public void addLabelToNode( long nodeId, Collection<String> labelNames ) throws NodeNotFoundException,
            BadInputException
    {
        try
        {
            Node node = node( nodeId );
            for ( String labelName : labelNames )
            {
                node.addLabel( label( labelName ) );
            }
        }
        catch ( org.neo4j.graphdb.ConstraintViolationException e )
        {
            throw new BadInputException( "Unable to add label, see nested exception.", e );
        }
    }


    public void setLabelsOnNode( long nodeId, Collection<String> labels ) throws NodeNotFoundException,
            BadInputException

    {
        Node node = node( nodeId );
        try
        {
            // Remove current labels
            for ( Label label : node.getLabels() )
            {
                node.removeLabel( label );
            }

            // Add new labels
            for ( String labelName : labels )
            {
                node.addLabel( label( labelName ) );
            }
        }
        catch ( org.neo4j.graphdb.ConstraintViolationException e )
        {
            throw new BadInputException( "Unable to add label, see nested exception.", e );
        }
    }

    public void removeLabelFromNode( long nodeId, String labelName ) throws NodeNotFoundException
    {
        node( nodeId ).removeLabel( label( labelName ) );
    }

    public ListRepresentation getNodeLabels( long nodeId ) throws NodeNotFoundException
    {
        Iterable<String> labels = new IterableWrapper<String, Label>( node( nodeId ).getLabels() )
        {
            @Override
            protected String underlyingObjectToObject( Label object )
            {
                return object.name();
            }
        };
        return ListRepresentation.string( labels );
    }

    public String[] getNodeIndexNames()
    {
        return graphDb.index().nodeIndexNames();
    }

    public String[] getRelationshipIndexNames()
    {
        return graphDb.index().relationshipIndexNames();
    }

    public IndexRepresentation createNodeIndex(
            Map<String, Object> indexSpecification )
    {
        final String indexName = (String) indexSpecification.get( "name" );

        assertIsLegalIndexName( indexName );

        if ( indexSpecification.containsKey( "config" ) )
        {

            @SuppressWarnings("unchecked")
            Map<String, String> config = (Map<String, String>) indexSpecification.get( "config" );
            graphDb.index().forNodes( indexName, config );

            return new NodeIndexRepresentation( indexName, config );
        }

        graphDb.index().forNodes( indexName );
        return new NodeIndexRepresentation( indexName,
                Collections.<String, String>emptyMap() );
    }

    public IndexRepresentation createRelationshipIndex(
            Map<String, Object> indexSpecification )
    {
        final String indexName = (String) indexSpecification.get( "name" );

        assertIsLegalIndexName( indexName );

        if ( indexSpecification.containsKey( "config" ) )
        {

            @SuppressWarnings("unchecked")
            Map<String, String> config = (Map<String, String>) indexSpecification.get( "config" );
            graphDb.index().forRelationships( indexName, config );

            return new RelationshipIndexRepresentation( indexName, config );
        }

        graphDb.index().forRelationships( indexName );
        return new RelationshipIndexRepresentation( indexName,
                Collections.<String, String>emptyMap() );
    }

    public void removeNodeIndex( String indexName )
    {
        if ( !graphDb.index().existsForNodes( indexName ) )
        {
            throw new NotFoundException( "No node index named '" + indexName + "'." );
        }

        graphDb.index().forNodes( indexName ).delete();
    }

    public void removeRelationshipIndex( String indexName )
    {
        if ( !graphDb.index().existsForRelationships( indexName ) )
        {
            throw new NotFoundException( "No relationship index named '" + indexName + "'." );
        }

        graphDb.index().forRelationships( indexName ).delete();
    }

    public boolean nodeIsIndexed( String indexName, String key, Object value, long nodeId )
    {
        Index<Node> index = graphDb.index().forNodes( indexName );
        Node expectedNode = graphDb.getNodeById( nodeId );
        IndexHits<Node> hits = index.get( key, value );
        return iterableContains( hits, expectedNode );
    }

    public boolean relationshipIsIndexed( String indexName, String key, Object value, long relationshipId )
    {

        Index<Relationship> index = graphDb.index().forRelationships( indexName );
        Relationship expectedNode = graphDb.getRelationshipById( relationshipId );
        IndexHits<Relationship> hits = index.get( key, value );
        return iterableContains( hits, expectedNode );
    }

    private <T> boolean iterableContains( Iterable<T> iterable,
                                          T expectedElement )
    {
        for ( T possibleMatch : iterable )
        {
            if ( possibleMatch.equals( expectedElement ) )
            {
                return true;
            }
        }
        return false;
    }

    public Representation isAutoIndexerEnabled( String type )
    {
        AutoIndexer<? extends PropertyContainer> index = getAutoIndexerForType( type );
        return ValueRepresentation.bool( index.isEnabled() );
    }

    public void setAutoIndexerEnabled( String type, boolean enable )
    {
        AutoIndexer<? extends PropertyContainer> index = getAutoIndexerForType( type );
        index.setEnabled( enable );
    }

    private AutoIndexer<? extends PropertyContainer> getAutoIndexerForType( String type )
    {
        final IndexManager indexManager = graphDb.index();
        switch ( type )
        {
            case "node":
                return indexManager.getNodeAutoIndexer();
            case "relationship":
                return indexManager.getRelationshipAutoIndexer();
            default:
                throw new IllegalArgumentException( "invalid type " + type );
        }
    }

    public Representation getAutoIndexedProperties( String type )
    {
        AutoIndexer<? extends PropertyContainer> indexer = getAutoIndexerForType( type );
        return ListRepresentation.string( indexer.getAutoIndexedProperties() );
    }

    public void startAutoIndexingProperty( String type, String property )
    {
        AutoIndexer<? extends PropertyContainer> indexer = getAutoIndexerForType( type );
        indexer.startAutoIndexingProperty( property );
    }

    public void stopAutoIndexingProperty( String type, String property )
    {
        AutoIndexer<? extends PropertyContainer> indexer = getAutoIndexerForType( type );
        indexer.stopAutoIndexingProperty( property );
    }

    // Relationships

    public enum RelationshipDirection
    {
        all( Direction.BOTH ),
        in( Direction.INCOMING ),
        out( Direction.OUTGOING );
        final Direction internal;

        private RelationshipDirection( Direction internal )
        {
            this.internal = internal;
        }
    }

    public RelationshipRepresentation createRelationship( long startNodeId,
                                                          long endNodeId, String type, Map<String, Object> properties )
            throws StartNodeNotFoundException, EndNodeNotFoundException,
            PropertyValueException
    {

        Node start, end;
        try
        {
            start = node( startNodeId );
        }
        catch ( NodeNotFoundException e )
        {
            throw new StartNodeNotFoundException( e );
        }
        try
        {
            end = node( endNodeId );
        }
        catch ( NodeNotFoundException e )
        {
            throw new EndNodeNotFoundException( e );
        }

        Relationship rel = start.createRelationshipTo( end,
                DynamicRelationshipType.withName( type ) );

        propertySetter.setProperties( rel, properties );

        return new RelationshipRepresentation( rel );
    }

    public RelationshipRepresentation getRelationship( long relationshipId )
            throws RelationshipNotFoundException
    {
        return new RelationshipRepresentation( relationship( relationshipId ) );
    }

    public void deleteRelationship( long relationshipId ) throws RelationshipNotFoundException
    {
        relationship( relationshipId ).delete();
    }

    @SuppressWarnings("unchecked")
    public ListRepresentation getNodeRelationships( long nodeId,
                                                    RelationshipDirection direction, Collection<String> types )
            throws NodeNotFoundException
    {
        Node node = node( nodeId );
        Expander expander;
        if ( types.isEmpty() )
        {
            expander = Traversal.expanderForAllTypes( direction.internal );
        }
        else
        {
            expander = Traversal.emptyExpander();
            for ( String type : types )
            {
                expander = expander.add(
                        DynamicRelationshipType.withName( type ),
                        direction.internal );
            }
        }
        return RelationshipRepresentation.list( expander.expand( node ) );
    }

    // Node degrees

    @SuppressWarnings("unchecked")
    public Representation getNodeDegree( long nodeId, RelationshipDirection direction, Collection<String> types )
            throws NodeNotFoundException
    {
        Node node = node( nodeId );
        if ( types.isEmpty() )
        {
            return PropertiesRepresentation.value( node.getDegree( direction.internal ) );
        }
        else
        {
            int sum = 0;
            for ( String type : types )
            {
                sum += node.getDegree(DynamicRelationshipType.withName( type), direction.internal);
            }
            return PropertiesRepresentation.value( sum );
        }
    }

    // Relationship properties

    public PropertiesRepresentation getAllRelationshipProperties(
            long relationshipId ) throws RelationshipNotFoundException
    {
        return new PropertiesRepresentation( relationship( relationshipId ) );
    }

    public Representation getRelationshipProperty( long relationshipId,
                                                   String key ) throws NoSuchPropertyException,
            RelationshipNotFoundException
    {
        Relationship relationship = relationship( relationshipId );
        try
        {
            return PropertiesRepresentation.value( relationship.getProperty( key ) );
        }
        catch ( NotFoundException e )
        {
            throw new NoSuchPropertyException( relationship, key );
        }
    }

    public void setAllRelationshipProperties( long relationshipId,
                                              Map<String, Object> properties ) throws PropertyValueException,
            RelationshipNotFoundException
    {
        propertySetter.setAllProperties( relationship( relationshipId ), properties );
    }

    public void setRelationshipProperty( long relationshipId, String key,
                                         Object value ) throws PropertyValueException,
            RelationshipNotFoundException
    {
        Relationship relationship = relationship( relationshipId );
        propertySetter.setProperty( relationship, key, value );
    }

    public void removeAllRelationshipProperties( long relationshipId )
            throws RelationshipNotFoundException, PropertyValueException
    {
        propertySetter.setAllProperties( relationship( relationshipId ), null );
    }

    public void removeRelationshipProperty( long relationshipId, String key )
            throws RelationshipNotFoundException, NoSuchPropertyException
    {
        Relationship relationship = relationship( relationshipId );

        if ( relationship.removeProperty( key ) == null )
        {
            throw new NoSuchPropertyException( relationship, key );
        }
    }

    public Representation nodeIndexRoot()
    {
        return new NodeIndexRootRepresentation( graphDb.index() );
    }

    public Representation relationshipIndexRoot()
    {
        return new RelationshipIndexRootRepresentation( graphDb.index() );
    }

    public IndexedEntityRepresentation addToRelationshipIndex( String indexName, String key, String value,
                                                               long relationshipId )
    {
        Relationship relationship = graphDb.getRelationshipById( relationshipId );
        Index<Relationship> index = graphDb.index().forRelationships( indexName );
        index.add( relationship, key, value );
        return new IndexedEntityRepresentation( relationship, key, value,
                new RelationshipIndexRepresentation( indexName,
                        Collections.<String, String>emptyMap() ) );
    }

    public IndexedEntityRepresentation addToNodeIndex( String indexName, String key, String value, long nodeId )
    {
        Node node = graphDb.getNodeById( nodeId );
        Index<Node> index = graphDb.index().forNodes( indexName );
        index.add( node, key, value );
        return new IndexedEntityRepresentation( node, key, value,
                new NodeIndexRepresentation( indexName,
                        Collections.<String, String>emptyMap() ) );
    }

    public void removeFromNodeIndex( String indexName, String key, String value, long id )
    {
        graphDb.index().forNodes( indexName ).remove( graphDb.getNodeById( id ), key, value );
    }

    public void removeFromNodeIndexNoValue( String indexName, String key, long id )
    {
        graphDb.index().forNodes( indexName ).remove( graphDb.getNodeById( id ), key );
    }

    public void removeFromNodeIndexNoKeyValue( String indexName, long id )
    {
        graphDb.index().forNodes( indexName ).remove( graphDb.getNodeById( id ) );
    }

    public void removeFromRelationshipIndex( String indexName, String key, String value, long id )
    {
        graphDb.index().forRelationships( indexName ).remove( graphDb.getRelationshipById( id ), key, value );
    }

    public void removeFromRelationshipIndexNoValue( String indexName, String key, long id )
    {
        graphDb.index().forRelationships( indexName ).remove( graphDb.getRelationshipById( id ), key );
    }

    public void removeFromRelationshipIndexNoKeyValue( String indexName, long id )
    {
        graphDb.index().forRelationships( indexName ).remove( graphDb.getRelationshipById( id ) );
    }

    public IndexedEntityRepresentation getIndexedNode( String indexName,
                                                       String key, String value, long id )
    {
        if ( !nodeIsIndexed( indexName, key, value, id ) )
        {
            throw new NotFoundException();
        }
        Node node = graphDb.getNodeById( id );
        return new IndexedEntityRepresentation( node, key, value,
                new NodeIndexRepresentation( indexName,
                        Collections.<String, String>emptyMap() ) );
    }

    public IndexedEntityRepresentation getIndexedRelationship(
            String indexName, String key, String value, long id )
    {
        if ( !relationshipIsIndexed( indexName, key, value, id ) )
        {
            throw new NotFoundException();
        }
        Relationship node = graphDb.getRelationshipById( id );
        return new IndexedEntityRepresentation( node, key, value,
                new RelationshipIndexRepresentation( indexName,
                        Collections.<String, String>emptyMap() ) );
    }

    public ListRepresentation getIndexedNodes( String indexName, final String key,
                                               final String value )
    {
        if ( !graphDb.index().existsForNodes( indexName ) )
        {
            throw new NotFoundException();
        }

        Index<Node> index = graphDb.index().forNodes( indexName );

        final IndexRepresentation indexRepresentation = new NodeIndexRepresentation( indexName );
        final IndexHits<Node> indexHits = index.get( key, value );

        final IterableWrapper<Representation, Node> results = new IterableWrapper<Representation, Node>( indexHits )
        {
            @Override
            protected Representation underlyingObjectToObject( Node node )
            {
                return new IndexedEntityRepresentation( node, key, value, indexRepresentation );
            }
        };
        return new ListRepresentation( RepresentationType.NODE, results );
    }

    public ListRepresentation getIndexedNodesByQuery( String indexName,
                                                      String query, String sort )
    {
        return getIndexedNodesByQuery( indexName, null, query, sort );
    }

    public ListRepresentation getIndexedNodesByQuery( String indexName,
                                                      String key, String query, String sort )
    {
        if ( !graphDb.index().existsForNodes( indexName ) )
        {
            throw new NotFoundException();
        }

        if ( query == null )
        {
            return toListNodeRepresentation();
        }
        Index<Node> index = graphDb.index().forNodes( indexName );

        IndexResultOrder order = getOrdering( sort );
        QueryContext queryCtx = order.updateQueryContext( new QueryContext( query ) );
        IndexHits<Node> result = index.query( key, queryCtx );
        return toListNodeRepresentation( result, order );
    }

    private ListRepresentation toListNodeRepresentation()
    {
        return new ListRepresentation( RepresentationType.NODE, Collections.<Representation>emptyList() );
    }

    private ListRepresentation toListNodeRepresentation( final IndexHits<Node> result, final IndexResultOrder order )
    {
        if ( result == null )
        {
            return new ListRepresentation( RepresentationType.NODE, Collections.<Representation>emptyList() );
        }
        final IterableWrapper<Representation, Node> results = new IterableWrapper<Representation, Node>( result )
        {
            @Override
            protected Representation underlyingObjectToObject( Node node )
            {
                final NodeRepresentation nodeRepresentation = new NodeRepresentation( node );
                if ( order == null )
                {
                    return nodeRepresentation;
                }
                return order.getRepresentationFor( nodeRepresentation, result.currentScore() );
            }
        };
        return new ListRepresentation( RepresentationType.NODE, results );
    }

    private ListRepresentation toListRelationshipRepresentation()
    {
        return new ListRepresentation( RepresentationType.RELATIONSHIP, Collections.<Representation>emptyList() );
    }

    private ListRepresentation toListRelationshipRepresentation( final IndexHits<Relationship> result,
                                                                 final IndexResultOrder order )
    {
        if ( result == null )
        {
            return new ListRepresentation( RepresentationType.RELATIONSHIP, Collections.<Representation>emptyList() );
        }
        final IterableWrapper<Representation, Relationship> results = new IterableWrapper<Representation,
                Relationship>( result )
        {
            @Override
            protected Representation underlyingObjectToObject( Relationship rel )
            {
                final RelationshipRepresentation relationshipRepresentation = new RelationshipRepresentation( rel );
                if ( order != null )
                {
                    return order.getRepresentationFor( relationshipRepresentation, result.currentScore() );
                }
                return relationshipRepresentation;
            }
        };
        return new ListRepresentation( RepresentationType.RELATIONSHIP, results );
    }



    public Pair<IndexedEntityRepresentation, Boolean> getOrCreateIndexedNode(
            String indexName, String key, String value, Long nodeOrNull, Map<String, Object> properties )
            throws BadInputException, NodeNotFoundException
    {
        assertIsLegalIndexName( indexName );
        Node result;
        boolean created;
        if ( nodeOrNull != null )
        {
            if ( properties != null )
            {
                throw new InvalidArgumentsException( "Cannot specify properties for a new node, " +
                        "when a node to index is specified." );
            }
            Node node = node( nodeOrNull );
            result = graphDb.index().forNodes( indexName ).putIfAbsent( node, key, value );
            created = result == null;
            if ( created )
            {
                UniqueNodeFactory factory = new UniqueNodeFactory( indexName, properties );
                UniqueEntity<Node> entity = factory.getOrCreateWithOutcome( key, value );
                // when given a node id, return as created if that node was newly added to the index
                created = entity.entity().getId() == node.getId() || entity.wasCreated();
                result = entity.entity();
            }
        }
        else
        {
            if ( properties != null )
            {
                for ( Map.Entry<String, Object> entry : properties.entrySet() )
                {
                    entry.setValue( propertySetter.convert( entry.getValue() ) );
                }
            }
            UniqueNodeFactory factory = new UniqueNodeFactory( indexName, properties );
            UniqueEntity<Node> entity = factory.getOrCreateWithOutcome( key, value );
            result = entity.entity();
            created = entity.wasCreated();
        }
        return Pair.of( new IndexedEntityRepresentation( result, key, value,
                new NodeIndexRepresentation( indexName, Collections.<String, String>emptyMap() ) ), created );
    }

    public Pair<IndexedEntityRepresentation, Boolean> getOrCreateIndexedRelationship(
            String indexName, String key, String value,
            Long relationshipOrNull, Long startNode, String type, Long endNode,
            Map<String, Object> properties )
            throws BadInputException, RelationshipNotFoundException, NodeNotFoundException
    {
        assertIsLegalIndexName( indexName );
        Relationship result;
        boolean created;
        if ( relationshipOrNull != null )
        {
            if ( startNode != null || type != null || endNode != null || properties != null )
            {
                throw new InvalidArgumentsException( "Either specify a relationship to index uniquely, " +
                        "or the means for creating it." );
            }
            Relationship relationship = relationship( relationshipOrNull );
            result = graphDb.index().forRelationships( indexName ).putIfAbsent( relationship, key, value );
            if ( created = result == null )
            {
                UniqueRelationshipFactory factory =
                        new UniqueRelationshipFactory( indexName, relationship.getStartNode(),
                                relationship.getEndNode(), relationship.getType().name(), properties );
                UniqueEntity<Relationship> entity = factory.getOrCreateWithOutcome( key, value );
                // when given a relationship id, return as created if that relationship was newly added to the index
                created = entity.entity().getId() == relationship.getId() || entity.wasCreated();
                result = entity.entity();
            }
        }
        else if ( startNode == null || type == null || endNode == null )
        {
            throw new InvalidArgumentsException( "Either specify a relationship to index uniquely, " +
                    "or the means for creating it." );
        }
        else
        {
            UniqueRelationshipFactory factory =
                new UniqueRelationshipFactory( indexName, node( startNode ), node( endNode ), type, properties );
            UniqueEntity<Relationship> entity = factory.getOrCreateWithOutcome( key, value );
            result = entity.entity();
            created = entity.wasCreated();
        }
        return Pair.of( new IndexedEntityRepresentation( result, key, value,
                new RelationshipIndexRepresentation( indexName, Collections.<String, String>emptyMap() ) ),
                created );
    }

    private class UniqueRelationshipFactory extends UniqueFactory.UniqueRelationshipFactory
    {
        private final Node start, end;
        private final RelationshipType type;
        private final Map<String, Object> properties;

        UniqueRelationshipFactory( String index, Node start, Node end, String type, Map<String, Object> properties )
        {
            super( graphDb, index );
            this.start = start;
            this.end = end;
            this.type = DynamicRelationshipType.withName( type );
            this.properties = properties;
        }

        @Override
        protected Relationship create( Map<String, Object> ignored )
        {
            return start.createRelationshipTo( end, type );
        }

        @Override
        protected void initialize( Relationship relationship, Map<String, Object> indexed )
        {
            for ( Map.Entry<String, Object> property : (properties == null ? indexed : properties).entrySet() )
            {
                relationship.setProperty( property.getKey(), property.getValue() );
            }
        }
    }

    private class UniqueNodeFactory extends UniqueFactory.UniqueNodeFactory
    {
        private final Map<String, Object> properties;

        UniqueNodeFactory( String index, Map<String, Object> properties )
        {
            super( graphDb, index );
            this.properties = properties;
        }

        @Override
        protected void initialize( Node node, Map<String, Object> indexed )
        {
            for ( Map.Entry<String, Object> property : (properties == null ? indexed : properties).entrySet() )
            {
                node.setProperty( property.getKey(), property.getValue() );
            }
        }
    }

    public Representation getAutoIndexedNodes( String key, String value )
    {
        ReadableIndex<Node> index = graphDb.index().getNodeAutoIndexer().getAutoIndex();

        return toListNodeRepresentation( index.get( key, value ), null );
    }

    public ListRepresentation getAutoIndexedNodesByQuery( String query )
    {
        if ( query != null )
        {
            ReadableIndex<Node> index = graphDb.index().getNodeAutoIndexer().getAutoIndex();
            return toListNodeRepresentation( index.query( query ), null );
        }
        return toListNodeRepresentation();
    }

    public ListRepresentation getIndexedRelationships( String indexName,
                                                       final String key, final String value )
    {
        if ( !graphDb.index().existsForRelationships( indexName ) )
        {
            throw new NotFoundException();
        }

        Index<Relationship> index = graphDb.index().forRelationships( indexName );

        final IndexRepresentation indexRepresentation = new RelationshipIndexRepresentation( indexName );

        IterableWrapper<Representation, Relationship> result =
                new IterableWrapper<Representation, Relationship>( index.get( key, value ) )
                {
                    @Override
                    protected Representation underlyingObjectToObject( Relationship relationship )
                    {
                        return new IndexedEntityRepresentation( relationship,
                                key, value, indexRepresentation );
                    }
                };
        return new ListRepresentation( RepresentationType.RELATIONSHIP, result );
    }

    public ListRepresentation getIndexedRelationshipsByQuery( String indexName,
                                                              String query, String sort )
    {
        return getIndexedRelationshipsByQuery( indexName, null, query, sort );
    }

    public ListRepresentation getIndexedRelationshipsByQuery( String indexName,
                                                              String key, String query, String sort )
    {
        if ( !graphDb.index().existsForRelationships( indexName ) )
        {
            throw new NotFoundException();
        }

        if ( query == null )
        {
            return toListRelationshipRepresentation();
        }
        Index<Relationship> index = graphDb.index().forRelationships( indexName );

        IndexResultOrder order = getOrdering( sort );
        QueryContext queryCtx = order.updateQueryContext( new QueryContext(
                query ) );

        return toListRelationshipRepresentation( index.query( key, queryCtx ), order );
    }

    public Representation getAutoIndexedRelationships( String key, String value )
    {
        final ReadableIndex<Relationship> index = graphDb.index().getRelationshipAutoIndexer().getAutoIndex();
        return toListRelationshipRepresentation( index.get( key, value ), null );
    }

    public ListRepresentation getAutoIndexedRelationshipsByQuery( String query )
    {
        final ReadableIndex<Relationship> index = graphDb.index().getRelationshipAutoIndexer().getAutoIndex();
        final IndexHits<Relationship> results = query != null ? index.query( query ) : null;
        return toListRelationshipRepresentation( results, null );
    }

    // Traversal

    public ListRepresentation traverse( long startNode,
                                        Map<String, Object> description, final TraverserReturnType returnType )
    {
        Node node = graphDb.getNodeById( startNode );

        TraversalDescription traversalDescription = traversalDescriptionBuilder.from( description );
        final Iterable<Path> paths = traversalDescription.traverse( node );
        return toListPathRepresentation( paths, returnType );
    }

    private ListRepresentation toListPathRepresentation( final Iterable<Path> paths,
                                                         final TraverserReturnType returnType )
    {
        final IterableWrapper<Representation, Path> result = new IterableWrapper<Representation, Path>( paths )
        {
            @Override
            protected Representation underlyingObjectToObject( Path position )
            {
                return returnType.toRepresentation( position );
            }
        };
        return new ListRepresentation( returnType.repType, result );
    }

    public ListRepresentation pagedTraverse( String traverserId,
                                             TraverserReturnType returnType )
    {
        Lease lease = leases.getLeaseById( traverserId );
        if ( lease == null )
        {
            throw new NotFoundException( String.format(
                    "The traverser with id [%s] was not found", traverserId ) );
        }

        PagedTraverser traverser = lease.getLeasedItemAndRenewLease();
        List<Path> paths = traverser.next();

        if ( paths != null )
        {
            return toListPathRepresentation( paths, returnType );
        }
        else
        {
            leases.remove( traverserId );
            // Yuck.
            throw new NotFoundException(
                    String.format(
                            "The results for paged traverser with id [%s] have been fully enumerated",
                            traverserId ) );
        }
    }

    public String createPagedTraverser( long nodeId,
                                        Map<String, Object> description, int pageSize, int leaseTime )
    {
        Node node = graphDb.getNodeById( nodeId );

        TraversalDescription traversalDescription = traversalDescriptionBuilder.from( description );

        PagedTraverser traverser = new PagedTraverser(
                traversalDescription.traverse( node ), pageSize );

        return leases.createLease( leaseTime, traverser ).getId();
    }

    public boolean removePagedTraverse( String traverserId )
    {
        Lease lease = leases.getLeaseById( traverserId );
        if ( lease == null )
        {
            return false;
        }
        else
        {
            leases.remove( lease.getId() );
            return true;
        }
    }

    // Graph algos

    @SuppressWarnings("rawtypes")
    public PathRepresentation findSinglePath( long startId, long endId,
                                              Map<String, Object> map )
    {
        FindParams findParams = new FindParams( startId, endId, map ).invoke();
        PathFinder finder = findParams.getFinder();
        Node startNode = findParams.getStartNode();
        Node endNode = findParams.getEndNode();

        Path path = finder.findSinglePath( startNode, endNode );
        if ( path == null )
        {
            throw new NotFoundException();
        }
        return findParams.pathRepresentationOf( path );
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public ListRepresentation findPaths( long startId, long endId,
                                         Map<String, Object> map )
    {
        final FindParams findParams = new FindParams( startId, endId, map ).invoke();
        PathFinder finder = findParams.getFinder();
        Node startNode = findParams.getStartNode();
        Node endNode = findParams.getEndNode();

        Iterable paths = finder.findAllPaths( startNode, endNode );

        IterableWrapper<PathRepresentation, Path> pathRepresentations = new IterableWrapper<PathRepresentation, Path>(
                paths )
        {
            @Override
            protected PathRepresentation underlyingObjectToObject( Path path )
            {
                return findParams.pathRepresentationOf( path );
            }
        };

        return new ListRepresentation( RepresentationType.PATH,
                pathRepresentations );
    }

    private class FindParams
    {
        private final long startId;
        private final long endId;
        private final Map<String, Object> map;
        private Node startNode;
        private Node endNode;
        private PathFinder<? extends Path> finder;
        @SuppressWarnings("rawtypes")
        private PathRepresentationCreator representationCreator = PATH_REPRESENTATION_CREATOR;

        public FindParams( final long startId, final long endId,
                           final Map<String, Object> map )
        {
            this.startId = startId;
            this.endId = endId;
            this.map = map;
        }

        public Node getStartNode()
        {
            return startNode;
        }

        public Node getEndNode()
        {
            return endNode;
        }

        public PathFinder<? extends Path> getFinder()
        {
            return finder;
        }

        @SuppressWarnings("unchecked")
        public PathRepresentation<? extends Path> pathRepresentationOf(
                Path path )
        {
            return representationCreator.from( path );
        }

        public FindParams invoke()
        {
            startNode = graphDb.getNodeById( startId );
            endNode = graphDb.getNodeById( endId );

            Integer maxDepthObj = (Integer) map.get( "max_depth" );
            int maxDepth = (maxDepthObj != null) ? maxDepthObj : 1;

            RelationshipExpander expander = RelationshipExpanderBuilder.describeRelationships( map );

            String algorithm = (String) map.get( "algorithm" );
            algorithm = (algorithm != null) ? algorithm : "shortestPath";

            finder = getAlgorithm( algorithm, expander, maxDepth );
            return this;
        }

        private PathFinder<? extends Path> getAlgorithm( String algorithm,
                                                         RelationshipExpander expander, int maxDepth )
        {
            if ( algorithm.equals( "shortestPath" ) )
            {
                return GraphAlgoFactory.shortestPath( expander, maxDepth );
            }
            else if ( algorithm.equals( "allSimplePaths" ) )
            {
                return GraphAlgoFactory.allSimplePaths( expander, maxDepth );
            }
            else if ( algorithm.equals( "allPaths" ) )
            {
                return GraphAlgoFactory.allPaths( expander, maxDepth );
            }
            else if ( algorithm.equals( "dijkstra" ) )
            {
                String costProperty = (String) map.get( "cost_property" );
                Number defaultCost = (Number) map.get( "default_cost" );
                CostEvaluator<Double> costEvaluator = defaultCost == null ? CommonEvaluators.doubleCostEvaluator(
                        costProperty )
                        : CommonEvaluators.doubleCostEvaluator( costProperty,
                        defaultCost.doubleValue() );
                representationCreator = WEIGHTED_PATH_REPRESENTATION_CREATOR;
                return GraphAlgoFactory.dijkstra( expander, costEvaluator );
            }

            throw new RuntimeException( "Failed to find matching algorithm" );
        }
    }

    /*
     * This enum binds the parameter-string-to-result-order mapping and
     * the kind of results returned. This is not correct in general but
     * at the time of writing it is the way things are done and is
     * quite handy. Feel free to rip out if requirements change.
     */
    private enum IndexResultOrder
    {
        INDEX_ORDER
                {
                    @Override
                    QueryContext updateQueryContext( QueryContext original )
                    {
                        return original.sort( Sort.INDEXORDER );
                    }
                }, RELEVANCE_ORDER
            {
                @Override
                QueryContext updateQueryContext( QueryContext original )
                {
                    return original.sort( Sort.RELEVANCE );
                }
            },
        SCORE_ORDER
                {
                    @Override
                    QueryContext updateQueryContext( QueryContext original )
                    {
                        return original.sortByScore();
                    }
                },
        NONE
                {
                    @Override
                    Representation getRepresentationFor( Representation delegate,
                                                         float score )
                    {
                        return delegate;
                    }

                    @Override
                    QueryContext updateQueryContext( QueryContext original )
                    {
                        return original;
                    }
                };

        Representation getRepresentationFor( Representation delegate,
                                             float score )
        {
            if ( delegate instanceof NodeRepresentation )
            {
                return new ScoredNodeRepresentation(
                        (NodeRepresentation) delegate, score );
            }
            if ( delegate instanceof RelationshipRepresentation )
            {
                return new ScoredRelationshipRepresentation(
                        (RelationshipRepresentation) delegate, score );
            }
            return delegate;
        }

        abstract QueryContext updateQueryContext( QueryContext original );
    }

    private final IndexResultOrder getOrdering( String order )
    {
        if ( INDEX_ORDER.equalsIgnoreCase( order ) )
        {
            return IndexResultOrder.INDEX_ORDER;
        }
        else if ( RELEVANCE_ORDER.equalsIgnoreCase( order ) )
        {
            return IndexResultOrder.RELEVANCE_ORDER;
        }
        else if ( SCORE_ORDER.equalsIgnoreCase( order ) )
        {
            return IndexResultOrder.SCORE_ORDER;
        }
        else
        {
            return IndexResultOrder.NONE;
        }
    }

    private interface PathRepresentationCreator<T extends Path>
    {
        PathRepresentation<T> from( T path );
    }

    private static final PathRepresentationCreator<Path> PATH_REPRESENTATION_CREATOR = new
            PathRepresentationCreator<Path>()
    {
        @Override
        public PathRepresentation<Path> from( Path path )
        {
            return new PathRepresentation<>( path );
        }
    };

    private static final PathRepresentationCreator<WeightedPath> WEIGHTED_PATH_REPRESENTATION_CREATOR = new
            PathRepresentationCreator<WeightedPath>()
    {
        @Override
        public PathRepresentation<WeightedPath> from( WeightedPath path )
        {
            return new WeightedPathRepresentation( path );
        }
    };

    private void assertIsLegalIndexName( String indexName )
    {
        if ( indexName == null || indexName.equals( "" ) )
        {
            throw new IllegalArgumentException( "Index name must not be empty." );
        }
    }

    public ListRepresentation getNodesWithLabel( String labelName, Map<String, Object> properties )
    {
        Iterator<Node> nodes;

        if ( properties.size() == 0 )
        {
            nodes = graphDb.findNodes( label( labelName ) );
        }
        else if ( properties.size() == 1 )
        {
            Map.Entry<String, Object> prop = Iterables.single( properties.entrySet() );
            nodes = graphDb.findNodes( label( labelName ), prop.getKey(), prop.getValue() );
        }
        else
        {
            throw new IllegalArgumentException( "Too many properties specified. Either specify one property to " +
                    "filter by, or none at all." );
        }

        IterableWrapper<NodeRepresentation, Node> nodeRepresentations =
                new IterableWrapper<NodeRepresentation, Node>( asList( nodes ) )
                {
                    @Override
                    protected NodeRepresentation underlyingObjectToObject( Node node )
                    {
                        return new NodeRepresentation( node );
                    }
                };

        return new ListRepresentation( RepresentationType.NODE, nodeRepresentations );
    }

    public ListRepresentation getAllLabels( boolean inUse )
    {
        GlobalGraphOperations operations = GlobalGraphOperations.at( graphDb );
        ResourceIterable<Label> labels = inUse ? operations.getAllLabelsInUse() : operations.getAllLabels();
        Collection<ValueRepresentation> labelNames = asSet( map( new Function<Label,ValueRepresentation>()
        {
            @Override
            public ValueRepresentation apply( Label label )
            {
                return ValueRepresentation.string( label.name() );
            }
        }, labels ) );


        return new ListRepresentation( RepresentationType.STRING, labelNames );
    }

    public IndexDefinitionRepresentation createSchemaIndex( String labelName, Iterable<String> propertyKey )
    {
        IndexCreator indexCreator = graphDb.schema().indexFor( label( labelName ) );
        for ( String key : propertyKey )
        {
            indexCreator = indexCreator.on( key );
        }
        return new IndexDefinitionRepresentation( indexCreator.create() );
    }

    public ListRepresentation getSchemaIndexes()
    {
        Iterable<IndexDefinition> definitions = graphDb.schema().getIndexes();
        Iterable<IndexDefinitionRepresentation> representations = map( new Function<IndexDefinition,
                IndexDefinitionRepresentation>()
        {
            @Override
            public IndexDefinitionRepresentation apply( IndexDefinition definition )
            {
                return new IndexDefinitionRepresentation( definition );
            }
        }, definitions );
        return new ListRepresentation( RepresentationType.INDEX_DEFINITION, representations );
    }

    public ListRepresentation getSchemaIndexes( String labelName )
    {
        Iterable<IndexDefinition> definitions = graphDb.schema().getIndexes( label( labelName ) );
        Iterable<IndexDefinitionRepresentation> representations = map( new Function<IndexDefinition,
                IndexDefinitionRepresentation>()
        {
            @Override
            public IndexDefinitionRepresentation apply( IndexDefinition definition )
            {
                return new IndexDefinitionRepresentation( definition );
            }
        }, definitions );
        return new ListRepresentation( RepresentationType.INDEX_DEFINITION, representations );
    }

    public boolean dropSchemaIndex( String labelName, String propertyKey )
    {
        boolean found = false;
        for ( IndexDefinition index : graphDb.schema().getIndexes( label( labelName ) ) )
        {
            // TODO Assumption about single property key
            if ( propertyKey.equals( single( index.getPropertyKeys() ) ) )
            {
                index.drop();
                found = true;
                break;
            }
        }
        return found;
    }

    public ConstraintDefinitionRepresentation createPropertyUniquenessConstraint( String labelName,
                                                                                  Iterable<String> propertyKeys )
    {
        ConstraintCreator constraintCreator = graphDb.schema().constraintFor( label( labelName ) );
        for ( String key : propertyKeys )
        {
            constraintCreator = constraintCreator.assertPropertyIsUnique( key );
        }
        ConstraintDefinition constraintDefinition = constraintCreator.create();
        return new ConstraintDefinitionRepresentation( constraintDefinition );
    }

    public boolean dropPropertyUniquenessConstraint( String labelName, Iterable<String> propertyKeys )
    {
        final Set<String> propertyKeysSet = asSet( propertyKeys );
        ConstraintDefinition constraint =
                singleOrNull( filteredNodeConstraints( labelName, propertyUniquenessFilter( propertyKeysSet ) ) );
        if ( constraint != null )
        {
            constraint.drop();
        }
        return constraint != null;
    }

    public boolean dropNodePropertyExistenceConstraint( String labelName, Iterable<String> propertyKeys )
    {
        final Set<String> propertyKeysSet = asSet( propertyKeys );
        ConstraintDefinition constraint =
                singleOrNull( filteredNodeConstraints( labelName, nodePropertyExistenceFilter( propertyKeysSet ) ) );
        if ( constraint != null )
        {
            constraint.drop();
        }
        return constraint != null;
    }

    public boolean dropRelationshipPropertyExistenceConstraint( String typeName, Iterable<String> propertyKeys )
    {
        final Set<String> propertyKeysSet = asSet( propertyKeys );
        ConstraintDefinition constraint = singleOrNull( filteredRelationshipConstraints( typeName,
                relationshipPropertyExistenceFilter( propertyKeysSet ) ) );
        if ( constraint != null )
        {
            constraint.drop();
        }
        return constraint != null;
    }

    public ListRepresentation getNodePropertyExistenceConstraint( String labelName, Iterable<String> propertyKeys )
    {
        Set<String> propertyKeysSet = asSet( propertyKeys );
        Iterable<ConstraintDefinition> constraints =
                filteredNodeConstraints( labelName, nodePropertyExistenceFilter( propertyKeysSet ) );
        if ( constraints.iterator().hasNext() )
        {
            Iterable<Representation> representationIterable = map( CONSTRAINT_DEF_TO_REPRESENTATION, constraints );
            return new ListRepresentation( CONSTRAINT_DEFINITION, representationIterable );
        }
        else
        {
            throw new IllegalArgumentException(
                    String.format( "Constraint with label %s for properties %s does not exist", labelName,
                            propertyKeys ) );
        }
    }

    public ListRepresentation getRelationshipPropertyExistenceConstraint( String typeName,
            Iterable<String> propertyKeys )
    {
        Set<String> propertyKeysSet = asSet( propertyKeys );
        Iterable<ConstraintDefinition> constraints =
                filteredRelationshipConstraints( typeName, relationshipPropertyExistenceFilter( propertyKeysSet ) );
        if ( constraints.iterator().hasNext() )
        {
            Iterable<Representation> representationIterable = map( CONSTRAINT_DEF_TO_REPRESENTATION, constraints );
            return new ListRepresentation( CONSTRAINT_DEFINITION, representationIterable );
        }
        else
        {
            throw new IllegalArgumentException(
                    String.format( "Constraint with relationship type %s for properties %s does not exist",
                            typeName, propertyKeys ) );
        }
    }

    public ListRepresentation getPropertyUniquenessConstraint( String labelName, Iterable<String> propertyKeys )
    {
        Set<String> propertyKeysSet = asSet( propertyKeys );
        Iterable<ConstraintDefinition> constraints =
                filteredNodeConstraints( labelName, propertyUniquenessFilter( propertyKeysSet ) );
        if ( constraints.iterator().hasNext() )
        {
            Iterable<Representation> representationIterable = map( CONSTRAINT_DEF_TO_REPRESENTATION, constraints );
            return new ListRepresentation( CONSTRAINT_DEFINITION, representationIterable );
        }
        else
        {
            throw new IllegalArgumentException(
                    String.format( "Constraint with label %s for properties %s does not exist", labelName,
                            propertyKeys ) );
        }
    }

    private Iterable<ConstraintDefinition> filteredNodeConstraints( String labelName,
                                                                    Predicate<ConstraintDefinition> filter )
    {
        Iterable<ConstraintDefinition> constraints = graphDb.schema().getConstraints( label( labelName ) );
        return filter( filter, constraints );
    }

    private Iterable<ConstraintDefinition> filteredRelationshipConstraints( String typeName,
                                                                            Predicate<ConstraintDefinition> filter )
    {
        DynamicRelationshipType type = DynamicRelationshipType.withName( typeName );
        Iterable<ConstraintDefinition> constraints = graphDb.schema().getConstraints( type );
        return filter( filter, constraints );
    }

    private Iterable<ConstraintDefinition> filteredNodeConstraints( String labelName, final ConstraintType type )
    {
        return filter(new Predicate<ConstraintDefinition>(){
            @Override
            public boolean test( ConstraintDefinition item )
            {
                return item.isConstraintType( type );
            }
        }, graphDb.schema().getConstraints( label( labelName ) ) );
    }

    private Iterable<ConstraintDefinition> filteredRelationshipConstraints( String typeName, final ConstraintType type )
    {
        return filter( new Predicate<ConstraintDefinition>()
        {
            @Override
            public boolean test( ConstraintDefinition item )
            {
                return item.isConstraintType( type );
            }
        }, graphDb.schema().getConstraints( DynamicRelationshipType.withName( typeName ) ) );
    }

    private Predicate<ConstraintDefinition> propertyUniquenessFilter( final Set<String> propertyKeysSet )
    {
        return new Predicate<ConstraintDefinition>()
        {
            @Override
            public boolean test( ConstraintDefinition item )
            {
                return item.isConstraintType( ConstraintType.UNIQUENESS ) &&
                        propertyKeysSet.equals( asSet( item.getPropertyKeys() ) );
            }
        };
    }

    private Predicate<ConstraintDefinition> nodePropertyExistenceFilter( final Set<String> propertyKeysSet )
    {
        return new Predicate<ConstraintDefinition>()
        {
            @Override
            public boolean test( ConstraintDefinition item )
            {
                return item.isConstraintType( ConstraintType.NODE_PROPERTY_EXISTENCE ) &&
                       propertyKeysSet.equals( asSet( item.getPropertyKeys() ) );
            }
        };
    }

    private Predicate<ConstraintDefinition> relationshipPropertyExistenceFilter( final Set<String> propertyKeysSet )
    {
        return new Predicate<ConstraintDefinition>()
        {
            @Override
            public boolean test( ConstraintDefinition item )
            {
                return item.isConstraintType( ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE ) &&
                       propertyKeysSet.equals( asSet( item.getPropertyKeys() ) );
            }
        };
    }

    public ListRepresentation getConstraints()
    {
        return new ListRepresentation( CONSTRAINT_DEFINITION,
                map( CONSTRAINT_DEF_TO_REPRESENTATION, graphDb.schema().getConstraints() ) );
    }

    public ListRepresentation getLabelConstraints( String labelName )
    {
        return new ListRepresentation( CONSTRAINT_DEFINITION, map( CONSTRAINT_DEF_TO_REPRESENTATION,
                filteredNodeConstraints( labelName, Predicates.<ConstraintDefinition>alwaysTrue() ) ) );
    }

    public Representation getLabelUniquenessConstraints( String labelName )
    {
        return new ListRepresentation( CONSTRAINT_DEFINITION, map( CONSTRAINT_DEF_TO_REPRESENTATION,
                filteredNodeConstraints( labelName, ConstraintType.UNIQUENESS ) ) );
    }

    public Representation getLabelExistenceConstraints( String labelName )
    {
        return new ListRepresentation( CONSTRAINT_DEFINITION, map( CONSTRAINT_DEF_TO_REPRESENTATION,
                filteredNodeConstraints( labelName, ConstraintType.NODE_PROPERTY_EXISTENCE ) ) );
    }

    public Representation getRelationshipTypeExistenceConstraints( String typeName )
    {
        return new ListRepresentation( CONSTRAINT_DEFINITION, map( CONSTRAINT_DEF_TO_REPRESENTATION,
                filteredRelationshipConstraints( typeName, ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE ) ) );
    }
}
