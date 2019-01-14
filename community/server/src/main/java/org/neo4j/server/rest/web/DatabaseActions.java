/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import org.neo4j.function.Predicates;
import org.neo4j.graphalgo.CommonEvaluators;
import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.PathExpanderBuilder;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.Paths;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.rest.domain.EndNodeNotFoundException;
import org.neo4j.server.rest.domain.PropertySettingStrategy;
import org.neo4j.server.rest.domain.RelationshipExpanderBuilder;
import org.neo4j.server.rest.domain.StartNodeNotFoundException;
import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.ConstraintDefinitionRepresentation;
import org.neo4j.server.rest.repr.DatabaseRepresentation;
import org.neo4j.server.rest.repr.IndexDefinitionRepresentation;
import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.server.rest.repr.NodeRepresentation;
import org.neo4j.server.rest.repr.PathRepresentation;
import org.neo4j.server.rest.repr.PropertiesRepresentation;
import org.neo4j.server.rest.repr.RelationshipRepresentation;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.server.rest.repr.RepresentationType;
import org.neo4j.server.rest.repr.ValueRepresentation;
import org.neo4j.server.rest.repr.WeightedPathRepresentation;

import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.server.rest.repr.RepresentationType.CONSTRAINT_DEFINITION;

public class DatabaseActions
{
    private final GraphDatabaseAPI graphDb;

    private final PropertySettingStrategy propertySetter;

    private final Function<ConstraintDefinition,Representation> CONSTRAINT_DEF_TO_REPRESENTATION =
            ConstraintDefinitionRepresentation::new;

    public DatabaseActions( GraphDatabaseAPI graphDatabaseAPI )
    {
        this.graphDb = graphDatabaseAPI;
        this.propertySetter = new PropertySettingStrategy( graphDb );
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

    public NodeRepresentation createNode( Map<String,Object> properties, Label... labels )
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
        Collection<ValueRepresentation> propKeys =
                Iterables.asSet( map( ValueRepresentation::string, graphDb.getAllPropertyKeys() ) );

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
            Map<String,Object> properties ) throws PropertyValueException,
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
        Iterable<String> labels = new IterableWrapper<String,Label>( node( nodeId ).getLabels() )
        {
            @Override
            protected String underlyingObjectToObject( Label object )
            {
                return object.name();
            }
        };
        return ListRepresentation.string( labels );
    }

    // Relationships

    public enum RelationshipDirection
    {
        all( Direction.BOTH ),
        in( Direction.INCOMING ),
        out( Direction.OUTGOING );
        final Direction internal;

        RelationshipDirection( Direction internal )
        {
            this.internal = internal;
        }
    }

    public RelationshipRepresentation createRelationship( long startNodeId,
            long endNodeId, String type, Map<String,Object> properties )
            throws StartNodeNotFoundException, EndNodeNotFoundException,
            PropertyValueException
    {

        Node start;
        Node end;
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
                RelationshipType.withName( type ) );

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

    @SuppressWarnings( "unchecked" )
    public ListRepresentation getNodeRelationships( long nodeId,
            RelationshipDirection direction, Collection<String> types )
            throws NodeNotFoundException
    {
        Node node = node( nodeId );
        PathExpander expander;
        if ( types.isEmpty() )
        {
            expander = PathExpanders.forDirection( direction.internal );
        }
        else
        {
            PathExpanderBuilder builder = PathExpanderBuilder.empty();
            for ( String type : types )
            {
                builder = builder.add(
                        RelationshipType.withName( type ),
                        direction.internal );
            }
            expander = builder.build();
        }
        return RelationshipRepresentation.list( expander.expand( Paths.singleNodePath( node ), BranchState.NO_STATE ) );
    }

    // Node degrees

    @SuppressWarnings( "unchecked" )
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
                sum += node.getDegree( RelationshipType.withName( type ), direction.internal );
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
            Map<String,Object> properties ) throws PropertyValueException,
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

    // Graph algos

    @SuppressWarnings( "rawtypes" )
    public PathRepresentation findSinglePath( long startId, long endId,
            Map<String,Object> map )
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

    @SuppressWarnings( {"rawtypes", "unchecked"} )
    public ListRepresentation findPaths( long startId, long endId,
            Map<String,Object> map )
    {
        final FindParams findParams = new FindParams( startId, endId, map ).invoke();
        PathFinder finder = findParams.getFinder();
        Node startNode = findParams.getStartNode();
        Node endNode = findParams.getEndNode();

        Iterable paths = finder.findAllPaths( startNode, endNode );

        IterableWrapper<PathRepresentation,Path> pathRepresentations = new IterableWrapper<PathRepresentation,Path>(
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
        private final Map<String,Object> map;
        private Node startNode;
        private Node endNode;
        private PathFinder<? extends Path> finder;
        @SuppressWarnings( "rawtypes" )
        private PathRepresentationCreator representationCreator = PATH_REPRESENTATION_CREATOR;

        FindParams( final long startId, final long endId, final Map<String,Object> map )
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

        @SuppressWarnings( "unchecked" )
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

            PathExpander expander = RelationshipExpanderBuilder.describeRelationships( map );

            String algorithm = (String) map.get( "algorithm" );
            algorithm = (algorithm != null) ? algorithm : "shortestPath";

            finder = getAlgorithm( algorithm, expander, maxDepth );
            return this;
        }

        private PathFinder<? extends Path> getAlgorithm( String algorithm,
                PathExpander expander, int maxDepth )
        {
            switch ( algorithm )
            {
            case "shortestPath":
                return GraphAlgoFactory.shortestPath( expander, maxDepth );
            case "allSimplePaths":
                return GraphAlgoFactory.allSimplePaths( expander, maxDepth );
            case "allPaths":
                return GraphAlgoFactory.allPaths( expander, maxDepth );
            case "dijkstra":
                String costProperty = (String) map.get( "cost_property" );
                Number defaultCost = (Number) map.get( "default_cost" );
                CostEvaluator<Double> costEvaluator =
                        defaultCost == null ? CommonEvaluators.doubleCostEvaluator( costProperty ) : CommonEvaluators
                                .doubleCostEvaluator( costProperty, defaultCost.doubleValue() );
                representationCreator = WEIGHTED_PATH_REPRESENTATION_CREATOR;
                return GraphAlgoFactory.dijkstra( expander, costEvaluator );
            default:
                throw new RuntimeException( "Failed to find matching algorithm" );
            }
        }
    }

    private interface PathRepresentationCreator<T extends Path>
    {
        PathRepresentation<T> from( T path );
    }

    private static final PathRepresentationCreator<Path> PATH_REPRESENTATION_CREATOR = PathRepresentation::new;

    private static final PathRepresentationCreator<WeightedPath> WEIGHTED_PATH_REPRESENTATION_CREATOR =
            WeightedPathRepresentation::new;

    public ListRepresentation getNodesWithLabel( String labelName, Map<String,Object> properties )
    {
        Iterator<Node> nodes;

        if ( properties.size() == 0 )
        {
            nodes = graphDb.findNodes( label( labelName ) );
        }
        else if ( properties.size() == 1 )
        {
            Map.Entry<String,Object> prop = Iterables.single( properties.entrySet() );
            nodes = graphDb.findNodes( label( labelName ), prop.getKey(), prop.getValue() );
        }
        else
        {
            throw new IllegalArgumentException( "Too many properties specified. Either specify one property to " +
                                                "filter by, or none at all." );
        }

        IterableWrapper<NodeRepresentation,Node> nodeRepresentations =
                new IterableWrapper<NodeRepresentation,Node>( asList( nodes ) )
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
        ResourceIterable<Label> labels = inUse ? graphDb.getAllLabelsInUse() : graphDb.getAllLabels();
        Collection<ValueRepresentation> labelNames = Iterables.asSet( map(
                label -> ValueRepresentation.string( label.name() ), labels ) );

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
        Iterable<IndexDefinitionRepresentation> representations = map( definition -> new IndexDefinitionRepresentation( definition,
                graphDb.schema().getIndexState( definition ),
                graphDb.schema().getIndexPopulationProgress( definition ) ), definitions );
        return new ListRepresentation( RepresentationType.INDEX_DEFINITION, representations );
    }

    public ListRepresentation getSchemaIndexes( String labelName )
    {
        Iterable<IndexDefinition> definitions = graphDb.schema().getIndexes( label( labelName ) );
        Iterable<IndexDefinitionRepresentation> representations = map( definition -> new IndexDefinitionRepresentation( definition,
                graphDb.schema().getIndexState( definition ),
                graphDb.schema().getIndexPopulationProgress( definition ) ), definitions );
        return new ListRepresentation( RepresentationType.INDEX_DEFINITION, representations );
    }

    public boolean dropSchemaIndex( String labelName, String propertyKey )
    {
        boolean found = false;
        for ( IndexDefinition index : graphDb.schema().getIndexes( label( labelName ) ) )
        {
            // TODO Assumption about single property key
            if ( propertyKey.equals( Iterables.single( index.getPropertyKeys() ) ) )
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
        final Set<String> propertyKeysSet = Iterables.asSet( propertyKeys );
        ConstraintDefinition constraint =
                Iterables.singleOrNull( filteredNodeConstraints( labelName, propertyUniquenessFilter( propertyKeysSet ) ) );
        if ( constraint != null )
        {
            constraint.drop();
        }
        return constraint != null;
    }

    public boolean dropNodePropertyExistenceConstraint( String labelName, Iterable<String> propertyKeys )
    {
        final Set<String> propertyKeysSet = Iterables.asSet( propertyKeys );
        ConstraintDefinition constraint =
                Iterables.singleOrNull( filteredNodeConstraints( labelName, nodePropertyExistenceFilter( propertyKeysSet ) ) );
        if ( constraint != null )
        {
            constraint.drop();
        }
        return constraint != null;
    }

    public boolean dropRelationshipPropertyExistenceConstraint( String typeName, Iterable<String> propertyKeys )
    {
        final Set<String> propertyKeysSet = Iterables.asSet( propertyKeys );
        ConstraintDefinition constraint = Iterables.singleOrNull( filteredRelationshipConstraints( typeName,
                relationshipPropertyExistenceFilter( propertyKeysSet ) ) );
        if ( constraint != null )
        {
            constraint.drop();
        }
        return constraint != null;
    }

    public ListRepresentation getNodePropertyExistenceConstraint( String labelName, Iterable<String> propertyKeys )
    {
        Set<String> propertyKeysSet = Iterables.asSet( propertyKeys );
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
        Set<String> propertyKeysSet = Iterables.asSet( propertyKeys );
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
        Set<String> propertyKeysSet = Iterables.asSet( propertyKeys );
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
        RelationshipType type = RelationshipType.withName( typeName );
        Iterable<ConstraintDefinition> constraints = graphDb.schema().getConstraints( type );
        return filter( filter, constraints );
    }

    private Iterable<ConstraintDefinition> filteredNodeConstraints( String labelName, final ConstraintType type )
    {
        return filter( item -> item.isConstraintType( type ), graphDb.schema().getConstraints( label( labelName ) ) );
    }

    private Iterable<ConstraintDefinition> filteredRelationshipConstraints( String typeName, final ConstraintType type )
    {
        return filter( item -> item.isConstraintType( type ), graphDb.schema().getConstraints( RelationshipType.withName( typeName ) ) );
    }

    private Predicate<ConstraintDefinition> propertyUniquenessFilter( final Set<String> propertyKeysSet )
    {
        return item -> item.isConstraintType( ConstraintType.UNIQUENESS ) &&
               propertyKeysSet.equals( Iterables.asSet( item.getPropertyKeys() ) );
    }

    private Predicate<ConstraintDefinition> nodePropertyExistenceFilter( final Set<String> propertyKeysSet )
    {
        return item -> item.isConstraintType( ConstraintType.NODE_PROPERTY_EXISTENCE ) &&
               propertyKeysSet.equals( Iterables.asSet( item.getPropertyKeys() ) );
    }

    private Predicate<ConstraintDefinition> relationshipPropertyExistenceFilter( final Set<String> propertyKeysSet )
    {
        return item -> item.isConstraintType( ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE ) &&
               propertyKeysSet.equals( Iterables.asSet( item.getPropertyKeys() ) );
    }

    public ListRepresentation getConstraints()
    {
        return new ListRepresentation( CONSTRAINT_DEFINITION,
                map( CONSTRAINT_DEF_TO_REPRESENTATION, graphDb.schema().getConstraints() ) );
    }

    public ListRepresentation getLabelConstraints( String labelName )
    {
        return new ListRepresentation( CONSTRAINT_DEFINITION, map( CONSTRAINT_DEF_TO_REPRESENTATION,
                filteredNodeConstraints( labelName, Predicates.alwaysTrue() ) ) );
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
