/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.function.primitive.FunctionFromPrimitiveInt;
import org.neo4j.function.primitive.FunctionFromPrimitiveLong;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Traverser;
import org.neo4j.graphdb.Traverser.Order;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.helpers.collection.ResourceClosingIterator;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementTokenNameLookup;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.exceptions.ReadOnlyDatabaseKernelException;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.operations.KeyReadOperations;
import org.neo4j.kernel.impl.traversal.OldTraverserWrapper;

import static java.lang.String.format;

import static org.neo4j.collection.primitive.PrimitiveIntCollections.map;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.map;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.Iterables.asResourceIterable;
import static org.neo4j.helpers.collection.IteratorUtil.asList;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP_TYPE;
import static org.neo4j.kernel.impl.core.TokenHolder.NO_ID;

public class NodeProxy implements Node
{
    public interface NodeLookup
    {
        GraphDatabaseService getGraphDatabase();

        NodeManager getNodeManager();
    }

    private final NodeLookup nodeLookup;
    private final RelationshipProxy.RelationshipLookups relLookup;
    private final ThreadToStatementContextBridge statementContextProvider;
    private final long nodeId;

    public NodeProxy( long nodeId, NodeLookup nodeLookup, RelationshipProxy.RelationshipLookups relLookup,
               ThreadToStatementContextBridge statementContextProvider )
    {
        this.nodeId = nodeId;
        this.nodeLookup = nodeLookup;
        this.relLookup = relLookup;
        this.statementContextProvider = statementContextProvider;
    }

    @Override
    public long getId()
    {
        return nodeId;
    }

    @Override
    public GraphDatabaseService getGraphDatabase()
    {
        return nodeLookup.getGraphDatabase();
    }

    @Override
    public void delete()
    {
        try ( Statement statement = statementContextProvider.instance() )
        {
            statement.dataWriteOperations().nodeDelete( getId() );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
        catch ( ReadOnlyDatabaseKernelException e )
        {
            throw new ReadOnlyDbException();
        }
    }

    @Override
    public ResourceIterable<Relationship> getRelationships()
    {
        return getRelationships( Direction.BOTH );
    }

    @Override
    public ResourceIterable<Relationship> getRelationships( Direction dir )
    {
        Statement statement = statementContextProvider.instance();
        try
        {
            return map2rels( statement, statement.readOperations().nodeGetRelationships( nodeId, dir ) );
        }
        catch ( EntityNotFoundException e )
        {
            statement.close();
            throw new NotFoundException( format( "Node %d not found", nodeId ), e );
        }
    }

    @Override
    public ResourceIterable<Relationship> getRelationships( RelationshipType... types )
    {
        return getRelationships( Direction.BOTH, types );
    }

    @Override
    public ResourceIterable<Relationship> getRelationships( RelationshipType type, Direction dir )
    {
        return getRelationships(dir, type);
    }

    @Override
    public ResourceIterable<Relationship> getRelationships( Direction direction, RelationshipType ... types )
    {
        Statement statement = statementContextProvider.instance();
        try
        {
            return map2rels( statement, statement.readOperations().nodeGetRelationships( nodeId, direction,
                    relTypeIds( types, statement ) ) );
        }
        catch ( EntityNotFoundException e )
        {
            statement.close();
            throw new NotFoundException( format( "Node %d not found", nodeId ), e );
        }
    }

    @Override
    public boolean hasRelationship()
    {
        return hasRelationship( Direction.BOTH );
    }

    @Override
    public boolean hasRelationship( Direction dir )
    {
        try(ResourceIterator<Relationship> rels = getRelationships(dir).iterator())
        {
            return rels.hasNext();
        }
    }

    @Override
    public boolean hasRelationship( RelationshipType... types )
    {
        return hasRelationship(Direction.BOTH, types);
    }

    @Override
    public boolean hasRelationship( Direction direction, RelationshipType... types )
    {
        try(ResourceIterator<Relationship> rels = getRelationships(direction, types).iterator())
        {
            return rels.hasNext();
        }
    }

    @Override
    public boolean hasRelationship( RelationshipType type, Direction dir )
    {
        return hasRelationship(dir, type);
    }

    @Override
    public Relationship getSingleRelationship( RelationshipType type, Direction dir )
    {
        try(ResourceIterator<Relationship> rels = getRelationships(dir, type).iterator())
        {
            if(!rels.hasNext())
            {
                return null;
            }

            Relationship rel = rels.next();
            while ( rels.hasNext() )
            {
                Relationship other = rels.next();
                if ( !other.equals( rel ) )
                {
                    throw new NotFoundException( "More than one relationship[" +
                            type + ", " + dir + "] found for " + this );
                }
            }
            return rel;
        }
    }

    private void assertInUninterruptedTransaction()
    {
        statementContextProvider.assertInUninterruptedTransaction();
    }

    @Override
    public void setProperty( String key, Object value )
    {
        try ( Statement statement = statementContextProvider.instance() )
        {
            int propertyKeyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( key );
            try
            {
                statement.dataWriteOperations().nodeSetProperty( nodeId, Property.property( propertyKeyId, value ) );
            }
            catch ( ConstraintValidationKernelException e )
            {
                throw new ConstraintViolationException( e.getUserMessage( new StatementTokenNameLookup( statement.readOperations() ) ), e );
            } catch (IllegalArgumentException e)
            {
                // Trying to set an illegal value is a critical error - fail this transaction
                statementContextProvider.getKernelTransactionBoundToThisThread( true ).failure();
                throw e;
            }
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( e );
        }
        catch ( IllegalTokenNameException e )
        {
            throw new IllegalArgumentException( format( "Invalid property key '%s'.", key ), e );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
        catch ( ReadOnlyDatabaseKernelException e )
        {
            throw new ReadOnlyDbException();
        }
    }

    @Override
    public Object removeProperty( String key ) throws NotFoundException
    {
        try ( Statement statement = statementContextProvider.instance() )
        {
            int propertyKeyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( key );
            return statement.dataWriteOperations().nodeRemoveProperty( nodeId, propertyKeyId ).value( null );
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( e );
        }
        catch ( IllegalTokenNameException e )
        {
            throw new IllegalArgumentException( format( "Invalid property key '%s'.", key ), e );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
        catch ( ReadOnlyDatabaseKernelException e )
        {
            throw new ReadOnlyDbException();
        }
    }

    @Override
    public Object getProperty( String key, Object defaultValue )
    {
        if ( null == key )
        {
            throw new IllegalArgumentException( "(null) property key is not allowed" );
        }

        try ( Statement statement = statementContextProvider.instance() )
        {
            int propertyKeyId = statement.readOperations().propertyKeyGetForName( key );
            return statement.readOperations().nodeGetProperty( nodeId, propertyKeyId ).value( defaultValue );
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( e );
        }
    }

    @Override
    public Iterable<String> getPropertyKeys()
    {
        try ( Statement statement = statementContextProvider.instance() )
        {
            List<String> keys = new ArrayList<>();
            Iterator<DefinedProperty> properties = statement.readOperations().nodeGetAllProperties( getId() );
            while ( properties.hasNext() )
            {
                keys.add( statement.readOperations().propertyKeyGetName( properties.next().propertyKeyId() ) );
            }
            return keys;
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( "Node not found", e );
        }
        catch ( PropertyKeyIdNotFoundKernelException e )
        {
            throw new ThisShouldNotHappenError( "Jake",
                    "Property key retrieved through kernel API should exist.", e );
        }
    }

    @Override
    public Object getProperty( String key ) throws NotFoundException
    {
        if ( null == key )
        {
            throw new IllegalArgumentException( "(null) property key is not allowed" );
        }

        try ( Statement statement = statementContextProvider.instance() )
        {
            try
            {
                int propertyKeyId = statement.readOperations().propertyKeyGetForName( key );
                if ( propertyKeyId == KeyReadOperations.NO_SUCH_PROPERTY_KEY )
                {
                    throw new NotFoundException( format( "No such property, '%s'.", key ) );
                }
                return statement.readOperations().nodeGetProperty( nodeId, propertyKeyId ).value();
            }
            catch ( EntityNotFoundException | PropertyNotFoundException e )
            {
                throw new NotFoundException(
                        e.getUserMessage( new StatementTokenNameLookup( statement.readOperations() ) ), e );
            }
        }
    }

    @Override
    public boolean hasProperty( String key )
    {
        if ( null == key )
        {
            return false;
        }

        try ( Statement statement = statementContextProvider.instance() )
        {
            int propertyKeyId = statement.readOperations().propertyKeyGetForName( key );
            return statement.readOperations().nodeGetProperty( nodeId, propertyKeyId ).isDefined();
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( e );
        }
    }

    public int compareTo( Object node )
    {
        Node n = (Node) node;
        long ourId = this.getId(), theirId = n.getId();

        if ( ourId < theirId )
        {
            return -1;
        }
        else if ( ourId > theirId )
        {
            return 1;
        }
        else
        {
            return 0;
        }
    }

    @Override
    public boolean equals( Object o )
    {
        return o instanceof Node && this.getId() == ((Node) o).getId();
    }

    @Override
    public int hashCode()
    {
        return (int) ((nodeId >>> 32) ^ nodeId);
    }

    @Override
    public String toString()
    {
        return "Node[" + this.getId() + "]";
    }

    @Override
    public Relationship createRelationshipTo( Node otherNode, RelationshipType type )
    {
        if ( otherNode == null )
        {
            throw new IllegalArgumentException( "Other node is null." );
        }
        // TODO: This is the checks we would like to do, but we have tests that expect to mix nodes...
        //if ( !(otherNode instanceof NodeProxy) || (((NodeProxy) otherNode).nodeLookup != nodeLookup) )
        //{
        //    throw new IllegalArgumentException( "Nodes do not belong to same graph database." );
        //}
        try ( Statement statement = statementContextProvider.instance() )
        {
            int relationshipTypeId = statement.tokenWriteOperations().relationshipTypeGetOrCreateForName( type.name() );
            return nodeLookup.getNodeManager().newRelationshipProxyById(
                    statement.dataWriteOperations().relationshipCreate( relationshipTypeId, nodeId, otherNode.getId() ) );
        }
        catch ( IllegalTokenNameException | RelationshipTypeIdNotFoundKernelException e )
        {
            throw new IllegalArgumentException( e );
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( e );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
        catch ( ReadOnlyDatabaseKernelException e )
        {
            throw new ReadOnlyDbException();
        }
    }

    @Override
    public Traverser traverse( Order traversalOrder,
                               StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator,
                               RelationshipType relationshipType, Direction direction )
    {
        assertInUninterruptedTransaction();
        return OldTraverserWrapper.traverse( this,
                traversalOrder, stopEvaluator,
                returnableEvaluator, relationshipType, direction );
    }

    @Override
    public Traverser traverse( Order traversalOrder,
                               StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator,
                               RelationshipType firstRelationshipType, Direction firstDirection,
                               RelationshipType secondRelationshipType, Direction secondDirection )
    {
        assertInUninterruptedTransaction();
        return OldTraverserWrapper.traverse( this,
                traversalOrder, stopEvaluator,
                returnableEvaluator, firstRelationshipType, firstDirection,
                secondRelationshipType, secondDirection );
    }

    @Override
    public Traverser traverse( Order traversalOrder,
                               StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator,
                               Object... relationshipTypesAndDirections )
    {
        assertInUninterruptedTransaction();
        return OldTraverserWrapper.traverse( this,
                traversalOrder, stopEvaluator,
                returnableEvaluator, relationshipTypesAndDirections );
    }

    @Override
    public void addLabel( Label label )
    {
        try ( Statement statement = statementContextProvider.instance() )
        {
            try
            {
                statement.dataWriteOperations().nodeAddLabel( getId(), statement.tokenWriteOperations().labelGetOrCreateForName( label.name() ) );
            }
            catch ( ConstraintValidationKernelException e )
            {
                throw new ConstraintViolationException( e.getUserMessage( new StatementTokenNameLookup( statement.readOperations() ) ), e );
            }
        }
        catch ( IllegalTokenNameException e )
        {
            throw new ConstraintViolationException( format( "Invalid label name '%s'.", label.name() ), e );
        }
        catch ( TooManyLabelsException e )
        {
            throw new ConstraintViolationException( "Unable to add label.", e );
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( "No node with id " + getId() + " found.", e );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
        catch ( ReadOnlyDatabaseKernelException e )
        {
            throw new ReadOnlyDbException();
        }
    }

    @Override
    public void removeLabel( Label label )
    {
        try ( Statement statement = statementContextProvider.instance() )
        {
            int labelId = statement.readOperations().labelGetForName( label.name() );
            if ( labelId != KeyReadOperations.NO_SUCH_LABEL )
            {
                statement.dataWriteOperations().nodeRemoveLabel( getId(), labelId );
            }
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( "No node with id " + getId() + " found.", e );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
        catch ( ReadOnlyDatabaseKernelException e )
        {
            throw new ReadOnlyDbException();
        }
    }

    @Override
    public boolean hasLabel( Label label )
    {
        try ( Statement statement = statementContextProvider.instance() )
        {
            int labelId = statement.readOperations().labelGetForName( label.name() );
            return statement.readOperations().nodeHasLabel( getId(), labelId );
        }
        catch ( EntityNotFoundException e )
        {
            return false;
        }
    }

    @Override
    public Iterable<Label> getLabels()
    {
        try ( Statement statement = statementContextProvider.instance() )
        {
            PrimitiveIntIterator labels = statement.readOperations().nodeGetLabels( getId() );
            List<Label> keys = new ArrayList<>();
            while ( labels.hasNext() )
            {
                int labelId = labels.next();
                keys.add( label( statement.readOperations().labelGetName( labelId ) ) );
            }
            return keys;
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( "Node not found", e );
        }
        catch ( LabelNotFoundKernelException e )
        {
            throw new ThisShouldNotHappenError( "Stefan", "Label retrieved through kernel API should exist." );
        }
    }

    @Override
    public int getDegree()
    {
        try ( Statement statement = statementContextProvider.instance() )
        {
            return statement.readOperations().nodeGetDegree(nodeId, Direction.BOTH);
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( "Node not found.", e );
        }
    }

    @Override
    public int getDegree( RelationshipType type )
    {
        try ( Statement statement = statementContextProvider.instance() )
        {
            ReadOperations ops = statement.readOperations();
            int typeId = ops.relationshipTypeGetForName( type.name() );
            if ( typeId == NO_ID )
            {   // This type doesn't even exist. Return 0
                return 0;
            }
            return ops.nodeGetDegree( nodeId, Direction.BOTH, typeId );
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( "Node not found.", e );
        }
    }

    @Override
    public int getDegree( Direction direction )
    {
        try ( Statement statement = statementContextProvider.instance() )
        {
            ReadOperations ops = statement.readOperations();
            return ops.nodeGetDegree( nodeId, direction );
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( "Node not found.", e );
        }
    }

    @Override
    public int getDegree( RelationshipType type, Direction direction )
    {
        try ( Statement statement = statementContextProvider.instance() )
        {
            ReadOperations ops = statement.readOperations();
            int typeId = ops.relationshipTypeGetForName( type.name() );
            if ( typeId == NO_ID )
            {   // This type doesn't even exist. Return 0
                return 0;
            }
            return ops.nodeGetDegree( nodeId, direction, typeId );
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( "Node not found.", e );
        }
    }

    @Override
    public Iterable<RelationshipType> getRelationshipTypes()
    {
        try(Statement statement = statementContextProvider.instance())
        {
            ReadOperations ops = statement.readOperations();
            return map2relTypes( statement, ops.nodeGetRelationshipTypes( nodeId ) );
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( "Node not found.", e );
        }
    }

    private int[] relTypeIds( RelationshipType[] types, Statement statement )
    {
        int[] ids = new int[types.length];
        int outIndex = 0;
        for(int i=0;i<types.length;i++)
        {
            int id = statement.readOperations().relationshipTypeGetForName( types[i].name() );
            if(id != NO_SUCH_RELATIONSHIP_TYPE )
            {
                ids[outIndex++] = id;
            }
        }

        if(outIndex != ids.length)
        {
            // One or more relationship types do not exist, so we can exclude them right away.
            ids = Arrays.copyOf(ids, outIndex);
        }
        return ids;
    }

    private ResourceIterable<Relationship> map2rels( Statement statement, PrimitiveLongIterator input )
    {
        return asResourceIterable( ResourceClosingIterator.newResourceIterator( statement, map( new FunctionFromPrimitiveLong
                <Relationship>()
        {
            @Override
            public Relationship apply( long id )
            {
                return new RelationshipProxy( id, relLookup, statementContextProvider );
            }
        }, input ) ) );
    }

    private Iterable<RelationshipType> map2relTypes( final Statement statement, PrimitiveIntIterator input )
    {
        return asList( map( new FunctionFromPrimitiveInt<RelationshipType>()
        {
            @Override
            public RelationshipType apply( int id )
            {
                try
                {
                    return DynamicRelationshipType.withName( statement.readOperations().relationshipTypeGetName( id ) );
                }
                catch ( RelationshipTypeIdNotFoundKernelException e )
                {
                    throw new ThisShouldNotHappenError( "Jake",
                            "Kernel API returned non-existent relationship type: " + id );
                }
            }
        }, input ) );
    }
}
