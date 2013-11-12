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
package org.neo4j.kernel.impl.merge;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.MergeResult;
import org.neo4j.graphdb.Merger;
import org.neo4j.graphdb.Node;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.PrimitiveLongPredicate;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ReadOnlyDatabaseKernelException;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.TokenWriteOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.operations.StatementTokenNameLookup;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.api.PrimitiveLongIterator;
import org.neo4j.kernel.impl.api.constraints.ConstraintValidationKernelException;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.coreapi.ThreadToStatementContextBridge;

import static java.lang.String.format;

import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE;
import static org.neo4j.kernel.api.properties.Property.property;

public class NodeMerger implements Merger<Node>
{
    public static NodeMerger createMerger( ThreadToStatementContextBridge statementContextProvider,
                                           NodeManager nodeManager, Label[] labels )
    {
        if ( labels == null )
        {
            throw new IllegalArgumentException( "Requires an array of labels" );
        }

        try ( Statement statement = statementContextProvider.instance() )
        {
            try
            {
                TokenWriteOperations tokenOperations = statement.tokenWriteOperations();
                return new NodeMerger( statementContextProvider, nodeManager,
                                       uniqueLabelIds( tokenOperations, labels ) );
            }
            catch ( IllegalTokenNameException e )
            {
                throw new IllegalArgumentException(
                        e.getUserMessage( new StatementTokenNameLookup( statement.readOperations() ) ), e );
            }
            catch ( ReadOnlyDatabaseKernelException | TooManyLabelsException e )
            {
                throw new IllegalStateException(
                        e.getUserMessage( new StatementTokenNameLookup( statement.readOperations() ) ), e );
            }
        }
    }

    private static int[] uniqueLabelIds( TokenWriteOperations tokenOperations, Label[] labels )
            throws IllegalTokenNameException, TooManyLabelsException
    {
        int[] labelIds = new int[labels.length];
        int duplicates = 0;
        LABELS:
        for ( int i = 0; i < labelIds.length; i++ )
        {
            Label current = labels[i];
            int labelId = tokenOperations.labelGetOrCreateForName( current.name() );
            for ( int j = 0; j < i; j++ )
            {
                if ( labelIds[j] == labelId )
                {
                    duplicates++;
                    continue LABELS;
                }
            }
            labelIds[i - duplicates] = labelId;
        }
        if ( duplicates > 0 )
        {
            labelIds = Arrays.copyOf( labelIds, labelIds.length - duplicates );
        }
        return labelIds;
    }

    private final ThreadToStatementContextBridge statementContextProvider;
    private final NodeManager nodeManager;

    final int[] labelIds;
    final DefinedProperty[] properties;

    private NodeMerger( ThreadToStatementContextBridge statementContextProvider,
                        NodeManager nodeManager,
                        int[] labelIds )
    {
        this.statementContextProvider = statementContextProvider;
        this.nodeManager = nodeManager;

        this.labelIds = labelIds;
        this.properties = DefinedProperty.EMPTY_ARRAY;
    }

    @Override
    public NodeMerger withProperty( String key, Object value )
    {
        try ( Statement statement = statementContextProvider.instance() )
        {
            try
            {
                int propertyKeyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( key );
                for ( DefinedProperty property : properties )
                {
                    if ( property.propertyKeyId() == propertyKeyId )
                    {
                        throw new IllegalArgumentException( format( "Multiple values given for property '%s'.", key ) );
                    }
                }
                return new NodeMerger( this, property( propertyKeyId, value ) );
            }
            catch ( IllegalTokenNameException e )
            {
                throw new IllegalArgumentException(
                        e.getUserMessage( new StatementTokenNameLookup( statement.readOperations() ) ), e );
            }
            catch ( ReadOnlyDatabaseKernelException e )
            {
                throw new IllegalStateException(
                        e.getUserMessage( new StatementTokenNameLookup( statement.readOperations() ) ), e );
            }
        }
    }

    private NodeMerger( NodeMerger previous, DefinedProperty property )
    {
        this.statementContextProvider = previous.statementContextProvider;
        this.nodeManager = previous.nodeManager;
        this.labelIds = previous.labelIds;
        this.properties = Arrays.copyOf( previous.properties, previous.properties.length + 1 );
        this.properties[previous.properties.length] = property;
    }

    @Override
    public MergeResult<Node> iterator()
    {
        return merge();
    }

    @Override
    public MergeResult<Node> merge()
    {
        try ( Statement statement = statementContextProvider.instance() )
        {
            ReadOperations readOps = statement.readOperations();
            try
            {
                // build all strategies
                List<NodeMergeStrategy> strategies = buildNodeMergeStrategies( readOps );
                Statement resultStatement = statementContextProvider.instance();

                if ( strategies.isEmpty() )
                {
                    return scanAllNodes( resultStatement );
                }
                else
                {
                    // if we find at least one strategy
                    NodeMergeStrategy firstStrategy = strategies.get( 0 );

                    if ( firstStrategy.type == NodeMergeStrategy.Type.UNIQUE_INDEX )
                    {

                        // and there is at least one strategy that uses a unique index: find single node or none
                        return intersectAllStrategies( resultStatement, strategies );
                    }
                    else
                    {
                        // else: scan single
                        return scanSingleStrategy( resultStatement, firstStrategy );
                    }
                }
            }
            catch ( ReadOnlyDatabaseKernelException | InvalidTransactionTypeKernelException e )
            {
                throw new IllegalStateException( e.getUserMessage( new StatementTokenNameLookup( readOps ) ), e );
            }
            catch ( ConstraintValidationKernelException e )
            {
                throw new ConstraintViolationException(
                        e.getUserMessage( new StatementTokenNameLookup( readOps ) ), e );
            }
            catch ( EntityNotFoundException e )
            {
                throw new ThisShouldNotHappenError( "Tobias", "Node that we just created should exist.", e );
            }
        }
    }

    private MergeResult<Node> scanAllNodes( Statement statement )
            throws ConstraintValidationKernelException, EntityNotFoundException,
                   InvalidTransactionTypeKernelException, ReadOnlyDatabaseKernelException

    {
        Iterator<Node> allNodes = nodeManager.getAllNodes();
        Iterator<Node> iterator = filter( new FilterMatchingNodes( NO_SUCH_NODE ), allNodes );
        if ( iterator.hasNext() )
        {
            return new NodeIteratorResult( statement, iterator );
        }
        else
        {
            long nodeId = createNewNode( statement );
            return new SingleMergeResult<Node>( statement, nodeProxy( nodeId ), true );
        }
    }

    private MergeResult<Node> scanSingleStrategy( Statement statement, NodeMergeStrategy first )
            throws InvalidTransactionTypeKernelException, ReadOnlyDatabaseKernelException,
                   EntityNotFoundException, ConstraintValidationKernelException
    {
        try
        {
            ReadOperations readOps = statement.readOperations();
            PrimitiveLongIterator nodeIds = first.lookupAll( readOps, new FilterMatchingNodes( NO_SUCH_NODE ) );
            if ( nodeIds.hasNext() )
            {
                return new MultiNodeMergeResult( statement, nodeManager, nodeIds );
            }
            else
            {
                long nodeId = createNewNode( statement );
                return new SingleMergeResult<Node>( statement, nodeProxy( nodeId ), true );
            }
        }
        catch ( IndexBrokenKernelException e )
        {
            throw new RuntimeException( e );
        }
    }

    private MergeResult<Node> intersectAllStrategies( Statement statement, List<NodeMergeStrategy> strategies )
            throws InvalidTransactionTypeKernelException, ReadOnlyDatabaseKernelException,
                   EntityNotFoundException, ConstraintValidationKernelException
    {
        long nodeId = NO_SUCH_NODE;
        boolean wasCreated;

        for ( NodeMergeStrategy strategy : strategies )
        {
            nodeId = strategy.merge( statement, nodeId, new FilterMatchingNodes( nodeId ) );
        }
        if ( nodeId == NO_SUCH_NODE ) // no node was found - create a node!
        {
            nodeId = createNewNode( statement );
            wasCreated = true;
        }
        else
        {
            wasCreated = false;
        }

        return new SingleMergeResult<Node>( statement, nodeProxy( nodeId ), wasCreated );
    }

    private NodeProxy nodeProxy( long nodeId )
    {
        return nodeManager.newNodeProxyById( nodeId );
    }

    private List<NodeMergeStrategy> buildNodeMergeStrategies( ReadOperations ops )
    {
        NodeMergeStrategyBuilder builder = new NodeMergeStrategyBuilder();

        // permutate labels and properties
        for ( int labelId : labelIds )
        {
            Set<Integer> uniqueIndexedKeys = indexKeySet( ops.uniqueIndexesGetForLabel( labelId ) );
            Set<Integer> indexedKeys = indexKeySet( ops.indexesGetForLabel( labelId ) );

            for ( DefinedProperty property : properties )
            {
                int propertyKeyId = property.propertyKeyId();

                if ( uniqueIndexedKeys.contains( propertyKeyId ) )
                {
                    IndexDescriptor index = new IndexDescriptor( labelId, propertyKeyId );
                    builder.addUsingUniqueIndex( labelId, property, index );
                    continue;
                }

                if ( indexedKeys.contains( propertyKeyId ) )
                {
                    IndexDescriptor index = new IndexDescriptor( labelId, propertyKeyId );

                    try
                    {
                        if ( InternalIndexState.ONLINE == ops.indexGetState( index ) )
                        {
                            builder.addUsingIndex( labelId, property, index );
                            continue;
                        }
                    }
                    catch ( IndexNotFoundKernelException ignored )
                    {
                        // probably something weird is going on if we get that but from the viewpoint of
                        // merge we are safe to just ignore that index
                    }
                }

                builder.addUsingLabel( labelId, property );
            }
        }

        return builder.buildStrategies();
    }

    private long createNewNode( Statement statement )
            throws InvalidTransactionTypeKernelException, ReadOnlyDatabaseKernelException, EntityNotFoundException,
            ConstraintValidationKernelException
    {
        long nodeId;
        DataWriteOperations writeOps = statement.dataWriteOperations();

        nodeId = writeOps.nodeCreate();
        for ( int labelId : labelIds )
        {
            writeOps.nodeAddLabel( nodeId, labelId );
        }
        for ( DefinedProperty property : properties )
        {
            writeOps.nodeSetProperty( nodeId, property );
        }
        return nodeId;
    }

    private Set<Integer> indexKeySet( Iterator<IndexDescriptor> iterator )
    {
        Set<Integer> result = new HashSet<>();

        if ( iterator != null )
        {
            while ( iterator.hasNext() )
            {
                result.add( iterator.next().getPropertyKeyId() );
            }
        }

        return result;
    }

    // this predicate accepts only nodes that match all requirements (labelIds, pkIds)
    private class FilterMatchingNodes implements PrimitiveLongPredicate, Predicate<Node>
    {
        private final long expectedId;

        private FilterMatchingNodes( long expectedId )
        {
            this.expectedId = expectedId;
        }

        @Override
        public boolean accept( long nodeId )
        {
            statementContextProvider.assertInTransaction();
            try ( Statement statement = statementContextProvider.instance() )
            {
                ReadOperations ops = statement.readOperations();

                if ( expectedId == nodeId )
                {
                    return NO_SUCH_NODE != nodeId;
                }

                for ( int labelId : labelIds )
                {
                    if ( ! ops.nodeHasLabel( nodeId, labelId ) )
                    {
                        return false;
                    }

                }
                for ( DefinedProperty property : properties )
                {
                    if ( ! ops.nodeGetProperty( nodeId, property.propertyKeyId() ).equals( property ) )
                    {
                        return false;
                    }
                }

                return true;
            }
            catch ( EntityNotFoundException e )
            {
                throw new ThisShouldNotHappenError( "Stefan", "Could not read previously retrieved node" );
            }
        }

        @Override
        public boolean accept( Node item )
        {
            return accept( item.getId() );
        }
    }
}

