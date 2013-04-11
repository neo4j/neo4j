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
package org.neo4j.kernel.impl.api.state;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.api.DiffSets;
import org.neo4j.kernel.impl.api.index.SchemaIndexProviderMap;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.persistence.PersistenceManager;

/**
 * This organizes three disjoint containers of state. The goal is to bring that down to one, but for now, it's three.
 * Those three are:
 *
 *  * TxState - this class itself, containing HashMaps and DiffSets for changes
 *  * TransactionState - The legacy transaction state, to be refactored into this class.
 *  * WriteTransaction - More legacy transaction state, accessed through PersistenceManager.
 *
 * TransactionState is used to change the view of the data within a transaction, eg. see your own writes.
 *
 * WriteTransaction contains the changes that will actually be applied to the store, eg. records.
 *
 * TxState should be a common interface for *updating* both kinds of state, and for *reading* the first kind.
 *
 * So, in ascii art, the current implementation is:
 *
 *      StateHandlingTransactionContext-------TransactionStateStatementContext
 *                   \                                      /
 *                    ---------------------|----------------
 *                                         |
 *                                      TxState
 *                                     /      \
 *                       PersistenceManager   TransactionState
 *
 *
 * We want it to look like:
 *
 *      StateHandlingTransactionContext-------TransactionStateStatementContext
 *                   \                                      /
 *                    ---------------------|----------------
 *                                         |
 *                                      TxState
 *
 *
 * Where, in the end implementation, the state inside TxState can be used both to overlay on the graph, eg. read writes,
 * as well as be applied to the graph through the logical log.
 */
public class TxState
{
    public static interface IdGeneration
    {
        // Because we generate id's up-front, rather than on commit, we need this for now.
        // For a rainy day, we should refactor to use tx-local ids before commit and global ids after
        long newSchemaRuleId();

    }

    // Node ID --> NodeState
    private final Map<Long, NodeState> nodeStates = new HashMap<Long, NodeState>();

    // Label ID --> LabelState
    private final Map<Long, LabelState> labelStates = new HashMap<Long, LabelState>();

    private final DiffSets<IndexRule> ruleDiffSets = new DiffSets<IndexRule>();
    private final DiffSets<Long> nodes = new DiffSets<Long>();


    private final OldTxStateBridge legacyState;
    private final PersistenceManager persistenceManager;
    private final IdGeneration idGeneration;
    private final SchemaIndexProviderMap providerMap;

    public TxState( OldTxStateBridge legacyState,
                    PersistenceManager legacyTransaction,
                    IdGeneration idGeneration,
                    SchemaIndexProviderMap providerMap
    )
    {
        this.legacyState = legacyState;
        this.persistenceManager = legacyTransaction;
        this.idGeneration = idGeneration;
        this.providerMap = providerMap;
    }

    public boolean hasChanges()
    {
        return !nodeStates.isEmpty() || !labelStates.isEmpty() || !nodes.isEmpty() || legacyState.hasChanges();
    }

    public Iterable<NodeState> getNodeStates()
    {
        return nodeStates.values();
    }

    public DiffSets<Long> getLabelStateNodeDiffSets( long labelId )
    {
        return getOrCreateLabelState( labelId ).getNodeDiffSets();
    }

    public DiffSets<Long> getNodeStateLabelDiffSets( long nodeId )
    {
        return getOrCreateNodeState( nodeId ).getLabelDiffSets();
    }

    public void deleteNode( long nodeId )
    {
        legacyState.deleteNode( nodeId );
        nodes.remove( nodeId );
    }

    public boolean nodeIsDeletedInThisTx( long nodeId )
    {
        return nodes.isRemoved( nodeId );
    }

    public boolean nodeIsAddedInThisTx( long nodeId )
    {
        return legacyState.nodeIsAddedInThisTx(nodeId);
    }

    public void addLabelToNode( long labelId, long nodeId )
    {
        getLabelStateNodeDiffSets( labelId ).add( nodeId );
        getNodeStateLabelDiffSets( nodeId ).add( labelId );
        persistenceManager.addLabelToNode( labelId, nodeId );
    }

    public void removeLabelFromNode( long labelId, long nodeId )
    {
        getLabelStateNodeDiffSets( labelId ).remove( nodeId );
        getNodeStateLabelDiffSets( nodeId ).remove( labelId );
        persistenceManager.removeLabelFromNode( labelId, nodeId );
    }

    /**
     * @return {@code true} if it has been added in this transaction.
     *         {@code false} if it has been removed in this transaction.
     *         {@code null} if it has not been touched in this transaction.
     */
    public Boolean getLabelState( long nodeId, long labelId )
    {
        NodeState nodeState = getState( nodeStates, nodeId, null );
        if ( nodeState != null )
        {
            DiffSets<Long> labelDiff = nodeState.getLabelDiffSets();
            if ( labelDiff.isAdded( labelId ) )
            {
                return Boolean.TRUE;
            }
            if ( labelDiff.isRemoved( labelId ) )
            {
                return Boolean.FALSE;
            }
        }
        return null;
    }

    /**
     * Returns all nodes that, in this tx, has got labelId added.
     */
    public Set<Long> getNodesWithLabelAdded( long labelId )
    {
        LabelState state = getState( labelStates, labelId, null );
        return state == null ? Collections.<Long>emptySet() : state.getNodeDiffSets().getAdded();
    }

    /**
     * Returns all nodes that, in this tx, has got labelId removed.
     */
    public DiffSets<Long> getNodesWithLabelChanged( long labelId )
    {
        LabelState state = getState( labelStates, labelId, null );
        return state == null ? DiffSets.<Long>emptyDiffSets() : state.getNodeDiffSets();
    }

    public IndexRule addIndexRule( long labelId, long propertyKey )
    {
        SchemaIndexProvider.Descriptor providerDescriptor = providerMap.getDefaultProvider().getProviderDescriptor();
        IndexRule rule = new IndexRule( idGeneration.newSchemaRuleId(), labelId, providerDescriptor, propertyKey );

        persistenceManager.createSchemaRule( rule );

        ruleDiffSets.add( rule );
        LabelState labelState = getOrCreateLabelState( rule.getLabel() );
        labelState.getIndexRuleDiffSets().add( rule );

        return rule;
    }

    public void dropIndexRule( IndexRule rule )
    {
        ruleDiffSets.remove( rule );
        LabelState labelState = getOrCreateLabelState( rule.getLabel() );
        labelState.getIndexRuleDiffSets().remove( rule );

        persistenceManager.dropSchemaRule( rule.getId() );
    }

    public DiffSets<IndexRule> getIndexRuleDiffSetsByLabel( long labelId )
    {
        LabelState labelState = getState( labelStates, labelId, null );
        return labelState != null ? labelState.getIndexRuleDiffSets() : DiffSets.<IndexRule>emptyDiffSets();
    }

    public DiffSets<IndexRule> getIndexRuleDiffSets()
    {
        return ruleDiffSets;
    }

    public DiffSets<Long> getNodesWithChangedProperty( long propertyKeyId, Object value )
    {
        return legacyState.getNodesWithChangedProperty( propertyKeyId, value );
    }

    public DiffSets<Long> getDeletedNodes()
    {
        return nodes;
    }

    public boolean hasSchemaChanges()
    {
        return !getIndexRuleDiffSets().getRemoved().isEmpty();
    }

    private LabelState getOrCreateLabelState( long labelId )
    {
        return getState( labelStates, labelId, new StateCreator<LabelState>()
        {
            @Override
            public LabelState newState( long id )
            {
                return new LabelState( id );
            }
        } );
    }

    private NodeState getOrCreateNodeState( long nodeId )
    {
        return getState( nodeStates, nodeId, new StateCreator<NodeState>()
        {
            @Override
            public NodeState newState( long id )
            {
                return new NodeState( id );
            }
        } );
    }

    private static interface StateCreator<STATE>
    {
        STATE newState( long id );
    }

    private <STATE> STATE getState( Map<Long, STATE> states, long id, StateCreator<STATE> creator )
    {
        STATE result = states.get( id );
        if ( result != null )
        {
            return result;
        }

        if ( creator != null )
        {
            result = creator.newState( id );
            states.put( id, result );
        }
        return result;
    }
}
