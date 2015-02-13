/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.xa;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.api.index.IndexUpdates;
import org.neo4j.kernel.impl.api.index.UpdateMode;
import org.neo4j.kernel.impl.core.IteratingPropertyReceiver;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.xa.Command.Mode;
import org.neo4j.kernel.impl.nioneo.xa.Command.NodeCommand;
import org.neo4j.kernel.impl.nioneo.xa.Command.PropertyCommand;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreTransaction.LabelChangeSummary;

import static org.neo4j.kernel.api.index.NodePropertyUpdate.add;
import static org.neo4j.kernel.api.index.NodePropertyUpdate.remove;
import static org.neo4j.kernel.impl.nioneo.store.labels.NodeLabelsField.parseLabelsField;

class LazyIndexUpdates implements IndexUpdates
{
    private final NodeStore nodeStore;
    private final PropertyStore propertyStore;
    private final Map<Long,List<PropertyCommand>> propCommands;
    private final Map<Long, NodeCommand> nodeCommands;
    private Collection<NodePropertyUpdate> updates;

    public LazyIndexUpdates( NodeStore nodeStore, PropertyStore propertyStore,
                             Map<Long,List<PropertyCommand>> propCommands, Map<Long, NodeCommand> nodeCommands )
    {
        this.nodeStore = nodeStore;
        this.propertyStore = propertyStore;
        this.propCommands = propCommands;
        this.nodeCommands = nodeCommands;
    }

    @Override
    public Iterator<NodePropertyUpdate> iterator()
    {
        if ( updates == null )
        {
            updates = gatherPropertyAndLabelUpdates();
        }
        return updates.iterator();
    }

    @Override
    public Set<Long> changedNodeIds()
    {
        Set<Long> nodeIds = new HashSet<>( nodeCommands.keySet() );
        for ( List<PropertyCommand> propCmd : propCommands.values() )
        {
            PropertyRecord record = propCmd.get( 0 ).getAfter();
            if ( record.isNodeSet() )
            {
                nodeIds.add( record.getNodeId() );
            }
        }
        return nodeIds;
    }

    private Collection<NodePropertyUpdate> gatherPropertyAndLabelUpdates()
    {
        Collection<NodePropertyUpdate> propertyUpdates = new HashSet<>();
        Map<Pair<Long, Integer>, NodePropertyUpdate> propertyChanges = new HashMap<>();
        gatherUpdatesFromPropertyCommands( propertyUpdates, propertyChanges );
        gatherUpdatesFromNodeCommands( propertyUpdates, propertyChanges );
        return propertyUpdates;
    }

    private void gatherUpdatesFromPropertyCommands( Collection<NodePropertyUpdate> updates,
                                                    Map<Pair<Long, Integer>, NodePropertyUpdate> propertyLookup )
    {
        for ( List<PropertyCommand> propertyCommands : propCommands.values() )
        {
            // Let after state of first command here be representative of the whole group
            PropertyRecord representative = propertyCommands.get( 0 ).getAfter();
            if ( !representative.isNodeSet() )
            {   // These changes wasn't for a node, skip them
                continue;
            }

            long nodeId = representative.getNodeId();
            long[] nodeLabelsBefore, nodeLabelsAfter;
            NodeCommand nodeChanges = nodeCommands.get( nodeId );
            if ( nodeChanges != null )
            {
                nodeLabelsBefore = parseLabelsField( nodeChanges.getBefore() ).get( nodeStore );
                nodeLabelsAfter = parseLabelsField( nodeChanges.getAfter() ).get( nodeStore );
            }
            else
            {
                /* If the node doesn't exist here then we've most likely encountered this scenario:
                 * - TX1: Node N exists and has property record P
                 * - rotate log
                 * - TX2: P gets changed
                 * - TX3: N gets deleted (also P, but that's irrelevant for this scenario)
                 * - N is persisted to disk for some reason
                 * - crash
                 * - recover
                 * - TX2: P has changed and updates to indexes are gathered. As part of that it tries to read
                 *        the labels of N (which does not exist a.t.m.).
                 *
                 * We can actually (if we disregard any potential inconsistencies) just assume that
                 * if this happens and we're in recovery mode that the node in question will be deleted
                 * in an upcoming transaction, so just skip this update.
                 */
                NodeRecord nodeRecord = nodeStore.getRecord( nodeId );
                nodeLabelsBefore = nodeLabelsAfter = parseLabelsField( nodeRecord ).get( nodeStore );
            }

            propertyStore.toLogicalUpdates( updates,
                    Iterables.<PropertyRecordChange,PropertyCommand>cast( propertyCommands ),
                    nodeLabelsBefore, nodeLabelsAfter );
        }

        for ( NodePropertyUpdate update : updates )
        {
            if ( update.getUpdateMode() == UpdateMode.CHANGED )
            {
                propertyLookup.put( Pair.of( update.getNodeId(), update.getPropertyKeyId() ), update );
            }
        }
    }

    private void gatherUpdatesFromNodeCommands( Collection<NodePropertyUpdate> propertyUpdates,
                                                Map<Pair<Long, Integer>, NodePropertyUpdate> propertyLookup )
    {
        for ( NodeCommand nodeCommand : nodeCommands.values() )
        {
            long nodeId = nodeCommand.getKey();
            long[] labelsBefore = parseLabelsField( nodeCommand.getBefore() ).get( nodeStore );
            long[] labelsAfter = parseLabelsField( nodeCommand.getAfter() ).get( nodeStore );

            if ( nodeCommand.getMode() != Mode.UPDATE )
            {
                // For created and deleted nodes rely on the updates from the perspective of properties to cover it all
                // otherwise we'll get duplicate update during recovery, or cannot load properties if deleted.
                continue;
            }

            LabelChangeSummary summary = new LabelChangeSummary( labelsBefore, labelsAfter );
            if ( !summary.hasAddedLabels() && !summary.hasRemovedLabels() )
            {
                continue;
            }

            Iterator<DefinedProperty> properties = nodeFullyLoadProperties( nodeId );
            while ( properties.hasNext() )
            {
                DefinedProperty property = properties.next();
                int propertyKeyId = property.propertyKeyId();
                if ( summary.hasAddedLabels() )
                {
                    Object value = property.value();
                    propertyUpdates.add( add( nodeId, propertyKeyId, value, summary.getAddedLabels() ) );
                }
                if ( summary.hasRemovedLabels() )
                {
                    NodePropertyUpdate propertyChange = propertyLookup.get( Pair.of( nodeId, propertyKeyId ) );
                    Object value = propertyChange == null ? property.value() : propertyChange.getValueBefore();
                    propertyUpdates.add( remove( nodeId, propertyKeyId, value, summary.getRemovedLabels() ) );
                }
            }
        }
    }

    private Iterator<DefinedProperty> nodeFullyLoadProperties( long nodeId )
    {
        IteratingPropertyReceiver receiver = new IteratingPropertyReceiver();
        Map<Long,PropertyRecord> propertiesById = propertiesFromCommandsForNode( nodeId );
        NeoStoreTransaction.loadProperties(
                propertyStore, nodeCommands.get( nodeId ).getAfter().getNextProp(), receiver, propertiesById );
        return receiver;
    }

    private Map<Long,PropertyRecord> propertiesFromCommandsForNode( long nodeId )
    {
        List<PropertyCommand> propertyCommands = propCommands.get( nodeId );
        if ( propertyCommands == null )
        {
            return Collections.emptyMap();
        }
        Map<Long,PropertyRecord> result = new HashMap<>( propertyCommands.size(), 1 );
        for ( PropertyCommand command : propertyCommands )
        {
            PropertyRecord after = command.getAfter();
            if ( after.inUse() && after.isNodeSet() )
            {
                result.put( after.getId(), after );
            }
        }
        return result;
    }
}
