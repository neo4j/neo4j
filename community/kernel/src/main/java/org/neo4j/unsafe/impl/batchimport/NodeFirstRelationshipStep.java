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
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipLink;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipLink.GroupVisitor;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

/**
 * Sets the {@link NodeRecord#setNextRel(long) relationship field} on all {@link NodeRecord nodes}.
 * This is done after all relationships have been imported and the {@link NodeRelationshipLink node cache}
 * points to the first relationship for each node.
 *
 * This step also creates {@link RelationshipGroupRecord group records} for the dense nodes as it encounters
 * dense nodes, where it gets all relationship group information from {@link NodeRelationshipLink}.
 */
public class NodeFirstRelationshipStep extends NodeStoreProcessorStep implements GroupVisitor
{
    private final RelationshipGroupStore relGroupStore;
    private final NodeRelationshipLink nodeRelationshipLink;

    private long nextGroupId = -1;

    public NodeFirstRelationshipStep( StageControl control, Configuration config,
                                      NodeStore nodeStore, RelationshipGroupStore relGroupStore,
                                      NodeRelationshipLink nodeRelationshipLink )
    {
        super( control, "LINKER", config, nodeStore );
        this.relGroupStore = relGroupStore;
        this.nodeRelationshipLink = nodeRelationshipLink;
    }

    @Override
    protected boolean process( NodeRecord node )
    {
        long nodeId = node.getId();
        long firstRel = nodeRelationshipLink.getFirstRel( nodeId, this );
        if ( firstRel == -1 )
        {
            return false;
        }

        node.setNextRel( firstRel );
        if ( nodeRelationshipLink.isDense( nodeId ) )
        {
            node.setDense( true );
        }
        return true;
    }

    @Override
    public long visit( long nodeId, int type, long next, long out, long in, long loop )
    {
        long id = nextGroupId != -1 ? nextGroupId : relGroupStore.nextId();
        nextGroupId = -1;

        RelationshipGroupRecord groupRecord = new RelationshipGroupRecord( id, type );
        groupRecord.setInUse( true );
        groupRecord.setFirstOut( out );
        groupRecord.setFirstIn( in );
        groupRecord.setFirstLoop( loop );
        groupRecord.setOwningNode( nodeId );
        if ( next != -1 )
        {
            groupRecord.setNext( nextGroupId = relGroupStore.nextId() );
        }
        relGroupStore.updateRecord( groupRecord );
        return id;
    }
}
