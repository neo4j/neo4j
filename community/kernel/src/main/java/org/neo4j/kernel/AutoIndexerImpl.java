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
package org.neo4j.kernel;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;

class AutoIndexerImpl implements TransactionEventHandler<Void>, AutoIndexer
{
    private final Set<String> nodePropertyKeys = new HashSet<String>();

    private final EmbeddedGraphDbImpl gdb;

    public AutoIndexerImpl( EmbeddedGraphDbImpl gdb )
    {
        this.gdb = gdb;
    }

    public Void beforeCommit( TransactionData data ) throws Exception
    {
        handleNodeProperties( data.removedNodeProperties(), data.assignedNodeProperties() );
        return null;
    }

    private <T extends PropertyContainer> void handleNodeProperties(
            Iterable<PropertyEntry<Node>> removed, Iterable<PropertyEntry<Node>> assigned )
    {
        final Index<Node> nodeIndex = getNodeIndex();
        for ( PropertyEntry<Node> entry : assigned )
        {
            // will fix thread saftey later
            if ( nodePropertyKeys.contains( entry.key()) )
            {
                Object previousValue = entry.previouslyCommitedValue();
                String key = entry.key();
                if ( previousValue != null )
                {
                    nodeIndex.remove( entry.entity(), key, previousValue );
                }
                nodeIndex.add( entry.entity(), key, entry.value() );
            }
        }
        for ( PropertyEntry<Node> entry : removed )
        {
            // will fix thread saftey later
            if ( nodePropertyKeys.contains( entry.key()) )
            {
                Object previouslyCommitedValue = entry.previouslyCommitedValue();
                if ( previouslyCommitedValue != null )
                {
                    nodeIndex.remove( entry.entity(), entry.key(), previouslyCommitedValue );
                }
            }
        }
    }

    public IndexHits<Node> getNodesFor( String key, Object value )
    {
        return getNodeIndex().get( key, value );
    }

    public Index<Node> getNodeIndex()
    {
        return gdb.index().forNodes( "node_auto_index" );
    }

    public void afterCommit( TransactionData data, Void state )
    {
    }

    public void afterRollback( TransactionData data, Void state )
    {
    }

    public synchronized void addAutoIndexingForNodeProperty( String key )
    {
        if ( nodePropertyKeys.isEmpty() )
        {
            // TODO: add check for index provider, if non exist throw exception
            gdb.registerTransactionEventHandler( this );
        }
        nodePropertyKeys.add( key );
    }

    public synchronized void removeAutoIndexingForNodeProperty( String key )
    {
        if ( nodePropertyKeys.remove( key ) && nodePropertyKeys.isEmpty() )
        {
            gdb.unregisterTransactionEventHandler( this );
        }
    }

    @Override
    public void addAutoIndexingForRelationshipProperty( String key )
    {
        // TODO Auto-generated method stub

    }

    @Override
    public Index<Relationship> getRelationshipIndex()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IndexHits<Relationship> getRelationshipsFor( String key, Object value )
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void removeAutIndexingForRelationshipProperty( String key )
    {
        // TODO Auto-generated method stub

    }
}