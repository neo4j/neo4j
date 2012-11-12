/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.ReadableIndex;
import org.neo4j.graphdb.index.ReadableRelationshipIndex;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.tooling.wrap.WrappedGraphDatabase;
import org.neo4j.tooling.wrap.WrappedIndex;
import org.neo4j.tooling.wrap.WrappedNode;
import org.neo4j.tooling.wrap.WrappedRelationship;

import java.util.Map;

public class HighlyAvailableGraphDatabase extends WrappedGraphDatabase
{
    public static final String CONFIG_KEY_OLD_SERVER_ID = HaConfig.CONFIG_KEY_OLD_SERVER_ID;
    public static final String CONFIG_KEY_SERVER_ID = HaConfig.CONFIG_KEY_SERVER_ID;

    public static final String CONFIG_KEY_OLD_COORDINATORS = HaConfig.CONFIG_KEY_OLD_COORDINATORS;
    public static final String CONFIG_KEY_COORDINATORS = HaConfig.CONFIG_KEY_COORDINATORS;

    public static final String CONFIG_KEY_SERVER = HaConfig.CONFIG_KEY_SERVER;
    public static final String CONFIG_KEY_CLUSTER_NAME = HaConfig.CONFIG_KEY_CLUSTER_NAME;
    public static final String CONFIG_KEY_PULL_INTERVAL = HaConfig.CONFIG_KEY_PULL_INTERVAL;
    public static final String CONFIG_KEY_ALLOW_INIT_CLUSTER = HaConfig.CONFIG_KEY_ALLOW_INIT_CLUSTER;
    public static final String CONFIG_KEY_MAX_CONCURRENT_CHANNELS_PER_SLAVE = HaConfig.CONFIG_KEY_MAX_CONCURRENT_CHANNELS_PER_SLAVE;
    public static final String CONFIG_KEY_BRANCHED_DATA_POLICY = HaConfig.CONFIG_KEY_BRANCHED_DATA_POLICY;
    public static final String CONFIG_KEY_READ_TIMEOUT = HaConfig.CONFIG_KEY_READ_TIMEOUT;
    public static final String CONFIG_KEY_LOCK_READ_TIMEOUT = HaConfig.CONFIG_KEY_LOCK_READ_TIMEOUT;
    public static final String CONFIG_KEY_SLAVE_COORDINATOR_UPDATE_MODE = HaConfig.CONFIG_KEY_SLAVE_COORDINATOR_UPDATE_MODE;

    public HighlyAvailableGraphDatabase( String storeDir, Map<String, String> config )
    {
        this( new HAGraphDb( storeDir, config ) );
    }

    public HighlyAvailableGraphDatabase( HAGraphDb graphdb )
    {
        super( graphdb );
    }

    HighlyAvailableGraphDatabase( AbstractGraphDatabase graphdb )
    {
        super( graphdb );
    }

    public static Map<String,String> loadConfigurations( String file )
    {
        return EmbeddedGraphDatabase.loadConfigurations( file );
    }

    public void pullUpdates()
    {
        ( (HAGraphDb) graphdb ).pullUpdates();
    }

    public boolean isMaster()
    {
        return ( (HAGraphDb) graphdb ).isMaster();
    }

    public HAGraphDb getRawHaDb()
    {
        return (HAGraphDb) graphdb;
    }

    @Override
    protected WrappedNode<? extends WrappedGraphDatabase> node( Node node, boolean created )
    {
        if ( node == null ) return null;
        return new LookupNode( this, node.getId() );
    }

    @Override
    protected WrappedRelationship<? extends WrappedGraphDatabase> relationship( Relationship relationship,
            boolean created )
    {
        if ( relationship == null ) return null;
        return new LookupRelationship( this, relationship.getId() );
    }

    @Override
    protected WrappedIndex.WrappedNodeIndex nodeIndex( Index<Node> index, final String indexName )
    {
        return new WrappedIndex.WrappedNodeIndex( this )
        {
            @Override
            protected ReadableIndex<Node> actual()
            {
                return HighlyAvailableGraphDatabase.this.graphdb.index().forNodes( indexName );
            }
        };
    }

    @Override
    protected WrappedIndex.WrappedRelationshipIndex relationshipIndex(
            RelationshipIndex index, final String indexName )
    {
        return new WrappedIndex.WrappedRelationshipIndex( this )
        {
            @Override
            protected ReadableRelationshipIndex actual()
            {
                return HighlyAvailableGraphDatabase.this.graphdb.index().forRelationships( indexName );
            }
        };
    }

    @Override
    protected WrappedIndex.WrappedNodeIndex automaticNodeIndex( ReadableIndex<Node> index )
    {
        return new WrappedIndex.WrappedNodeIndex( this )
        {
            @Override
            protected ReadableIndex<Node> actual()
            {
                return HighlyAvailableGraphDatabase.this.graphdb.index().getNodeAutoIndexer().getAutoIndex();
            }
        };
    }

    @Override
    protected WrappedIndex.WrappedRelationshipIndex automaticRelationshipIndex(
            ReadableRelationshipIndex index )
    {
        return new WrappedIndex.WrappedRelationshipIndex( this )
        {
            @Override
            protected ReadableRelationshipIndex actual()
            {
                return HighlyAvailableGraphDatabase.this.graphdb.index().getRelationshipAutoIndexer().getAutoIndex();
            }
        };
    }

    private static class LookupNode extends WrappedNode<HighlyAvailableGraphDatabase>
    {
        private final long id;

        LookupNode( HighlyAvailableGraphDatabase graphdb, long id )
        {
            super( graphdb );
            this.id = id;
        }

        @Override
        protected Node actual()
        {
            return graphdb.graphdb.getNodeById( id );
        }

        @Override
        public long getId()
        {
            return id;
        }

        @Override
        public int hashCode()
        {
            return (int) ( ( id >>> 32 ) ^ id );
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( this == obj ) return true;
            if ( obj instanceof LookupNode )
            {
                LookupNode that = (LookupNode) obj;
                return this.id == that.id && this.graphdb == that.graphdb;
            }
            return false;
        }
    }

    private static class LookupRelationship extends WrappedRelationship<HighlyAvailableGraphDatabase>
    {
        private final long id;

        LookupRelationship( HighlyAvailableGraphDatabase graphdb, long id )
        {
            super( graphdb );
            this.id = id;
        }

        @Override
        protected Relationship actual()
        {
            return graphdb.graphdb.getRelationshipById( id );
        }

        @Override
        public long getId()
        {
            return id;
        }

        @Override
        public int hashCode()
        {
            return (int) ( ( id >>> 32 ) ^ id );
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( this == obj ) return true;
            if ( obj instanceof LookupRelationship )
            {
                LookupRelationship that = (LookupRelationship) obj;
                return this.id == that.id && this.graphdb == that.graphdb;
            }
            return false;
        }
    }
}
