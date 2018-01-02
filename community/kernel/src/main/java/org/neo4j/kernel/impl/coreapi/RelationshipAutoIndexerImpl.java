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
package org.neo4j.kernel.impl.coreapi;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.ReadableRelationshipIndex;
import org.neo4j.graphdb.index.RelationshipAutoIndexer;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.NodeManager;

public class RelationshipAutoIndexerImpl extends AbstractAutoIndexerImpl<Relationship>
        implements RelationshipAutoIndexer
{
    public static abstract class Configuration
    {
        public static final Setting<Boolean> relationship_auto_indexing = GraphDatabaseSettings
                .relationship_auto_indexing;
        public static final Setting<String> relationship_keys_indexable = GraphDatabaseSettings
                .relationship_keys_indexable;
    }

    static final String RELATIONSHIP_AUTO_INDEX = "relationship_auto_index";
    private Config config;
    private IndexProvider indexProvider;
    private NodeManager nodeManager;

    public RelationshipAutoIndexerImpl( Config config, IndexProvider indexProvider,
                                        NodeManager nodeManager )
    {
        super();
        this.config = config;
        this.indexProvider = indexProvider;
        this.nodeManager = nodeManager;
    }

    @Override
    public void init()
            throws Throwable
    {
    }

    @Override
    public void start()
    {
        setEnabled( config.get( Configuration.relationship_auto_indexing ) );
        propertyKeysToInclude.addAll( parseConfigList( config.get( Configuration.relationship_keys_indexable ) ) );
    }

    @Override
    public void stop()
            throws Throwable
    {
    }

    @Override
    public void shutdown()
            throws Throwable
    {
    }

    @Override
    protected RelationshipIndex getIndexInternal()
    {
        return indexProvider.getOrCreateRelationshipIndex(
                RELATIONSHIP_AUTO_INDEX, null );
    }

    @Override
    public ReadableRelationshipIndex getAutoIndex()
    {
        return getIndexInternal();
    }

    @Override
    public void setEnabled( boolean enabled )
    {
        super.setEnabled( enabled );
        if ( enabled )
        {
            nodeManager.addRelationshipPropertyTracker(
                    this );
        }
        else
        {
            nodeManager.removeRelationshipPropertyTracker(
                    this );
        }
    }

    static class RelationshipReadOnlyIndexToIndexAdapter extends
            ReadOnlyIndexToIndexAdapter<Relationship> implements
            RelationshipIndex
    {
        private final ReadableRelationshipIndex delegate;

        public RelationshipReadOnlyIndexToIndexAdapter(
                ReadableRelationshipIndex delegate )
        {
            super( delegate );
            this.delegate = delegate;
        }

        @Override
        public IndexHits<Relationship> get( String key, Object valueOrNull,
                                            Node startNodeOrNull, Node endNodeOrNull )
        {
            return delegate.get( key,
                    valueOrNull, startNodeOrNull,
                    endNodeOrNull );
        }

        @Override
        public IndexHits<Relationship> query( String key,
                                              Object queryOrQueryObjectOrNull, Node startNodeOrNull,
                                              Node endNodeOrNull )
        {
            return delegate.query( key,
                    queryOrQueryObjectOrNull,
                    startNodeOrNull, endNodeOrNull );
        }

        @Override
        public IndexHits<Relationship> query( Object queryOrQueryObjectOrNull,
                                              Node startNodeOrNull, Node endNodeOrNull )
        {
            return delegate.query(
                    queryOrQueryObjectOrNull, startNodeOrNull,
                    endNodeOrNull );
        }
    }
}
