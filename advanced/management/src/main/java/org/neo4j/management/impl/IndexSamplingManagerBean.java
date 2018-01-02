/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.management.impl;

import javax.management.NotCompliantMBeanException;

import org.neo4j.helpers.Service;
import org.neo4j.jmx.impl.ManagementBeanProvider;
import org.neo4j.jmx.impl.ManagementData;
import org.neo4j.jmx.impl.Neo4jMBean;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.management.IndexSamplingManager;

@Service.Implementation( ManagementBeanProvider.class )
public final class IndexSamplingManagerBean extends ManagementBeanProvider
{
    public IndexSamplingManagerBean()
    {
        super( IndexSamplingManager.class );
    }

    @Override
    protected Neo4jMBean createMBean( ManagementData management ) throws NotCompliantMBeanException
    {
        return new IndexSamplingManagerImpl( management );
    }

    @Override
    protected Neo4jMBean createMXBean( ManagementData management ) throws NotCompliantMBeanException
    {
        return new IndexSamplingManagerImpl( management, true );
    }

    private static class IndexSamplingManagerImpl extends Neo4jMBean implements IndexSamplingManager
    {
        private final StoreAccess access;

        IndexSamplingManagerImpl( ManagementData management ) throws NotCompliantMBeanException
        {
            super( management );
            this.access = access( management );
        }

        IndexSamplingManagerImpl( ManagementData management, boolean mxBean )
        {
            super( management, mxBean );
            this.access = access( management );
        }

        @Override
        public void triggerIndexSampling( String labelKey, String propertyKey, boolean forceSample )
        {
            access.triggerIndexSampling( labelKey, propertyKey, forceSample );
        }
    }

    private static StoreAccess access( ManagementData management )
    {
        StoreAccess access = new StoreAccess();
        management.getKernelData().graphDatabase().getDependencyResolver().resolveDependency( DataSourceManager.class )
                .addListener( access );
        return access;
    }

    static class StoreAccess implements DataSourceManager.Listener
    {
        private static class State
        {
            final StoreReadLayer storeLayer;
            final IndexingService indexingService;

            State( StoreReadLayer storeLayer, IndexingService indexingService )
            {
                this.storeLayer = storeLayer;
                this.indexingService = indexingService;
            }
        }
        private volatile State state;

        @Override
        public void registered( NeoStoreDataSource dataSource )
        {
            state = new State( dataSource.getStoreLayer(), dataSource.getIndexService());
        }

        @Override
        public void unregistered( NeoStoreDataSource dataSource )
        {
            state = null;
        }

        public void triggerIndexSampling( String labelKey, String propertyKey, boolean forceSample )
        {
            int labelKeyId = -1, propertyKeyId = -1;
            State state = this.state;
            if ( state != null )
            {
                labelKeyId = state.storeLayer.labelGetForName( labelKey );
                propertyKeyId = state.storeLayer.propertyKeyGetForName( propertyKey );
            }
            if ( labelKeyId == -1 || propertyKeyId == -1 )
            {
                throw new IllegalArgumentException( "No property or label key was found associated with " +
                        propertyKey + " and " + labelKey );
            }
            state.indexingService.triggerIndexSampling( new IndexDescriptor( labelKeyId, propertyKeyId ),
                    getIndexSamplingMode( forceSample ) );
        }

        private IndexSamplingMode getIndexSamplingMode( boolean forceSample )
        {
            if ( forceSample )
            {
                return IndexSamplingMode.TRIGGER_REBUILD_ALL;
            }
            else
            {
                return IndexSamplingMode.TRIGGER_REBUILD_UPDATED;
            }
        }
    }
}
