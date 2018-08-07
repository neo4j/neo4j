/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.management.impl;

import javax.management.NotCompliantMBeanException;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.Service;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.jmx.impl.ManagementBeanProvider;
import org.neo4j.jmx.impl.ManagementData;
import org.neo4j.jmx.impl.Neo4jMBean;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.management.IndexSamplingManager;
import org.neo4j.storageengine.api.StorageEngine;

import static org.neo4j.internal.kernel.api.TokenRead.NO_TOKEN;

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
    protected Neo4jMBean createMXBean( ManagementData management )
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
        management.getKernelData().getDataSourceManager().addListener( access );
        return access;
    }

    static class StoreAccess implements DataSourceManager.Listener
    {
        private static class State
        {
            final StorageEngine storageEngine;
            final IndexingService indexingService;
            final TokenHolders tokenHolders;

            State( StorageEngine storageEngine, IndexingService indexingService, TokenHolders tokenHolders )
            {
                this.storageEngine = storageEngine;
                this.indexingService = indexingService;
                this.tokenHolders = tokenHolders;
            }
        }
        private volatile State state;

        @Override
        public void registered( NeoStoreDataSource dataSource )
        {
            DependencyResolver dependencyResolver = dataSource.getDependencyResolver();
            state = new State(
                    dependencyResolver.resolveDependency( StorageEngine.class ),
                    dependencyResolver.resolveDependency( IndexingService.class ),
                    dependencyResolver.resolveDependency( TokenHolders.class ) );
        }

        @Override
        public void unregistered( NeoStoreDataSource dataSource )
        {
            state = null;
        }

        public void triggerIndexSampling( String labelKey, String propertyKey, boolean forceSample )
        {
            int labelKeyId = NO_TOKEN;
            int propertyKeyId = NO_TOKEN;
            State state = this.state;
            if ( state != null )
            {
                labelKeyId = state.tokenHolders.labelTokens().getIdByName( labelKey );
                propertyKeyId = state.tokenHolders.propertyKeyTokens().getIdByName( propertyKey );
            }
            if ( state == null || labelKeyId == NO_TOKEN || propertyKeyId == NO_TOKEN )
            {
                throw new IllegalArgumentException( "No property or label key was found associated with " +
                        propertyKey + " and " + labelKey );
            }
            try
            {
                state.indexingService.triggerIndexSampling(
                        SchemaDescriptorFactory.forLabel( labelKeyId, propertyKeyId ),
                        getIndexSamplingMode( forceSample ) );
            }
            catch ( IndexNotFoundKernelException e )
            {
                throw new IllegalArgumentException( e.getMessage() );
            }
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
