/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.ArrayList;
import java.util.List;

import javax.management.NotCompliantMBeanException;

import org.neo4j.helpers.Service;
import org.neo4j.jmx.impl.ManagementBeanProvider;
import org.neo4j.jmx.impl.ManagementData;
import org.neo4j.jmx.impl.Neo4jMBean;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.management.XaManager;
import org.neo4j.management.XaResourceInfo;

@Service.Implementation( ManagementBeanProvider.class )
public final class XaManagerBean extends ManagementBeanProvider
{
    public XaManagerBean()
    {
        super( XaManager.class );
    }

    @Override
    protected Neo4jMBean createMBean( ManagementData management ) throws NotCompliantMBeanException
    {
        return new XaManagerImpl( management );
    }

    @Override
    protected Neo4jMBean createMXBean( ManagementData management ) throws NotCompliantMBeanException
    {
        return new XaManagerImpl( management, true );
    }

    private static class XaManagerImpl extends Neo4jMBean implements XaManager
    {
        private final XaDataSourceManager datasourceMananger;

        XaManagerImpl( ManagementData management ) throws NotCompliantMBeanException
        {
            super( management );
            this.datasourceMananger = xaManager( management );
        }

        XaManagerImpl( ManagementData management, boolean isMxBean )
        {
            super( management, isMxBean );
            this.datasourceMananger = xaManager( management );
        }

        public XaResourceInfo[] getXaResources()
        {
            return getXaResourcesImpl( datasourceMananger );
        }

        private static XaResourceInfo[] getXaResourcesImpl( XaDataSourceManager datasourceMananger )
        {
            List<XaResourceInfo> result = new ArrayList<XaResourceInfo>();
            for ( XaDataSource datasource : datasourceMananger.getAllRegisteredDataSources() )
            {
                result.add( createXaResourceInfo( datasource ) );
            }
            return result.toArray( new XaResourceInfo[result.size()] );
        }

        private static XaResourceInfo createXaResourceInfo( XaDataSource datasource )
        {
            return new XaResourceInfo( datasource.getName(), toHexString( datasource.getBranchId() ),
                datasource.getLastCommittedTxId(), datasource.getCurrentLogVersion() );
        }

        private XaDataSourceManager xaManager( ManagementData management )
        {
            return management.getKernelData().graphDatabase().getDependencyResolver().resolveDependency( XaDataSourceManager.class );
        }
    }

    public static String toHexString( byte[] branchId )
    {
        StringBuilder result = new StringBuilder();
        for ( byte part : branchId )
        {
            String chunk = Integer.toHexString( part );
            if ( chunk.length() < 2 ) result.append( "0" );
            if ( chunk.length() > 2 )
                result.append( chunk.substring( chunk.length() - 2 ) );
            else
                result.append( chunk );
        }
        return result.toString();
    }
}
