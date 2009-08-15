/*
 * Copyright (c) 2002-2009 "Neo Technology," Network Engine for Objects in Lund
 * AB [http://neotechnology.com] This file is part of Neo4j. Neo4j is free
 * software: you can redistribute it and/or modify it under the terms of the GNU
 * Affero General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version. This
 * program is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details. You should have received a copy of the GNU Affero General Public
 * License along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.onlinebackup;

import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.impl.transaction.XaDataSourceManager;
import org.neo4j.impl.transaction.xaframework.XaDataSource;

/**
 * Wraps an EmbeddedNeo to be used as data source and give access to other data
 * sources as well.
 */
public class EmbeddedNeoResource extends AbstractResource implements
    NeoResource
{
    protected final EmbeddedNeo neo;
    protected final XaDataSourceManager xaDsm;

    EmbeddedNeoResource( final EmbeddedNeo neo )
    {
        super( neo.getConfig().getPersistenceModule().getPersistenceManager()
            .getPersistenceSource().getXaDataSource() );
        this.neo = neo;
        this.xaDsm = neo.getConfig().getTxModule().getXaDataSourceManager();
    }

    public XaDataSourceResource getDataSource( final String name )
    {
        XaDataSource ds = xaDsm.getXaDataSource( name );
        if ( ds == null )
        {
            return null;
        }
        return new XaDataSourceResource( ds );
    }

    public XaDataSourceResource getDataSource()
    {
        XaDataSource ds = neo.getConfig().getPersistenceModule()
            .getPersistenceManager().getPersistenceSource().getXaDataSource();
        if ( ds == null )
        {
            return null;
        }
        return new XaDataSourceResource( ds );
    }

    @Override
    public void close()
    {
        neo.shutdown();
    }
}
