/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.onlinebackup.impl;

import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

/**
 * Wraps an {@link AbstractGraphDatabase} to be used as data source and give
 * access to other data sources as well.
 */
public class AbstractGraphDatabaseResource extends AbstractResource
    implements Neo4jResource
{
    protected final AbstractGraphDatabase graphDb;
    protected final XaDataSourceManager xaDsm;

    public AbstractGraphDatabaseResource( final AbstractGraphDatabase graphDb )
    {
        super( graphDb.getConfig().getPersistenceModule()
            .getPersistenceManager().getPersistenceSource().getXaDataSource() );
        this.graphDb = graphDb;
        this.xaDsm = graphDb.getConfig().getTxModule().getXaDataSourceManager();
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
        XaDataSource ds = graphDb.getConfig().getPersistenceModule()
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
        graphDb.shutdown();
    }
}
