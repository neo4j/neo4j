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

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

import org.neo4j.impl.transaction.xaframework.XaDataSource;

/**
 * Class that wraps a XA data source.
 */
public abstract class AbstractResource
{
    final XaDataSource xaDs;

    AbstractResource( final XaDataSource xaDataSource )
    {
        this.xaDs = xaDataSource;
    }

    abstract public void close();

    public void applyLog( final ReadableByteChannel log ) throws IOException
    {
        xaDs.applyLog( log );
    }

    public long getCreationTime()
    {
        return xaDs.getCreationTime();
    }

    public long getIdentifier()
    {
        return xaDs.getCreationTime();
    }

    public String getName()
    {
        return xaDs.getName();
    }

    public boolean hasLogicalLog( final long version )
    {
        return xaDs.hasLogicalLog( version );
    }

    public ReadableByteChannel getLogicalLog( final long version )
        throws IOException
    {
        return xaDs.getLogicalLog( version );
    }

    public long getVersion()
    {
        return xaDs.getCurrentLogVersion();
    }

    public void rotateLog() throws IOException
    {
        xaDs.rotateLogicalLog();
    }

    public void makeBackupSlave()
    {
        xaDs.makeBackupSlave();
    }
}