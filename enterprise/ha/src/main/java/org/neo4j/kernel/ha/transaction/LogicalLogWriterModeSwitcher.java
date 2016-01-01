/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.ha.transaction;

import java.net.URI;

import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.cluster.AbstractModeSwitcher;
import org.neo4j.kernel.ha.cluster.HighAvailability;
import org.neo4j.kernel.impl.nioneo.xa.command.LogWriterSupplier;

public class LogicalLogWriterModeSwitcher extends AbstractModeSwitcher<LogWriterSupplier>
{
    protected LogicalLogWriterModeSwitcher( HighAvailability highAvailability, DelegateInvocationHandler
            < LogWriterSupplier > delegate )
    {
        super( highAvailability, delegate );
    }

    @Override
    protected LogWriterSupplier getSlaveImpl( URI serverHaUri )
    {
        return null;
    }

    @Override
    protected LogWriterSupplier getMasterImpl()
    {
        return null;
    }
}
