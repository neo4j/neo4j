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
package org.neo4j.kernel.impl.locking;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Internal;
import org.neo4j.kernel.configuration.Settings;

import static java.util.Objects.requireNonNull;
import static org.neo4j.kernel.configuration.Settings.setting;

/**
 * A {@link StatementLocksFactory} that created {@link DeferringStatementLocks} based on the given
 * {@link Locks} and {@link Config}.
 */
public class DeferringStatementLocksFactory implements StatementLocksFactory
{
    @Internal
    @Description( "Enable deferring of locks to commit time. This feature weakens the isolation level. " +
                  "It can result in both domain and storage level inconsistencies." )
    public static final Setting<Boolean> deferred_locks_enabled =
            setting( "unsupported.dbms.deferred_locks.enabled", Settings.BOOLEAN, Settings.FALSE );

    private Locks locks;
    private boolean deferredLocksEnabled;

    @Override
    public void initialize( Locks locks, Config config )
    {
        this.locks = requireNonNull( locks );
        this.deferredLocksEnabled = config.get( deferred_locks_enabled );
    }

    @Override
    public StatementLocks newInstance()
    {
        if ( locks == null )
        {
            throw new IllegalStateException( "Factory has not been initialized" );
        }

        Locks.Client client = locks.newClient();
        return deferredLocksEnabled ? new DeferringStatementLocks( client ) : new SimpleStatementLocks( client );
    }
}
