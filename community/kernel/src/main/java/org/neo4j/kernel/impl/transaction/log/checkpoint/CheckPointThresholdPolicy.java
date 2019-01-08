/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import java.time.Clock;
import java.util.NoSuchElementException;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.logging.LogProvider;

/**
 * The {@link CheckPointThresholdPolicy} specifies the overall <em>type</em> of threshold that should be used for
 * deciding when to check point.
 *
 * The is determined by the {@link org.neo4j.graphdb.factory.GraphDatabaseSettings#check_point_policy} setting, and
 * based on this, the concrete policies are loaded and used to
 * {@link CheckPointThreshold#createThreshold(Config, Clock, LogPruning, LogProvider) create} the final and fully
 * configured check point thresholds.
 */
public abstract class CheckPointThresholdPolicy extends Service
{
    /**
     * Create a new instance of a service implementation identified with the
     * specified key(s).
     *
     * @param key the main key for identifying this service implementation
     * @param altKeys alternative spellings of the identifier of this service
     */
    protected CheckPointThresholdPolicy( String key, String... altKeys )
    {
        super( key, altKeys );
    }

    /**
     * Load the {@link CheckPointThresholdPolicy} by the given name.
     *
     * @throws NoSuchElementException if the policy was not found.
     */
    public static CheckPointThresholdPolicy loadPolicy( String policyName ) throws NoSuchElementException
    {
        return Service.load( CheckPointThresholdPolicy.class, policyName );
    }

    /**
     * Create a {@link CheckPointThreshold} instance based on this policy and the given configurations.
     */
    public abstract CheckPointThreshold createThreshold(
            Config config, Clock clock, LogPruning logPruning, LogProvider logProvider );
}
