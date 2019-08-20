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

import java.util.NoSuchElementException;

import org.neo4j.annotations.service.Service;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.logging.LogProvider;
import org.neo4j.service.NamedService;
import org.neo4j.service.Services;
import org.neo4j.time.SystemNanoClock;

/**
 * The {@link CheckPointThresholdPolicy} specifies the overall <em>type</em> of threshold that should be used for
 * deciding when to check point.
 *
 * The is determined by the {@link GraphDatabaseSettings#check_point_policy} setting, and
 * based on this, the concrete policies are loaded and used to
 * {@link CheckPointThreshold#createThreshold(Config, SystemNanoClock, LogPruning, LogProvider) create} the final and fully
 * configured check point thresholds.
 */
@Service
public interface CheckPointThresholdPolicy extends NamedService
{
    /**
     * Load the {@link CheckPointThresholdPolicy} by the given name.
     *
     * @throws NoSuchElementException if the policy was not found.
     */
    static CheckPointThresholdPolicy loadPolicy( String policyName ) throws NoSuchElementException
    {
        return Services.loadOrFail( CheckPointThresholdPolicy.class, policyName );
    }

    /**
     * Create a {@link CheckPointThreshold} instance based on this policy and the given configurations.
     */
    CheckPointThreshold createThreshold( Config config, SystemNanoClock clock, LogPruning logPruning, LogProvider logProvider );
}
