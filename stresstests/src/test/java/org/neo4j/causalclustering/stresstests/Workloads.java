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
package org.neo4j.causalclustering.stresstests;

import org.neo4j.helper.Workload;

enum Workloads
{
    CreateNodesWithProperties
            {
                @Override
                Workload create( Control control, Resources resources, Config config )
                {
                    return new CreateNodesWithProperties( control, resources, config );
                }
            },
    StartStopRandomMember
            {
                @Override
                Workload create( Control control, Resources resources, Config config )
                {
                    return new StartStopRandomMember( control, resources );
                }
            },
    StartStopRandomCore
            {
                @Override
                Workload create( Control control, Resources resources, Config config )
                {
                    return new StartStopRandomCore( control, resources );
                }
            },
    BackupRandomMember
            {
                @Override
                Workload create( Control control, Resources resources, Config config )
                {
                    return new BackupRandomMember( control, resources );
                }
            },
    CatchupNewReadReplica
            {
                @Override
                Workload create( Control control, Resources resources, Config config )
                {
                    return new CatchupNewReadReplica( control, resources );
                }
            },
    ReplaceRandomMember
            {
                @Override
                Workload create( Control control, Resources resources, Config config )
                {
                    return new ReplaceRandomMember( control, resources );
                }
            },
    IdReuseInsertion
            {
                @Override
                Workload create( Control control, Resources resources, Config config )
                {
                    return new IdReuse.InsertionWorkload( control, resources );
                }
            },
    IdReuseDeletion
            {
                @Override
                Workload create( Control control, Resources resources, Config config )
                {
                    return new IdReuse.DeletionWorkload( control, resources );
                }
            },
    IdReuseReelection
            {
                @Override
                Workload create( Control control, Resources resources, Config config )
                {
                    return new IdReuse.ReelectionWorkload( control, resources, config );
                }
            };

    abstract Workload create( Control control, Resources resources, Config config );
}
