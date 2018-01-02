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
package org.neo4j.management;

import org.neo4j.jmx.Description;
import org.neo4j.jmx.ManagementInterface;

@ManagementInterface( name = IndexSamplingManager.NAME )
@Description( "Handle index sampling." )
public interface IndexSamplingManager
{
    final String NAME = "Index sampler";

    @Description("Trigger index sampling for the index associated with the provided label and property key." +
            " If forceSample is set to true an index sampling will always happen otherwise a sampling is only " +
            "done if the number of updates exceeds the configured index_sampling_update_percentage.")
    void triggerIndexSampling( String labelKey, String propertyKey, boolean forceSample );
}
