/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.management;

import org.neo4j.jmx.Description;
import org.neo4j.jmx.ManagementInterface;

@ManagementInterface( name = IndexSamplingManager.NAME )
@Description( "Handle index sampling." )
public interface IndexSamplingManager
{
    String NAME = "Index sampler";

    @Description( "Trigger index sampling for the index associated with the provided label and property key." +
            " If forceSample is set to true an index sampling will always happen otherwise a sampling is only " +
            "done if the number of updates exceeds the configured dbms.index_sampling.update_percentage." )
    void triggerIndexSampling( String labelKey, String propertyKey, boolean forceSample );
}
