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
package org.neo4j.harness;

import java.io.File;

import org.neo4j.harness.internal.EnterpriseInProcessServerBuilder;

/**
 * Factories for creating {@link TestServerBuilder} instances.
 */
public final class EnterpriseTestServerBuilders
{
    /**
     * Create a builder capable of starting an in-process Neo4j instance. This builder will use the standard java temp
     * directory (configured via the 'java.io.tmpdir' system property) as the location for the temporary Neo4j directory.
     */
    public static TestServerBuilder newInProcessBuilder()
    {
        return new EnterpriseInProcessServerBuilder();
    }

    /**
     * Create a builder capable of starting an in-process Neo4j instance, running in a subdirectory of the specified directory.
     */
    public static TestServerBuilder newInProcessBuilder( File workingDirectory )
    {
        return new EnterpriseInProcessServerBuilder( workingDirectory );
    }

    private EnterpriseTestServerBuilders(){}
}
