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
package org.neo4j.causalclustering.catchup;

import java.util.function.Supplier;

import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.util.Dependencies;

public class CheckpointerSupplier implements Supplier<CheckPointer>
{
    private final Dependencies dependencies;

    public CheckpointerSupplier( Dependencies dependencies )
    {
        this.dependencies = dependencies;
    }

    @Override
    public CheckPointer get()
    {
        return dependencies.resolveDependency( CheckPointer.class );
    }
}
