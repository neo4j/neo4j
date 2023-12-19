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
package org.neo4j.kernel.impl.api;

import org.neo4j.kernel.impl.enterprise.lock.forseti.ForsetiLocksFactory;
import org.neo4j.test.rule.DatabaseRule;

import static org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory.Configuration.lock_manager;

public class KernelTransactionTimeoutMonitorWithForsetiIT extends KernelTransactionTimeoutMonitorIT
{
    @Override
    protected DatabaseRule createDatabaseRule()
    {
        return super.createDatabaseRule().withSetting( lock_manager, ForsetiLocksFactory.KEY );
    }
}
