/*
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
package org.neo4j.causalclustering.stresstests;

import java.util.concurrent.Callable;
import java.util.function.BooleanSupplier;

abstract class RepeatUntilCallable implements Callable<Throwable>
{
    private BooleanSupplier keepGoing;
    private Runnable onFailure;

    RepeatUntilCallable( BooleanSupplier keepGoing, Runnable onFailure )
    {
        this.keepGoing = keepGoing;
        this.onFailure = onFailure;
    }

    @Override
    public final Throwable call()
    {
        try
        {
            while ( keepGoing.getAsBoolean() )
            {
                doWork();
            }
        }
        catch ( Throwable t )
        {
            t.printStackTrace();
            onFailure.run();
            return t;
        }

        return null;
    }

    protected abstract void doWork();
}
