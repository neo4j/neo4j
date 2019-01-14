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
package org.neo4j.unsafe.impl.batchimport.staging;

import java.util.function.Supplier;

import static org.neo4j.helpers.Exceptions.throwIfUnchecked;

/**
 * A simple {@link StageControl} for tests with multiple steps and where an error or assertion failure
 * propagates to other steps. Create the {@link SimpleStageControl}, pass it into the {@link Step steps}
 * and then when all steps are created, call {@link #steps(Step...)} to let the control know about them.
 */
public class SimpleStageControl implements StageControl
{
    private volatile Throwable panic;
    private volatile Step<?>[] steps;

    public void steps( Step<?>... steps )
    {
        this.steps = steps;
    }

    @Override
    public void panic( Throwable cause )
    {
        this.panic = cause;
        for ( Step<?> step : steps )
        {
            step.receivePanic( cause );
        }
    }

    @Override
    public void assertHealthy()
    {
        if ( panic != null )
        {
            throwIfUnchecked( panic );
            throw new RuntimeException( panic );
        }
    }

    @Override
    public void recycle( Object batch )
    {
    }

    @Override
    public <T> T reuse( Supplier<T> fallback )
    {
        return fallback.get();
    }
}
