/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.neo4j.helpers.Clock;
import org.neo4j.unsafe.impl.batchimport.Configuration;

import static java.lang.Math.min;
import static java.lang.Runtime.getRuntime;

/**
 * Convenience around executing and supervising {@link Stage stages}.
 */
public class ExecutionSupervisors
{
    /**
     * Using an {@link ExecutionMonitors#invisible() invisible} monitor.
     * @see #superviseDynamicExecution(ExecutionMonitor, Stage...)
     */
    public static void superviseDynamicExecution( Stage... stages )
    {
        superviseDynamicExecution( ExecutionMonitors.invisible(), stages );
    }

    /**
     * With {@link Configuration#DEFAULT}.
     * @see #superviseDynamicExecution(ExecutionMonitor, Configuration, Stage...).
     */
    public static void superviseDynamicExecution( ExecutionMonitor monitor, Stage... stages )
    {
        superviseDynamicExecution( monitor, Configuration.DEFAULT, stages );
    }

    /**
     * Supervises an execution with the given monitor AND a {@link DynamicProcessorAssigner} to giv
     * the execution a dynamic and optimal nature.
     *
     * @see #superviseExecution(ExecutionMonitor, Configuration, Stage...)
     */
    public static void superviseDynamicExecution( ExecutionMonitor monitor, Configuration config, Stage... stages )
    {
        superviseExecution( withDynamicProcessorAssignment( monitor, config ), config, stages );
    }

    /**
     * Executes a number of stages simultaneously, letting the given {@code monitor} get insight into the
     * execution.
     *
     * @param monitor {@link ExecutionMonitor} to get insight into the execution.
     * @param config {@link Configuration} for the execution.
     * @param stages {@link Stage stages} to execute.
     */
    public static void superviseExecution( ExecutionMonitor monitor, Configuration config, Stage... stages )
    {
        ExecutionSupervisor supervisor = new ExecutionSupervisor( Clock.SYSTEM_CLOCK, monitor );
        try
        {
            StageExecution[] executions = new StageExecution[stages.length];
            for ( int i = 0; i < stages.length; i++ )
            {
                executions[i] = stages[i].execute();
            }
            supervisor.supervise( executions );
        }
        finally
        {
            for ( Stage stage : stages )
            {
                stage.close();
            }
        }
    }

    /**
     * Decorates an {@link ExecutionMonitor} with a {@link DynamicProcessorAssigner} responsible for
     * constantly assigning and reevaluating an optimal number of processors to all individual steps.
     *
     * @param monitor {@link ExecutionMonitor} to decorate.
     * @param config {@link Configuration} that the {@link DynamicProcessorAssigner} will use. Max total processors
     * in a {@link Stage} will be the smallest of that value and {@link Runtime#availableProcessors()}.
     * @return the decorated monitor with dynamic processor assignment capabilities.
     */
    public static ExecutionMonitor withDynamicProcessorAssignment( ExecutionMonitor monitor, Configuration config )
    {
        DynamicProcessorAssigner dynamicProcessorAssigner = new DynamicProcessorAssigner( config,
                min( config.maxNumberOfProcessors(), getRuntime().availableProcessors() ) );
        return new MultiExecutionMonitor( monitor, dynamicProcessorAssigner );
    }
}
