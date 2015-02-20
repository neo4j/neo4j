/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
    public static void superviseDynamicExecution( Stage... stages )
    {
        superviseDynamicExecution( ExecutionMonitors.invisible(), Configuration.DEFAULT, stages );
    }

    public static void superviseDynamicExecution( ExecutionMonitor monitor, Stage... stages )
    {
        superviseDynamicExecution( monitor, Configuration.DEFAULT, stages );
    }

    /**
     * Executes a number of stages simultaneously, letting the given {@code monitor} get insight into the
     * execution. A {@link DynamicProcessorAssigner} is also added as a {@link ExecutionMonitor} to give
     * the execution a dynamic nature.
     *
     * @param monitor {@link ExecutionMonitor} to get insight into the execution.
     * @param config {@link Configuration} for the execution.
     * @param stages {@link Stage stages} to execute.
     */
    public static void superviseDynamicExecution( ExecutionMonitor monitor, Configuration config, Stage... stages )
    {
        DynamicProcessorAssigner dynamicProcessorAssigner = new DynamicProcessorAssigner( config,
                min( config.maxNumberOfProcessors(), getRuntime().availableProcessors() ) );
        ExecutionSupervisor supervisor = new ExecutionSupervisor( Clock.SYSTEM_CLOCK,
                new MultiExecutionMonitor( monitor, dynamicProcessorAssigner ) );
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
}
