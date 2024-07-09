/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.batchimport.staging;

import org.neo4j.batchimport.api.Configuration;
import org.neo4j.time.Clocks;

/**
 * Convenience around executing and supervising {@link Stage stages}.
 */
public class ExecutionSupervisors {
    private ExecutionSupervisors() {}

    /**
     * Using an {@link ExecutionMonitor#INVISIBLE invisible} monitor.
     * @param stage {@link Stage} to supervise.
     * @see #superviseDynamicExecution(ExecutionMonitor, Stage)
     */
    public static void superviseDynamicExecution(Stage stage) {
        superviseDynamicExecution(ExecutionMonitor.INVISIBLE, stage);
    }

    /**
     * With {@link Configuration#DEFAULT}.
     * @param monitor {@link ExecutionMonitor} notifying user about progress.
     * @param stage {@link Stage} to supervise.
     * @see #superviseDynamicExecution(ExecutionMonitor, Configuration, Stage)
     */
    public static void superviseDynamicExecution(ExecutionMonitor monitor, Stage stage) {
        superviseDynamicExecution(monitor, Configuration.DEFAULT, stage);
    }

    /**
     * Supervises an execution with the given monitor AND a {@link DynamicProcessorAssigner} to give
     * the execution a dynamic and optimal nature.
     * @param monitor {@link ExecutionMonitor} notifying user about progress.
     * @param config {@link Configuration} of the import.
     * @param stage {@link Stage} to supervise.
     *
     * @see #superviseExecution(ExecutionMonitor, Stage)
     */
    public static void superviseDynamicExecution(ExecutionMonitor monitor, Configuration config, Stage stage) {
        superviseExecution(withDynamicProcessorAssignment(monitor, config), stage);
    }

    /**
     * Executes a number of stages simultaneously, letting the given {@code monitor} get insight into the
     * execution.
     *
     * @param monitor {@link ExecutionMonitor} to get insight into the execution.
     * @param stage {@link Stage stages} to execute.
     */
    public static void superviseExecution(ExecutionMonitor monitor, Stage stage) {
        ExecutionSupervisor supervisor = new ExecutionSupervisor(Clocks.systemClock(), monitor);
        StageExecution execution = null;
        try {
            execution = stage.execute();
            supervisor.supervise(execution);
        } finally {
            stage.close();
            if (execution != null) {
                execution.assertHealthy();
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
    public static ExecutionMonitor withDynamicProcessorAssignment(ExecutionMonitor monitor, Configuration config) {
        DynamicProcessorAssigner dynamicProcessorAssigner = new DynamicProcessorAssigner(config);
        return new MultiExecutionMonitor(monitor, dynamicProcessorAssigner);
    }
}
