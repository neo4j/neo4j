package org.neo4j.cypher.javacompat;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.neo4j.cypher.ProfilerStatisticsNotReadyException;

/**
 * Instances describe single execution steps in a Cypher query execution plan
 *
 * Execution plans form a tree of execution steps.  Each step is described by a PlanDescription object.
 *
 */
public interface PlanDescription
{
    /**
     * @return descriptive name for this kind of execution step
     */
    public String getName();

    /**
     * @return a map containing arbitrary arguments that describe this execution step in more detail
     */
    public Map<String, Object> getArguments();

    /**
     * @return true if this is a topmost execution step
     */
    public boolean isRoot();

    /**
     * @return topmost PlanDescription below which the associated execution step is taking place
     */
    public PlanDescription getRoot();

    /**
     * @return PlanDescription of which this PlanDescription is a child
     * @throws java.util.NoSuchElementException if this PlanDescription is a root
     */
    public PlanDescription getParent() throws NoSuchElementException;

    /**
     * Starting from this PlanDescription, retrieve children by successive calls to getChild() and
     * return the final PlanDescription thus found
     *
     * @return PlanDescription of the final child retrieved
     * @throws NoSuchElementException if no child could be retrieved
     */
    public PlanDescription cd(String... names) throws NoSuchElementException;

    /**
     * @return first child PlanDescription found by searching all children that have the given name
     * @throws java.util.NoSuchElementException if no matching child is found
     */
    public PlanDescription getChild(String name) throws NoSuchElementException;

    /**
     * @return list of previous (child) execution step descriptions
     */
    public List<PlanDescription> getChildren();

    /**
     * @return true, if ProfilerStatistics are available for this execution step
     */
    public boolean hasProfilerStatistics();

    /**
     * @return profiler statistics for this execution step iff available
     * @throws ProfilerStatisticsNotReadyException iff profiler statistics are not available
     */
    public ProfilerStatistics getProfilerStatistics() throws ProfilerStatisticsNotReadyException;
}
