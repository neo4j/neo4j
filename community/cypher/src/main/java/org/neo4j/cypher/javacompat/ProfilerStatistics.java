package org.neo4j.cypher.javacompat;

/**
 * Profiler statistics for a single execution step of a Cypher query execution plan
 */
public interface ProfilerStatistics
{
    /**
     * @return PlanDescription for which these ProfilerStatistics have been collected
     */
    PlanDescription getPlanDescription();

    /**
     * @return number of rows processed by the associated execution step
     */
    long getRows();

    /**
     * @return number of database hits (potential disk accesses) caused by executing the associated execution step
     */
    long getDbHits();
}
