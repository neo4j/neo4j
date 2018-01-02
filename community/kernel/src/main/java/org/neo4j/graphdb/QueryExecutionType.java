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
package org.neo4j.graphdb;

import static java.util.Objects.requireNonNull;

/**
 * Signifies how a query is executed, as well as what side effects and results could be expected from the query.
 * <p>
 * In Cypher there are three different modes of execution:
 * <ul>
 * <li>Normal execution,</li>
 * <li>execution with the <a href="http://neo4j.com/docs/stable/execution-plans.html">{@code PROFILE}</a> directive,
 * and</li>
 * <li>execution with the <a href="http://neo4j.com/docs/stable/execution-plans.html">{@code EXPLAIN}</a>
 * directive.</li>
 * </ul>
 * Instances of this class contain the required information to be able to tell these different execution modes apart.
 * It also contains information about what effects the query could have, and whether it could yield any results, in
 * form
 * of the {@link QueryType QueryType enum}.
 * <p>
 * Queries executed with the {@code PROFILE} directive can have side effects and produce results in the same way as a
 * normally executed method. The difference being that the user has expressed an interest in seeing the plan used to
 * execute the query, and that this plan will (after execution completes) be annotated with
 * {@linkplain ExecutionPlanDescription#getProfilerStatistics() profiling information} from the execution of the query.
 * <p>
 * Queries executed with the {@code EXPLAIN} directive never have any side effects, nor do they ever yield any rows in
 * the results, the sole purpose of this mode of execution is to
 * {@linkplain Result#getExecutionPlanDescription() get a description of the plan} that <i>would</i> be executed
 * if/when the query is executed normally (or under {@code PROFILE}).
 */
public final class QueryExecutionType
{
    /**
     * Signifies what type of query an {@link QueryExecutionType} executes.
     */
    public enum QueryType
    {
        /** A read-only query, that does not change any data, but only produces a result. */
        READ_ONLY,
        /** A read/write query, that creates or updates data, and also produces a result. */
        READ_WRITE,
        /** A write-only query, that creates or updates data, but does not yield any rows in the result. */
        WRITE,
        /**
         * A schema changing query, that updates the schema but neither changes any data nor yields any rows in the
         * result.
         */
        SCHEMA_WRITE,;
        private final QueryExecutionType query, profiled, explained;

        QueryType()
        {
            this.query = new QueryExecutionType( Execution.QUERY, this );
            this.profiled = new QueryExecutionType( Execution.PROFILE, this );
            this.explained = new QueryExecutionType( Execution.EXPLAIN, this );
        }
    }

    /**
     * Get the {@link QueryExecutionType} that signifies normal execution of a query of the supplied type.
     *
     * @param type the type of query executed.
     * @return The instance that signifies normal execution of the supplied {@link QueryType}.
     */
    public static QueryExecutionType query( QueryType type )
    {
        return requireNonNull( type, "QueryType" ).query;
    }

    /**
     * Get the {@link QueryExecutionType} that signifies profiled execution of a query of the supplied type.
     *
     * @param type the type of query executed.
     * @return The instance that signifies profiled execution of the supplied {@link QueryType}.
     */
    public static QueryExecutionType profiled( QueryType type )
    {
        return requireNonNull( type, "QueryType" ).profiled;
    }

    /**
     * Get the {@link QueryExecutionType} that signifies explaining the plan of a query of the supplied type.
     *
     * @param type the type of query executed.
     * @return The instance that signifies explaining the plan of the supplied {@link QueryType}.
     */
    public static QueryExecutionType explained( QueryType type )
    {
        return requireNonNull( type, "QueryType" ).explained;
    }

    /**
     * Get the type of query this execution refers to.
     *
     * @return the type of query this execution refers to.
     */
    public QueryType queryType()
    {
        return type;
    }

    /**
     * Signifies whether results from this execution
     * {@linkplain ExecutionPlanDescription#getProfilerStatistics() contains profiling information}.
     *
     * This is {@code true} for queries executed with the
     * <a href="http://neo4j.com/docs/stable/execution-plans.html">{@code PROFILE}</a> directive.
     *
     * @return {@code true} if the results from this execution would contain profiling information.
     */
    public boolean isProfiled()
    {
        return execution == Execution.PROFILE;
    }

    /**
     * Signifies whether the supplied query contained a directive that asked for a
     * {@linkplain ExecutionPlanDescription description of the execution plan}.
     *
     * This is {@code true} for queries executed with either the
     * <a href="http://neo4j.com/docs/stable/execution-plans.html">{@code EXPLAIN} or {@code PROFILE} directives</a>.
     *
     * @return {@code true} if a description of the plan should be presented to the user.
     */
    public boolean requestedExecutionPlanDescription()
    {
        return execution != Execution.QUERY;
    }

    /**
     * Signifies that the query was executed with the
     * <a href="http://neo4j.com/docs/stable/execution-plans.html">{@code EXPLAIN} directive</a>.
     *
     * @return {@code true} if the query was executed using the {@code EXPLAIN} directive.
     */
    public boolean isExplained()
    {
        return execution == Execution.EXPLAIN;
    }

    /**
     * Signifies that the execution of the query could produce a result.
     *
     * This is an important distinction from the result being empty.
     *
     * @return {@code true} if the execution would yield rows in the result set.
     */
    public boolean canContainResults()
    {
        return (type == QueryType.READ_ONLY || type == QueryType.READ_WRITE) && execution != Execution.EXPLAIN;
    }

    /**
     * Signifies that the execution of the query could perform changes to the data.
     *
     * {@link Result}{@link Result#getQueryStatistics() .getQueryStatistics()}{@link QueryStatistics#containsUpdates()
     * .containsUpdates()} signifies whether the query actually performed any updates.
     *
     * @return {@code true} if the execution could perform changes to data.
     */
    public boolean canUpdateData()
    {
        return (type == QueryType.READ_WRITE || type == QueryType.WRITE) && execution != Execution.EXPLAIN;
    }

    /**
     * Signifies that the execution of the query updates the schema.
     *
     * @return {@code true} if the execution updates the schema.
     */
    public boolean canUpdateSchema()
    {
        return type == QueryType.SCHEMA_WRITE && execution != Execution.EXPLAIN;
    }

    private final Execution execution;
    private final QueryType type;

    private QueryExecutionType( Execution execution, QueryType type )
    {
        this.execution = execution;
        this.type = type;
    }

    @Override
    public String toString()
    {
        return execution.toString( type );
    }

    private enum Execution
    {
        QUERY
                {
                    @Override
                    String toString( QueryType type )
                    {
                        return type.name();
                    }
                },
        PROFILE,
        EXPLAIN,;

        String toString( QueryType type )
        {
            return name() + ":" + type.name();
        }
    }
}
