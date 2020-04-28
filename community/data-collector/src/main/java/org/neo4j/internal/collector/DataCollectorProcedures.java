/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.collector;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.procedure.SystemProcedure;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.values.ValueMapper;

import static java.lang.String.format;

@SuppressWarnings( "WeakerAccess" )
public class DataCollectorProcedures
{
    @Context
    public DataCollector dataCollector;

    @Context
    public KernelTransaction transaction;

    @Context
    public ProcedureCallContext callContext;

    @Context
    public ValueMapper valueMapper;

    @Admin
    @SystemProcedure
    @Description( "Retrieve statistical data about the current database. Valid sections are '" +
                  Sections.GRAPH_COUNTS + "', '" + Sections.TOKENS + "', '" + Sections.QUERIES + "', '" + Sections.META + '\'' )
    @Procedure( name = "db.stats.retrieve", mode = Mode.READ )
    public Stream<RetrieveResult> retrieve( @Name( value = "section" ) String section,
                                            @Name( value = "config", defaultValue = "{}" ) Map<String, Object> config )
            throws InvalidArgumentsException, IndexNotFoundKernelException, TransactionFailureException
    {
        if ( callContext.isSystemDatabase() )
        {
            return Stream.empty();
        }

        String upperSection = section.toUpperCase();
        switch ( upperSection )
        {
        case Sections.GRAPH_COUNTS:
            return GraphCountsSection.retrieve( dataCollector.getKernel(), Anonymizer.PLAIN_TEXT );

        case Sections.TOKENS:
            return TokensSection.retrieve( dataCollector.getKernel() );

        case Sections.META:
            return MetaSection.retrieve( null, dataCollector.getKernel(), dataCollector.getQueryCollector().numSilentQueryDrops() );

        case Sections.QUERIES:
            return QueriesSection.retrieve( dataCollector.getQueryCollector().getData(),
                                            new PlainText( (ValueMapper.JavaMapper) valueMapper ),
                                            RetrieveConfig.of( config ).maxInvocations );

        default:
            throw Sections.unknownSectionException( section );
        }
    }

    @Admin
    @SystemProcedure
    @Description( "Retrieve all available statistical data about the current database, in an anonymized form." )
    @Procedure( name = "db.stats.retrieveAllAnonymized", mode = Mode.READ )
    public Stream<RetrieveResult> retrieveAllAnonymized( @Name( value = "graphToken" ) String graphToken,
                                                         @Name( value = "config", defaultValue = "{}" ) Map<String, Object> config )
            throws IndexNotFoundKernelException, TransactionFailureException, InvalidArgumentsException
    {
        if ( callContext.isSystemDatabase() )
        {
            return Stream.empty();
        }

        if ( graphToken == null || graphToken.equals( "" ) )
        {
            throw new InvalidArgumentsException( "Graph token must be a non-empty string" );
        }

        return Stream.of( MetaSection.retrieve( graphToken, dataCollector.getKernel(), dataCollector.getQueryCollector().numSilentQueryDrops() ),
                          GraphCountsSection.retrieve( dataCollector.getKernel(), Anonymizer.IDS ),
                          QueriesSection.retrieve( dataCollector.getQueryCollector().getData(),
                                                   new IdAnonymizer( transaction.tokenRead() ),
                                                   RetrieveConfig.of( config ).maxInvocations )
            ).flatMap( x -> x );
    }

    @Admin
    @SystemProcedure
    @Description( "Retrieve the status of all available collector daemons, for this database." )
    @Procedure( name = "db.stats.status", mode = Mode.READ )
    public Stream<StatusResult> status()
    {
        if ( callContext.isSystemDatabase() )
        {
            return Stream.empty();
        }

        CollectorStateMachine.Status status = dataCollector.getQueryCollector().status();
        return Stream.of( new StatusResult( Sections.QUERIES, status.message, Collections.emptyMap() ) );
    }

    @Admin
    @SystemProcedure
    @Description( "Start data collection of a given data section. Valid sections are '" + Sections.QUERIES + '\'' )
    @Procedure( name = "db.stats.collect", mode = Mode.READ )
    public Stream<ActionResult> collect( @Name( value = "section" ) String section,
                                         @Name( value = "config", defaultValue = "{}" ) Map<String, Object> config ) throws InvalidArgumentsException
    {
        if ( callContext.isSystemDatabase() )
        {
            return Stream.empty();
        }

        CollectorStateMachine.Result result = collectorStateMachine( section ).collect( config );
        return Stream.of( new ActionResult( section, result.success, result.message ) );
    }

    @Admin
    @SystemProcedure
    @Description( "Stop data collection of a given data section. Valid sections are '" + Sections.QUERIES + '\'' )
    @Procedure( name = "db.stats.stop", mode = Mode.READ )
    public Stream<ActionResult> stop( @Name( value = "section" ) String section ) throws InvalidArgumentsException
    {
        if ( callContext.isSystemDatabase() )
        {
            return Stream.empty();
        }

        CollectorStateMachine.Result result = collectorStateMachine( section ).stop( Long.MAX_VALUE );
        return Stream.of( new ActionResult( section, result.success, result.message ) );
    }

    @Admin
    @SystemProcedure
    @Description( "Clear collected data of a given data section. Valid sections are '" + Sections.QUERIES + '\'' )
    @Procedure( name = "db.stats.clear", mode = Mode.READ )
    public Stream<ActionResult> clear( @Name( value = "section" ) String section ) throws InvalidArgumentsException
    {
        if ( callContext.isSystemDatabase() )
        {
            return Stream.empty();
        }

        CollectorStateMachine.Result result = collectorStateMachine( section ).clear();
        return Stream.of( new ActionResult( section, result.success, result.message ) );
    }

    private QueryCollector collectorStateMachine( String section ) throws InvalidArgumentsException
    {
        switch ( section )
        {
        case Sections.TOKENS:
        case Sections.GRAPH_COUNTS:
            throw new InvalidArgumentsException(
                    format( "Section '%s' does not have to be explicitly collected, it can always be directly retrieved.", section ) );
        case Sections.QUERIES:
            return dataCollector.getQueryCollector();
        default:
            throw Sections.unknownSectionException( section );
        }
    }
}
