/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

@SuppressWarnings( "WeakerAccess" )
public class DataCollectorProcedures
{
    @Context
    public DataCollector dataCollector;

    @Admin
    @Description( "Retrieve statistical data about the current database. Available sections are '" +
                  GraphCountsSection.NAME + "', '" + TokensSection.NAME + "'" )
    @Procedure( name = "db.stats.retrieve", mode = Mode.READ )
    public Stream<RetrieveResult> retrieve( @Name( value = "section" ) String section )
            throws InvalidArgumentsException, IndexNotFoundKernelException, TransactionFailureException
    {
        String upperSection = section.toUpperCase();
        switch ( upperSection )
        {
        case GraphCountsSection.NAME:
            return GraphCountsSection.collect( dataCollector.kernel, Anonymizer.PLAIN_TEXT );

        case TokensSection.NAME:
            return TokensSection.collect( dataCollector.kernel );

        default:
            throw new InvalidArgumentsException( String.format( "Unknown retrieve section '%s', known sections are ['%s', '%s']",
                                                                section, GraphCountsSection.NAME, TokensSection.NAME ) );
        }
    }

    @Admin
    @Description( "Retrieve all available statistical data about the current database, in an anonymized form." )
    @Procedure( name = "db.stats.retrieveAllAnonymized", mode = Mode.READ )
    public Stream<RetrieveResult> retrieveAllAnonymized( @Name( value = "graphToken" ) String graphToken )
            throws IndexNotFoundKernelException, TransactionFailureException
    {
        Map<String, Object> metaData = new HashMap<>();
        metaData.put( "graphToken", graphToken );
        metaData.put( "retrieveTime", ZonedDateTime.now() );
        TokensSection.putTokenCounts( metaData, dataCollector.kernel );
        Stream<RetrieveResult> meta = Stream.of( new RetrieveResult( "META", metaData ) );

        return Stream.concat( meta, GraphCountsSection.collect( dataCollector.kernel, Anonymizer.IDS ) );
    }

    @Admin
    @Description( "Retrieve the status of all available collector daemons, for this database." )
    @Procedure( name = "db.stats.status", mode = Mode.READ )
    public Stream<StatusResult> status()
    {
        CollectorStateMachine.Status status = dataCollector.queryCollector.status();
        return Stream.of( new StatusResult( QueriesSection.NAME, status.message, Collections.emptyMap() ) );
    }

    @Admin
    @Description( "Start data collection of a given data section." )
    @Procedure( name = "db.stats.collect", mode = Mode.READ )
    public Stream<ActionResult> collect( @Name( value = "section" ) String section )
    {
        CollectorStateMachine.Result result = dataCollector.queryCollector.collect();
        return Stream.of( new ActionResult( section, result.success, result.message ) );
    }

    @Admin
    @Description( "Stop data collection of a given data section." )
    @Procedure( name = "db.stats.stop", mode = Mode.READ )
    public Stream<ActionResult> stop( @Name( value = "section" ) String section )
    {
        CollectorStateMachine.Result result = dataCollector.queryCollector.stop();
        return Stream.of( new ActionResult( section, result.success, result.message ) );
    }

    @Admin
    @Description( "Clear collected data of a given data section." )
    @Procedure( name = "db.stats.clear", mode = Mode.READ )
    public Stream<ActionResult> clear( @Name( value = "section" ) String section )
    {
        CollectorStateMachine.Result result = dataCollector.queryCollector.clear();
        return Stream.of( new ActionResult( section, result.success, result.message ) );
    }
}
