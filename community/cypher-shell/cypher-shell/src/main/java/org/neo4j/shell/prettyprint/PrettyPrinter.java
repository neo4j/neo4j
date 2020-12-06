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
package org.neo4j.shell.prettyprint;

import java.util.Set;
import javax.annotation.Nonnull;

import org.neo4j.shell.cli.Format;
import org.neo4j.shell.state.BoltResult;

import static org.neo4j.shell.prettyprint.OutputFormatter.Capabilities.FOOTER;
import static org.neo4j.shell.prettyprint.OutputFormatter.Capabilities.INFO;
import static org.neo4j.shell.prettyprint.OutputFormatter.Capabilities.PLAN;
import static org.neo4j.shell.prettyprint.OutputFormatter.Capabilities.RESULT;
import static org.neo4j.shell.prettyprint.OutputFormatter.Capabilities.STATISTICS;

/**
 * Print the result from neo4j in a intelligible fashion.
 */
public class PrettyPrinter
{
    private final StatisticsCollector statisticsCollector;
    private final OutputFormatter outputFormatter;

    public PrettyPrinter( @Nonnull PrettyConfig prettyConfig )
    {
        this.statisticsCollector = new StatisticsCollector( prettyConfig.format );
        this.outputFormatter = selectFormatter( prettyConfig );
    }

    public void format( @Nonnull final BoltResult result, LinePrinter linePrinter )
    {
        Set<OutputFormatter.Capabilities> capabilities = outputFormatter.capabilities();

        int numberOfRows = 0;
        if ( capabilities.contains( RESULT ) )
        {
            numberOfRows = outputFormatter.formatAndCount( result, linePrinter );
        }

        if ( capabilities.contains( INFO ) )
        {
            printIfNotEmpty( outputFormatter.formatInfo( result.getSummary() ), linePrinter );
        }
        if ( capabilities.contains( PLAN ) )
        {
            printIfNotEmpty( outputFormatter.formatPlan( result.getSummary() ), linePrinter );
        }
        if ( capabilities.contains( FOOTER ) )
        {
            printIfNotEmpty( outputFormatter.formatFooter( result, numberOfRows ), linePrinter );
        }
        if ( capabilities.contains( STATISTICS ) )
        {
            printIfNotEmpty( statisticsCollector.collect( result.getSummary() ), linePrinter );
        }
    }

    // Helper for testing
    String format( @Nonnull final BoltResult result )
    {
        StringBuilder sb = new StringBuilder();
        format( result, line ->
        {
            if ( line != null && !line.trim().isEmpty() )
            {
                sb.append( line ).append( OutputFormatter.NEWLINE );
            }
        } );
        return sb.toString();
    }

    private void printIfNotEmpty( String s, LinePrinter linePrinter )
    {
        if ( !s.isEmpty() )
        {
            linePrinter.printOut( s );
        }
    }

    private OutputFormatter selectFormatter( PrettyConfig prettyConfig )
    {
        if ( prettyConfig.format == Format.VERBOSE )
        {
            return new TableOutputFormatter( prettyConfig.wrap, prettyConfig.numSampleRows );
        }
        else
        {
            return new SimpleOutputFormatter();
        }
    }
}
