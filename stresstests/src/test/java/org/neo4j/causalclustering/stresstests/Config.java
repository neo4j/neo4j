/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.stresstests;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.LogProvider;

import static java.lang.System.getProperty;
import static java.lang.System.getenv;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Optional.ofNullable;

@SuppressWarnings( {"SameParameterValue", "unused"} )
public class Config
{
    private static final String ENV_OVERRIDE_PREFIX = "STRESS_TESTING_";

    /* platform */
    private LogProvider logProvider;
    private String workingDir;

    /* general */
    private int numberOfCores;
    private int numberOfEdges;

    private int workDurationMinutes;
    private int shutdownDurationMinutes;

    private String txPrune;

    private Collection<Preparations> preparations;
    private Collection<Workloads> workloads;
    private Collection<Validations> validations;

    /* workload specific */
    private boolean enableIndexes;
    private long reelectIntervalSeconds;

    public Config()
    {
        logProvider = FormattedLogProvider.toOutputStream( System.out );
        workingDir = envOrDefault( "WORKING_DIR", new File( getProperty( "java.io.tmpdir" ) ).getPath() );

        numberOfCores = envOrDefault( "NUMBER_OF_CORES", 3 );
        numberOfEdges = envOrDefault( "NUMBER_OF_EDGES", 1 );

        workDurationMinutes = envOrDefault( "WORK_DURATION_MINUTES", 30 );
        shutdownDurationMinutes = envOrDefault( "SHUTDOWN_DURATION_MINUTES", 5 );

        txPrune = envOrDefault( "TX_PRUNE", "50 files" );

        preparations = envOrDefault( Preparations.class, "PREPARATIONS" );
        workloads = envOrDefault( Workloads.class, "WORKLOADS" );
        validations = envOrDefault( Validations.class, "VALIDATIONS", Validations.ConsistencyCheck );

        enableIndexes = envOrDefault( "ENABLE_INDEXES", false );
        reelectIntervalSeconds = envOrDefault( "REELECT_INTERVAL_SECONDS", 60L );
    }

    private static String envOrDefault( String name, String defaultValue )
    {
        String environmentVariableName = ENV_OVERRIDE_PREFIX + name;
        return ofNullable( getenv( environmentVariableName ) ).orElse( defaultValue );
    }

    private static int envOrDefault( String name, int defaultValue )
    {
        String environmentVariableName = ENV_OVERRIDE_PREFIX + name;
        return ofNullable( getenv( environmentVariableName ) ).map( Integer::parseInt ).orElse( defaultValue );
    }

    private static long envOrDefault( String name, long defaultValue )
    {
        String environmentVariableName = ENV_OVERRIDE_PREFIX + name;
        return ofNullable( getenv( environmentVariableName ) ).map( Long::parseLong ).orElse( defaultValue );
    }

    private static boolean envOrDefault( String name, boolean defaultValue )
    {
        String environmentVariableName = ENV_OVERRIDE_PREFIX + name;
        return ofNullable( getenv( environmentVariableName ) ).map( Boolean::parseBoolean ).orElse( defaultValue );
    }

    @SafeVarargs
    private static <T extends Enum<T>> Collection<T> envOrDefault( Class<T> type, String name, T... defaultValue )
    {
        String environmentVariableName = ENV_OVERRIDE_PREFIX + name;
        return ofNullable( getenv( environmentVariableName ) ).map( env -> parseEnum( env, type ) ).orElse( asList( defaultValue ) );
    }

    private static <T extends Enum<T>> Collection<T> parseEnum( String value, Class<T> type )
    {
        if ( value == null || value.length() == 0 )
        {
            return emptyList();
        }

        ArrayList<T> workloads = new ArrayList<>();
        String[] split = value.split( "," );
        for ( String workloadString : split )
        {
            workloads.add( T.valueOf( type, workloadString ) );
        }
        return workloads;
    }

    public LogProvider logProvider()
    {
        return logProvider;
    }

    public void logProvider( LogProvider logProvider )
    {
        this.logProvider = logProvider;
    }

    public String workingDir()
    {
        return workingDir;
    }

    public int numberOfCores()
    {
        return numberOfCores;
    }

    public int numberOfEdges()
    {
        return numberOfEdges;
    }

    public void numberOfEdges( int numberOfEdges )
    {
        this.numberOfEdges = numberOfEdges;
    }

    public void workDurationMinutes( int workDurationMinutes )
    {
        this.workDurationMinutes = workDurationMinutes;
    }

    public int workDurationMinutes()
    {
        return workDurationMinutes;
    }

    public int shutdownDurationMinutes()
    {
        return shutdownDurationMinutes;
    }

    public String txPrune()
    {
        return txPrune;
    }

    public void preparations( Preparations... preparations )
    {
        this.preparations = asList( preparations );
    }

    public Collection<Preparations> preparations()
    {
        return preparations;
    }

    public void workloads( Workloads... workloads )
    {
        this.workloads = asList( workloads );
    }

    public Collection<Workloads> workloads()
    {
        return workloads;
    }

    public void validations( Validations... validations )
    {
        this.validations = asList( validations );
    }

    public Collection<Validations> validations()
    {
        return validations;
    }

    public boolean enableIndexes()
    {
        return enableIndexes;
    }

    public long reelectIntervalSeconds()
    {
        return reelectIntervalSeconds;
    }

    public void reelectIntervalSeconds( int reelectIntervalSeconds )
    {
        this.reelectIntervalSeconds = reelectIntervalSeconds;
    }

    @Override
    public String toString()
    {
        return "Config{" + "numberOfCores=" + numberOfCores + ", numberOfEdges=" + numberOfEdges + ", workingDir='" + workingDir + '\'' +
                ", workDurationMinutes=" + workDurationMinutes + ", shutdownDurationMinutes=" + shutdownDurationMinutes + ", txPrune='" + txPrune + '\'' +
                ", preparations=" + preparations + ", workloads=" + workloads + ", validations=" + validations + ", enableIndexes=" + enableIndexes +
                ", reelectIntervalSeconds=" + reelectIntervalSeconds + '}';
    }
}
