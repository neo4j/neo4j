/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.benchmark;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.NeoService;

/**
 * Base class for benchmarks done on a NeoSevice. The method neoAlgoBenchMarkRun
 * will call the abstract methods setUpGlobal and tearDownGlobal and in between
 * them call the three methods setUp, runBenchMark and tearDown a number of
 * times defined by the numberOfRuns variable. An average time is calculated and
 * saved for later inspection.
 * @author Patrik Larsson
 */
abstract public class NeoAlgoBenchmark
{
    protected NeoService neo;
    protected int numberOfWarmupRuns = 3;
    protected int numberOfRuns = 10;
    protected List<Object> internalIds;
    protected Object currentInternalId;
    private Map<Object,Double> results = new HashMap<Object,Double>();

    /**
     * Method called for setting up the global benchmark environment, such as
     * starting a NeoService and maybe generating graphs.
     * @param neoStoreDir
     */
    protected void setUpGlobal( String neoStoreDir )
    {
        neo = new EmbeddedNeo( neoStoreDir );
    }

    protected void setUpGlobal()
    {
        setUpGlobal( "target/benchmarkdata" );
    }

    protected void tearDownGlobal()
    {
        neo.shutdown();
    }

    abstract public String getTestId();

    /**
     * Method called before every single run of the runBenchMark method.
     */
    abstract protected void setUp();

    /**
     * Method called after every single run of the runBenchMark method.
     */
    abstract protected void tearDown();

    /**
     * Method called to run the benchmark once. This is the called that is
     * timed.
     */
    abstract protected void runBenchMark();

    public void neoAlgoBenchMarkRun()
    {
        if ( internalIds == null )
        {
            neoAlgoBenchMarkRunInternal();
        }
        else
        {
            for ( Object id : internalIds )
            {
                currentInternalId = id;
                neoAlgoBenchMarkRunInternal();
            }
            System.out.println( " --- Results ---" );
            for ( Object object : internalIds )
            {
                System.out
                    .println( object + ": " + results.get( object ) + "s" );
            }
        }
    }

    private void neoAlgoBenchMarkRunInternal()
    {
        System.out.println( "Starting benchmark " + getTestId() );
        setUpGlobal();
        // Warmup
        System.out.println( "Running warmup" );
        for ( int i = 0; i < numberOfWarmupRuns; ++i )
        {
            runBenchMark();
        }
        System.out.println( "Running benchmarks" );
        long start, end;
        long averageTime = 0;
        for ( int i = 0; i < numberOfRuns; ++i )
        {
            setUp();
            start = System.currentTimeMillis();
            runBenchMark();
            end = System.currentTimeMillis();
            tearDown();
            averageTime += end - start;
            System.out.println( "Time: " + ((end - start) / 1000.0) + "s" );
        }
        System.out.print( "Average time" );
        if ( currentInternalId != null )
        {
            System.out.print( " for " + currentInternalId );
            results.put( currentInternalId,
                (averageTime / numberOfRuns / 1000.0) );
        }
        System.out.println( ": " + (averageTime / numberOfRuns / 1000.0) + "s" );
        tearDownGlobal();
        if ( !saveStatistic( averageTime / numberOfRuns, true ) )
        {
            System.out.println( "Failed to save statistics" );
        }
    }

    /**
     * This method saves the measured time, by appending it to a normal text
     * file whose name is decided by getTestId().
     * @param milliSeconds
     * @param print
     *            If true, the time will be printed, together with a number of
     *            previous results.
     * @return False if something goes wrong, like no write permission to the
     *         file etc.
     */
    protected boolean saveStatistic( long milliSeconds, boolean print )
    {
        String fileName = "target/benchmarkstatistics/" + getTestId();
        String data = "";
        // Make directory
        new File( "target/benchmarkstatistics/" ).mkdirs();
        // Read
        try
        {
            BufferedReader reader = new BufferedReader( new FileReader(
                fileName ) );
            data = reader.readLine();
            reader.close();
        }
        catch ( Exception exception )
        {
        }
        // Print it
        if ( print && !data.equals( "" ) )
        {
            System.out.println( "Previous results: " + data );
        }
        // Append new data and write
        try
        {
            if ( !data.equals( "" ) )
            {
                data += ", ";
            }
            data += (milliSeconds / 1000.0);
            BufferedWriter writer = new BufferedWriter( new FileWriter(
                fileName ) );
            writer.write( data );
            writer.newLine();
            writer.close();
        }
        catch ( Exception exception )
        {
            return false;
        }
        return true;
    }

    /**
     * @return the numberOfRuns
     */
    public int getNumberOfRuns()
    {
        return numberOfRuns;
    }

    /**
     * @param numberOfRuns
     *            the numberOfRuns to set
     */
    public void setNumberOfRuns( int numberOfRuns )
    {
        this.numberOfRuns = numberOfRuns;
    }

    /**
     * @return the numberOfWarmupRuns
     */
    public int getNumberOfWarmupRuns()
    {
        return numberOfWarmupRuns;
    }

    /**
     * @param numberOfWarmupRuns
     *            the numberOfWarmupRuns to set
     */
    public void setNumberOfWarmupRuns( int numberOfWarmupRuns )
    {
        this.numberOfWarmupRuns = numberOfWarmupRuns;
    }
}
