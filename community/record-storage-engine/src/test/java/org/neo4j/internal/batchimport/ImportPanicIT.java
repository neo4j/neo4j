/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.internal.batchimport;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

import org.neo4j.configuration.Config;
import org.neo4j.csv.reader.CharReadable;
import org.neo4j.csv.reader.DataAfterQuoteException;
import org.neo4j.csv.reader.Readables;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.IdType;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.input.InputEntityDecorators;
import org.neo4j.internal.batchimport.input.InputException;
import org.neo4j.internal.batchimport.input.csv.CsvInput;
import org.neo4j.internal.batchimport.input.csv.DataFactories;
import org.neo4j.internal.batchimport.input.csv.DataFactory;
import org.neo4j.internal.batchimport.staging.ExecutionMonitors;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_4;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.internal.batchimport.input.csv.Configuration.COMMAS;

public class ImportPanicIT
{
    private static final int BUFFER_SIZE = 1000;

    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
    private final TestDirectory directory = TestDirectory.testDirectory();
    private final RandomRule random = new RandomRule();

    @Rule
    public final RuleChain rules = RuleChain.outerRule( random ).around( directory );

    /**
     * There was this problem where some steps and in particular parallel CSV input parsing that
     * paniced would hang the import entirely.
     */
    @Test
    public void shouldExitAndThrowExceptionOnPanic() throws Exception
    {
        try ( JobScheduler jobScheduler = new ThreadPoolJobScheduler() )
        {
            BatchImporter importer = new ParallelBatchImporter( directory.databaseLayout(), fs, null, Configuration.DEFAULT,
                    NullLogService.getInstance(), ExecutionMonitors.invisible(), AdditionalInitialIds.EMPTY,
                    Config.defaults(), StandardV3_4.RECORD_FORMATS, ImportLogic.NO_MONITOR, jobScheduler, Collector.EMPTY, EmptyLogFilesInitializer.INSTANCE );
            Iterable<DataFactory> nodeData =
                    DataFactories.datas( DataFactories.data( InputEntityDecorators.NO_DECORATOR, fileAsCharReadable( nodeCsvFileWithBrokenEntries() ) ) );
            Input brokenCsvInput = new CsvInput(
                    nodeData, DataFactories.defaultFormatNodeFileHeader(),
                    DataFactories.datas(), DataFactories.defaultFormatRelationshipFileHeader(),
                    IdType.ACTUAL,
                    csvConfigurationWithLowBufferSize() );
            importer.doImport( brokenCsvInput );
            fail( "Should have failed properly" );
        }
        catch ( InputException e )
        {
            // THEN
            assertTrue( e.getCause() instanceof DataAfterQuoteException );
            // and we managed to shut down properly
        }
    }

    private static org.neo4j.internal.batchimport.input.csv.Configuration csvConfigurationWithLowBufferSize()
    {
        return new org.neo4j.internal.batchimport.input.csv.Configuration.Overridden( COMMAS )
        {
            @Override
            public int bufferSize()
            {
                return BUFFER_SIZE;
            }
        };
    }

    private static Supplier<CharReadable> fileAsCharReadable( File file )
    {
        return () ->
        {
            try
            {
                return Readables.files( StandardCharsets.UTF_8, file );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        };
    }

    private File nodeCsvFileWithBrokenEntries() throws IOException
    {
        File file = directory.file( "broken-node-data.csv" );
        try ( PrintWriter writer = new PrintWriter(
                fs.openAsWriter( file, StandardCharsets.UTF_8, false ) ) )
        {
            writer.println( ":ID,name" );
            int numberOfLines = BUFFER_SIZE * 10;
            int brokenLine = random.nextInt( numberOfLines );
            for ( int i = 0; i < numberOfLines; i++ )
            {
                if ( i == brokenLine )
                {
                    writer.println( i + ",\"broken\"line" );
                }
                else
                {
                    writer.println( i + ",name" + i );
                }
            }
        }
        return file;
    }
}
