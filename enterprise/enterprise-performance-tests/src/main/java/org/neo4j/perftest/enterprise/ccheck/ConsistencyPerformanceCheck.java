package org.neo4j.perftest.enterprise.ccheck;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.util.DefaultPrettyPrinter;
import org.neo4j.backup.check.ConsistencyCheck;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.ProgressIndicator;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConfigurationDefaults;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.perftest.enterprise.util.Configuration;
import org.neo4j.perftest.enterprise.util.Parameters;
import org.neo4j.perftest.enterprise.util.Setting;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.perftest.enterprise.util.Configuration.SYSTEM_PROPERTIES;
import static org.neo4j.perftest.enterprise.util.Configuration.settingsOf;
import static org.neo4j.perftest.enterprise.util.Setting.booleanSetting;
import static org.neo4j.perftest.enterprise.util.Setting.stringSetting;

public class ConsistencyPerformanceCheck
{
    private static final Setting<Boolean> generate_graph = booleanSetting( "generate_graph", false );
    private static final Setting<String> report_file = stringSetting( "report_file" );

    /**
     * Sample execution:
     * java -cp ... org.neo4j.perftest.enterprise.ccheck.ConsistencyPerformanceCheck
     *    -generate_graph
     *    -report_file target/ccheck_performance.json
     *    -neo4j.store_dir target/ccheck_perf_graph
     *    -report_progress
     *    -node_count 10000000
     *    -relationships FOO:2,BAR:1
     *    -node_properties SINGLE_STRING,SINGLE_STRING,SINGLE_STRING,SINGLE_STRING,SINGLE_STRING
     */
    public static void main( String... args ) throws Exception
    {
        run( Parameters.configuration( SYSTEM_PROPERTIES,
                                       settingsOf( DataGenerator.class, ConsistencyPerformanceCheck.class ) )
                       .convert( args ) );
    }

    private static void run( Configuration configuration ) throws IOException
    {
        File reportFile = new File( configuration.get( report_file ) );
        if ( configuration.get( generate_graph ) )
        {
            File storeDir = new File( configuration.get( DataGenerator.store_dir ) );
            if ( storeDir.isDirectory() )
            {
                FileUtils.deleteRecursively( storeDir );
            }
            DataGenerator.run( configuration );
        }
        // ensure that the store is recovered
        new EmbeddedGraphDatabase( configuration.get( DataGenerator.store_dir ) ).shutdown();

        // run the consistency check
        ProgressIndicator.Factory progress;
        if ( configuration.get( DataGenerator.report_progress ) )
        {
            progress = new ProgressIndicator.Textual( System.out );
        }
        else
        {
            progress = ProgressIndicator.Factory.NONE;
        }
        ConsistencyCheck.run( new TimingProgress( new JsonReportWriter( reportFile ), progress ),
                              configuration.get( DataGenerator.store_dir ),
                              new Config( new ConfigurationDefaults( GraphDatabaseSettings.class )
                                                  .apply( stringMap() ) ) );
    }

    private static class JsonReportWriter implements TimingProgress.Visitor
    {
        private final File target;
        private JsonGenerator json;

        JsonReportWriter( File target )
        {
            this.target = target;
        }

        @Override
        public void beginTimingProgress( long totalElementCount, long totalTimeNanos ) throws IOException
        {
            ensureOpen( false );
            json = new JsonFactory().configure( JsonGenerator.Feature.AUTO_CLOSE_TARGET, true )
                                    .createJsonGenerator( new FileWriter( target ) );
            json.setPrettyPrinter( new DefaultPrettyPrinter() );
            json.writeStartObject();
            {
                json.writeFieldName( "total" );
                json.writeStartObject();
                json.writeNumberField( "elementCount", totalElementCount );
                json.writeNumberField( "time", nanosToMillis( totalTimeNanos ) );
                json.writeEndObject();
            }
            json.writeFieldName( "phases" );
            json.writeStartArray();
        }

        @Override
        public void phaseTimingProgress( String phase, long elementCount, long timeNanos ) throws IOException
        {
            ensureOpen( true );
            json.writeStartObject();
            json.writeStringField( "name", phase );
            json.writeNumberField( "elementCount", elementCount );
            json.writeNumberField( "time", nanosToMillis( timeNanos ) );
            json.writeEndObject();
        }

        @Override
        public void endTimingProgress() throws IOException
        {
            ensureOpen( true );
            json.writeEndArray();
            json.writeEndObject();
            json.close();
        }

        private static double nanosToMillis( long nanoTime )
        {
            return nanoTime / 1000000.0;
        }

        private void ensureOpen( boolean open ) throws IOException
        {
            if ( (json == null) == open )
            {
                throw new IOException(
                        new IllegalStateException( String.format( "Writing %s started.", open ? "not" : "already" ) ) );
            }
        }
    }
}
