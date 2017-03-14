/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.query;

import java.io.Closeable;
import java.io.File;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.Strings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.query.QuerySnapshot;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.FormattedLog;
import org.neo4j.logging.Log;
import org.neo4j.logging.RotatingFileOutputStreamSupplier;

import static java.lang.String.format;
import static org.neo4j.io.file.Files.createOrOpenAsOuputStream;

@Service.Implementation( KernelExtensionFactory.class )
public class QueryLoggerKernelExtension extends KernelExtensionFactory<QueryLoggerKernelExtension.Dependencies>
{
    public interface Dependencies
    {
        FileSystemAbstraction fileSystem();

        Config config();

        Monitors monitoring();

        LogService logger();

        JobScheduler jobScheduler();
    }

    enum Flag
    {
        LOG_PARAMETERS( GraphDatabaseSettings.log_queries_parameter_logging_enabled ),
        LOG_DETAILED_TIME( GraphDatabaseSettings.log_queries_detailed_time_logging_enabled ),
        LOG_ALLOCATED_BYTES( GraphDatabaseSettings.log_queries_allocation_logging_enabled );
        private final Setting<Boolean> setting;

        Flag( Setting<Boolean> setting )
        {
            this.setting = setting;
        }

        boolean enabledIn( Config config )
        {
            return config.get( setting );
        }
    }

    public QueryLoggerKernelExtension()
    {
        super( "query-logging" );
    }

    @Override
    public Lifecycle newInstance( @SuppressWarnings( "unused" ) KernelContext context,
            final Dependencies dependencies ) throws Throwable
    {
        final Config config = dependencies.config();
        boolean queryLogEnabled = config.get( GraphDatabaseSettings.log_queries );
        final File queryLogFile = config.get( GraphDatabaseSettings.log_queries_filename );
        final FileSystemAbstraction fileSystem = dependencies.fileSystem();
        final JobScheduler jobScheduler = dependencies.jobScheduler();
        final Monitors monitoring = dependencies.monitoring();

        if (!queryLogEnabled)
        {
            return createEmptyAdapter();
        }

        return new LifecycleAdapter()
        {
            Closeable closable;

            @Override
            public void init() throws Throwable
            {
                Long thresholdMillis = config.get( GraphDatabaseSettings.log_queries_threshold );
                Long rotationThreshold = config.get( GraphDatabaseSettings.log_queries_rotation_threshold );
                int maxArchives = config.get( GraphDatabaseSettings.log_queries_max_archives );
                EnumSet<Flag> flags = EnumSet.noneOf( Flag.class );
                for ( Flag flag : Flag.values() )
                {
                    if (flag.enabledIn(config))
                    {
                        flags.add( flag );
                    }
                }

                FormattedLog.Builder logBuilder = FormattedLog.withUTCTimeZone();
                Log log;
                if (rotationThreshold == 0)
                {
                    OutputStream logOutputStream = createOrOpenAsOuputStream( fileSystem, queryLogFile, true );
                    log = logBuilder.toOutputStream( logOutputStream );
                    closable = logOutputStream;
                }
                else
                {
                    RotatingFileOutputStreamSupplier
                            rotatingSupplier = new RotatingFileOutputStreamSupplier( fileSystem, queryLogFile,
                            rotationThreshold, 0, maxArchives,
                            jobScheduler.executor( JobScheduler.Groups.queryLogRotation ) );
                    log = logBuilder.toOutputStream( rotatingSupplier );
                    closable = rotatingSupplier;
                }

                QueryLogger logger = new QueryLogger( log, thresholdMillis, flags );
                monitoring.addMonitorListener( logger );
            }

            @Override
            public void shutdown() throws Throwable
            {
                closable.close();
            }
        };
    }

    private Lifecycle createEmptyAdapter()
    {
        return new LifecycleAdapter();
    }

    static class QueryLogger implements QueryExecutionMonitor
    {
        private final Log log;
        private final long thresholdMillis;
        private final boolean logQueryParameters, logDetailedTime, logAllocatedBytes;

        private static final Pattern PASSWORD_PATTERN = Pattern.compile(
                // call signature
                "(?:(?i)call)\\s+dbms(?:\\.security)?\\.change(?:User)?Password\\(" +
                // optional username parameter, in single, double quotes, or parametrized
                "(?:\\s*(?:'(?:(?<=\\\\)'|[^'])*'|\"(?:(?<=\\\\)\"|[^\"])*\"|[^,]*)\\s*,)?" +
                // password parameter, in single, double quotes, or parametrized
                "\\s*('(?:(?<=\\\\)'|[^'])*'|\"(?:(?<=\\\\)\"|[^\"])*\"|\\$\\w*|\\{\\w*\\})\\s*\\)" );

        QueryLogger( Log log, long thresholdMillis, EnumSet<Flag> flags )
        {
            this.log = log;
            this.thresholdMillis = thresholdMillis;
            this.logQueryParameters = flags.contains( Flag.LOG_PARAMETERS );
            this.logDetailedTime = flags.contains( Flag.LOG_DETAILED_TIME );
            this.logAllocatedBytes = flags.contains( Flag.LOG_ALLOCATED_BYTES );
        }

        @Override
        public void startQueryExecution( ExecutingQuery query )
        {
        }

        @Override
        public void endFailure( ExecutingQuery query, Throwable failure )
        {
            log.error( logEntry( query.snapshot() ), failure );
        }

        @Override
        public void endSuccess( ExecutingQuery query )
        {
            QuerySnapshot snapshot = query.snapshot();
            if ( snapshot.elapsedTimeMillis() >= thresholdMillis )
            {
                log.info( logEntry( snapshot ) );
            }
        }

        private String logEntry( QuerySnapshot query )
        {
            String sourceString = query.clientConnection().asConnectionDetails();
            String queryText = query.queryText();
            String metaData = mapAsString( query.transactionAnnotationData() );

            Set<String> passwordParams = new HashSet<>();
            Matcher matcher = PASSWORD_PATTERN.matcher( queryText );

            while ( matcher.find() )
            {
                String password = matcher.group( 1 ).trim();
                if ( password.charAt( 0 ) == '$' )
                {
                    passwordParams.add( password.substring( 1 ) );
                }
                else if ( password.charAt( 0 ) == '{' )
                {
                    passwordParams.add( password.substring( 1, password.length() - 1 ) );
                }
                else
                {
                    queryText = queryText.replace( password, "******" );
                    password = "";
                }
            }

            StringBuilder result = new StringBuilder();
            result.append( query.elapsedTimeMillis() ).append( " ms: " );
            if ( logDetailedTime )
            {
                result.append( "(planning: " ).append( query.planningTimeMillis() );
                Long cpuTime = query.cpuTimeMillis();
                if ( cpuTime != null )
                {
                    result.append( ", cpu: " ).append( cpuTime );
                }
                result.append( ", waiting: " ).append( query.waitTimeMillis() );
                result.append( ") - " );
            }
            if ( logAllocatedBytes )
            {
                Long bytes = query.allocatedBytes();
                if ( bytes != null )
                {
                    result.append( bytes ).append( " B - " );
                }
            }
            result.append( sourceString ).append( " - " ).append( queryText );
            if ( logQueryParameters )
            {
                result.append( " - " ).append( mapAsString( query.queryParameters(), passwordParams ) );
            }
            result.append( " - " ).append( metaData );
            return result.toString();
        }

        private static String mapAsString( Map<String,Object> params )
        {
            return mapAsString( params, Collections.emptySet() );
        }

        private static String mapAsString( Map<String, Object> params, Collection<String> obfuscate )
        {
            if ( params == null )
            {
                return "{}";
            }

            StringBuilder builder = new StringBuilder( "{" );
            String sep = "";
            for ( Map.Entry<String,Object> entry : params.entrySet() )
            {
                builder
                        .append( sep )
                        .append( entry.getKey() )
                        .append( ": " );

                if ( obfuscate.contains( entry.getKey() ) )
                {
                    builder.append( "******" );
                }
                else
                {
                    builder.append( valueToString( entry.getValue() ) );
                }
                sep = ", ";
            }
            builder.append( "}" );

            return builder.toString();
        }

        private static String valueToString( Object value )
        {
            if ( value instanceof Map<?,?> )
            {
                return mapAsString( (Map<String,Object>) value );
            }
            if ( value instanceof String )
            {
                return format( "'%s'", String.valueOf( value ) );
            }
            return Strings.prettyPrint( value );
        }
    }
}
