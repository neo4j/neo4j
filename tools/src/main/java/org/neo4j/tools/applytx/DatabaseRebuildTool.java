/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.tools.applytx;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.ConsistencyCheckService.Result;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.Provider;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.util.Listener;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.tools.console.input.ArgsCommand;
import org.neo4j.tools.console.input.ConsoleInput;

import static org.neo4j.tools.console.input.ConsoleUtil.NO_PROMPT;
import static org.neo4j.tools.console.input.ConsoleUtil.oneCommand;
import static org.neo4j.tools.console.input.ConsoleUtil.staticPrompt;

/**
 * Tool for rebuilding database from transaction logs onto a new store. Transaction can be applied interactively,
 * i.e. applied up to any transaction id and consistency checked at any point. Also there are other utilities,
 * such as printing record structure at any point as well.
 *
 * Running this tool will eventually go into a mode where it's awaiting user input, so typed commands
 * will control what happens. Type help to get more information.
 */
public class DatabaseRebuildTool
{
    private final InputStream in;
    private final PrintStream out;
    private final PrintStream err;

    public DatabaseRebuildTool()
    {
        this( System.in, System.out, System.err );
    }

    public DatabaseRebuildTool( InputStream in, PrintStream out, PrintStream err )
    {
        this.in = in;
        this.out = out;
        this.err = err;
    }

    public static void main( String[] arguments ) throws Exception
    {
        new DatabaseRebuildTool().run( arguments );
    }

    public void run( String... arguments ) throws Exception
    {
        if ( arguments.length == 0 )
        {
            System.err.println( "Tool for rebuilding database from transaction logs onto a new store" );
            System.err.println( "Example: dbrebuild --from path/to/some.db --to path/to/new.db apply next" );
            System.err.println( "         dbrebuild --from path/to/some.db --to path/to/new.db -i" );
            System.err.println( "          --from : which db to use as source for reading transactions" );
            System.err.println( "            --to : where to build the new db" );
            System.err.println( "  --overwrite-to : always starts from empty 'to' db" );
            System.err.println( "              -i : interactive mode (enter a shell)" );
            return;
        }

        Args args = Args.withFlags( "i", "overwrite-to" ).parse( arguments );
        File fromPath = getFrom( args );
        File toPath = getTo( args );
        GraphDatabaseBuilder dbBuilder = newDbBuilder( toPath, args );
        boolean interactive = args.getBoolean( "i" );
        if ( interactive && !args.orphans().isEmpty() )
        {
            throw new IllegalArgumentException( "No additional commands allowed in interactive mode" );
        }

        @SuppressWarnings( "resource" )
        InputStream input = interactive ? in : oneCommand( args.orphansAsArray() );
        LifeSupport life = new LifeSupport();
        ConsoleInput consoleInput = console( fromPath, dbBuilder, input,
                interactive ? staticPrompt( "# " ) : NO_PROMPT, life );
        life.start();
        try
        {
            consoleInput.waitFor();
        }
        finally
        {
            life.shutdown();
        }
    }

    private File getTo( Args args ) throws IOException
    {
        String to = args.get( "to" );
        if ( to == null )
        {
            to = "target/db-from-apply-txs";
            err.println( "Defaulting --to to " + to );
        }
        File toPath = new File( to );
        if ( args.getBoolean( "overwrite-to" ) )
        {
            FileUtils.deleteRecursively( toPath );
        }
        return toPath;
    }

    private File getFrom( Args args )
    {
        String from = args.get( "from" );
        if ( from == null )
        {
            throw new IllegalArgumentException( "Missing --from i.e. from where to read transaction logs" );
        }
        return new File( from );
    }

    private static GraphDatabaseBuilder newDbBuilder( File path, Args args )
    {
        GraphDatabaseBuilder builder = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( path.getAbsolutePath() );
        for ( Map.Entry<String, String> entry : args.asMap().entrySet() )
        {
            if ( entry.getKey().startsWith( "D" ) )
            {
                String key = entry.getKey().substring( 1 );
                String value = entry.getValue();
                builder = builder.setConfig( key, value );
            }
        }
        return builder;
    }

    private static class Store
    {
        private final GraphDatabaseAPI db;
        private final StoreAccess access;
        private final File storeDir;

        public Store( GraphDatabaseBuilder dbBuilder )
        {
            this.db = (GraphDatabaseAPI) dbBuilder.newGraphDatabase();
            this.access = new StoreAccess( db ).initialize();
            this.storeDir = new File( db.getStoreDir() );
        }

        public void shutdown()
        {
            db.shutdown();
        }
    }

    private ConsoleInput console( final File fromPath, final GraphDatabaseBuilder dbBuilder,
            InputStream in, Listener<PrintStream> prompt, LifeSupport life )
            throws Exception
    {
        // We must have this indirection here since in order to perform CC (one of the commands) we must shut down
        // the database and let CC instantiate its own to run on. After that completes the db
        // should be restored. The commands has references to providers of things to accommodate for this.
        final AtomicReference<Store> store =
                new AtomicReference<>( new Store( dbBuilder ) );
        final Provider<StoreAccess> storeAccess = new Provider<StoreAccess>()
        {
            @Override
            public StoreAccess instance()
            {
                return store.get().access;
            }
        };
        final Provider<GraphDatabaseAPI> dbAccess = new Provider<GraphDatabaseAPI>()
        {
            @Override
            public GraphDatabaseAPI instance()
            {
                return store.get().db;
            }
        };

        ConsoleInput consoleInput = life.add( new ConsoleInput( in, out, prompt ) );
        consoleInput.add( "apply", new ApplyTransactionsCommand( fromPath, dbAccess ) );
        consoleInput.add( DumpRecordsCommand.NAME, new DumpRecordsCommand( storeAccess ) );
        consoleInput.add( "cc", new ArgsCommand()
        {
            @Override
            public void run( Args action, PrintStream out ) throws Exception
            {
                File storeDir = store.get().storeDir;
                store.get().shutdown();
                try
                {
                    Result result = new ConsistencyCheckService().runFullConsistencyCheck( storeDir,
                            new Config(), ProgressMonitorFactory.textual( out ),
                            FormattedLogProvider.toOutputStream( System.out ), false );
                    out.println( (result.isSuccessful() ? "consistent" : "INCONSISTENT") );
                }
                finally
                {
                    store.set( new Store( dbBuilder ) );
                }
            }

            @Override
            public String toString()
            {
                return "Runs consistency check on the database for data that has been applied up to this point";
            }
        } );
        life.add( new LifecycleAdapter()
        {
            @Override
            public void shutdown()
            {
                store.get().shutdown();
            }
        } );
        return consoleInput;
    }
}
