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
package org.neo4j.tools.console.input;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.util.Listener;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static java.util.Arrays.copyOfRange;

import static org.neo4j.tools.console.input.ConsoleUtil.tokenizeStringWithQuotes;

/**
 * Useful utility which listens to input from console and reacts to each line, i.e. after each ENTER.
 * {@link Command} are added with {@link #add(String, Command)} and are then reacted to in a separate thread,
 * which continuously sits and listens to console input.
 *
 * Use of this class can be a shell-like tool which boots up, instantiates a {@link ConsoleInput},
 * {@link #start() starts it} followed by {@link #waitFor()} which will block until the input stream ends
 * or an exit command is issued.
 *
 * Another use is to instantiate {@link ConsoleInput}, {@link #start() start it} and then move on to do
 * something else entirely. That way the commands added here will be available user input something in
 * the console while all other things are happening. In this case {@link #shutdown()} should be called
 * when the application otherwise shuts down.
 */
public class ConsoleInput extends LifecycleAdapter
{
    private final Map<String,Command> commands = new HashMap<>();
    private Reactor reactor;
    private final BufferedReader inputReader;
    private final Listener<PrintStream> prompt;
    private final PrintStream out;

    public ConsoleInput( InputStream input, PrintStream out, Listener<PrintStream> prompt )
    {
        this.out = out;
        this.prompt = prompt;
        this.inputReader = new BufferedReader( new InputStreamReader( input ) );
    }

    /**
     * Add {@link Command} to be available and executed when input uses it.
     *
     * @param name command name, i.e the first word of the whole command line to listen for.
     * @param command {@link Command} to {@link Command#run(String[]) run} as part of command line
     * starting with {@code name}-
     */
    public void add( String name, Command command )
    {
        commands.put( name, command );
    }

    /**
     * Starts to listen on the input supplied in constructor.
     */
    @Override
    public void start()
    {
        reactor = new Reactor();
        reactor.start();
    }

    /**
     * Waits till input stream ends or exit command is given.
     */
    public void waitFor() throws InterruptedException
    {
        reactor.join();
    }

    /**
     * Shuts down and stops listen on the input.
     */
    @Override
    public void stop() throws InterruptedException
    {
        reactor.halt();
        waitFor();
    }

    /**
     * Prints usage and help for all available commands.
     */
    public void printUsage()
    {
        out.println( "Available commands:" );
        for ( Map.Entry<String, Command> entry : commands.entrySet() )
        {
            out.println( entry.getKey() + ": " + entry.getValue() );
        }
    }

    private class Reactor extends Thread
    {
        private volatile boolean halted;

        public Reactor()
        {
            super( ConsoleInput.class.getSimpleName() + " reactor" );
        }

        void halt()
        {
            halted = true;

            // Interrupt this thread since it's probably sitting listening to input.
            interrupt();
        }

        @Override
        public void run()
        {
            while ( !halted )
            {
                try
                {
                    prompt.receive( out );
                    String commandLine = inputReader.readLine(); // Blocking call
                    if ( commandLine == null )
                    {
                        halted = true;
                        break;
                    }

                    String[] args = tokenizeStringWithQuotes( commandLine );
                    if ( args.length == 0 )
                    {
                        continue;
                    }
                    String commandName = args[0];
                    Command action = commands.get( commandName );
                    if ( action != null )
                    {
                        action.run( copyOfRange( args, 1, args.length ), out );
                    }
                    else
                    {
                        switch ( commandName )
                        {
                            case "help":
                            case "?":
                            case "man":
                                printUsage();
                                break;
                            case "exit":
                                halt();
                                break;
                            default:
                                System.err.println( "Unrecognized command '" + commandName + "'" );
                                break;
                        }
                    }
                }
                catch ( Exception e )
                {
                    e.printStackTrace();
                    // The show must go on
                }
            }
        }
    }
}
