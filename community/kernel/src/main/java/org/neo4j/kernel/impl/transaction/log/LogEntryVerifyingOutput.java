/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.transaction.log;


public class LogEntryVerifyingOutput //implements Output
{
//    private final List<LogEntry> otherEntriesToVerify;
//    private final List<Command> commandsToVerify;
//
//    public LogEntryVerifyingOutput( List<LogEntry> entriesToVerify )
//    {
//        commandsToVerify = pullOutCommands( entriesToVerify );
//        otherEntriesToVerify = pullOutOtherEntries( entriesToVerify );
//    }
//
//    private List<LogEntry> pullOutOtherEntries( List<LogEntry> entriesToVerify )
//    {
//        List<LogEntry> otherEntries = new ArrayList<>();
//        for ( LogEntry entry : entriesToVerify )
//        {
//            if ( !(entry instanceof LogEntry.Command) )
//            {
//                otherEntries.add( entry );
//            }
//        }
//        return otherEntries;
//    }
//
//    private List<XaCommand> pullOutCommands( List<LogEntry> entriesToVerify )
//    {
//        List<XaCommand> commands = new ArrayList<>();
//        for ( LogEntry entry : entriesToVerify )
//        {
//            if ( entry instanceof LogEntry.Command )
//            {
//                commands.add( ((LogEntry.Command) entry).getXaCommand() );
//            }
//        }
//        return commands;
//    }
//
//    @Override
//    public void writeStart( Xid xid, int identifier, int masterId, int myId, long startTimestamp,
//            long latestCommittedTxWhenTxStarted ) throws IOException
//    {
//        // TODO Check this too?
//    }
//
//    @Override
//    public void writeCommand( int identifier, XaCommand command ) throws IOException
//    {
//        boolean removed = commandsToVerify.remove( command );
//        assert removed  : "Unexpected command " + command + ". I had these left to verify " + commandsToVerify;
//    }
//
//    @Override
//    public void writePrepare( int identifier, long prepareTimestamp ) throws IOException
//    {
//        // TODO Check this too?
//    }
//
//    @Override
//    public void writeCommit( int identifier, boolean twoPhase, long txId, long commitTimestamp ) throws IOException
//    {
//        // TODO Check this too?
//    }
//
//    @Override
//    public void writeDone( int identifier ) throws IOException
//    {
//        // TODO Check this too?
//    }
//
//    public void done()
//    {
//        assert Collections.emptyList().equals( commandsToVerify ) : "Unexpected commands found" + commandsToVerify;
//    }
}
