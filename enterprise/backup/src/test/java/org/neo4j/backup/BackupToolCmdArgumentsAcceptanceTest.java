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
package org.neo4j.backup;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.PrintStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;

import org.neo4j.backup.impl.BackupClient;
import org.neo4j.backup.impl.BackupProtocolService;
import org.neo4j.backup.impl.ConsistencyCheck;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.configuration.Config;

import static java.util.Arrays.asList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

/**
 * This test builds all valid combinations/permutations of args for {@link org.neo4j.backup.BackupTool} and asserts
 * that it can handle those.
 * It tests legacy and modern sets of args in all possible forms: (-option, --option, -option value, -option=value).
 * Legacy is (-from, -to, -verify) and modern is (-host, -port, -to, -verify).
 */
@RunWith( Parameterized.class )
public class BackupToolCmdArgumentsAcceptanceTest
{
    private static final String HOST = "localhost";
    private static final int PORT = 9090;
    private static final Path PATH = Paths.get( "/var/backup/neo4j/" );

    @Parameter( 0 )
    public String argsAsString;
    @Parameter( 1 )
    public boolean expectedVerifyStoreValue;

    @Parameters( name = "args=({0})" )
    public static Iterable<Object[]> data()
    {
        return Iterables.concat(
                allCombinations(
                        stringMap(
                                "host", HOST,
                                "port", String.valueOf( PORT ),
                                "to", PATH.toString()
                        )
                ),
                allCombinations(
                        stringMap(
                                "from", HOST + ":" + PORT,
                                "to", PATH.toString()
                        )
                )
        );
    }

    @Test
    public void shouldInvokeBackupServiceWhenArgsAreValid() throws Exception
    {
        // Given
        String[] args = argsAsString.split( " " );

        BackupProtocolService backupProtocolService = mock( BackupProtocolService.class );
        PrintStream printStream = mock( PrintStream.class );
        BackupTool backupTool = new BackupTool( backupProtocolService, printStream );

        // When
        backupTool.run( args );

        // Then
        verify( backupProtocolService ).doIncrementalBackupOrFallbackToFull(
                eq( HOST ),
                eq( PORT ),
                eq( PATH ),
                expectedVerifyStoreValue ? eq( ConsistencyCheck.FULL ) : eq( ConsistencyCheck.NONE ),
                any( Config.class ),
                eq( BackupClient.BIG_READ_TIMEOUT ),
                eq( false )
        );
    }

    private static List<String> allFlagValues( String name, boolean value )
    {
        return value ?
               asList(
                       "-" + name,
                       "--" + name,
                       "-" + name + "=true",
                       "--" + name + "=true",
                       "-" + name + " true",
                       "--" + name + " true" )
                     :
               asList(
                       "-" + name + "=false",
                       "--" + name + "=false",
                       "-" + name + " false",
                       "--" + name + " false" );
    }

    private static Iterable<Object[]> allCombinations( Map<String,String> optionsMap )
    {
        return Iterables.concat( allCombinations( optionsMap, true ), allCombinations( optionsMap, false ) );
    }

    @SuppressWarnings( "unchecked" )
    private static List<Object[]> allCombinations( Map<String,String> optionsMap, boolean verifyStore )
    {
        List<Object[]> result = new ArrayList<>();

        List<List<String>> optionCombinations = gatherAllOptionCombinations( optionsMap );
        List<String> verifyFlagValues = allFlagValues( "verify", verifyStore );

        for ( List<String> options : optionCombinations )
        {
            for ( String flag : verifyFlagValues )
            {
                List<String> args = join( options, flag );
                List<List<String>> argsPermutations = permutations( args );
                for ( List<String> permutation : argsPermutations )
                {
                    StringBuilder sb = new StringBuilder();
                    for ( String arg : permutation )
                    {
                        sb.append( sb.length() > 0 ? " " : "" ).append( arg );
                    }
                    String argsJoinedAsString = sb.toString();
                    result.add( new Object[]{argsJoinedAsString, verifyStore} );
                }
            }
        }

        return result;
    }

    @SuppressWarnings( "unchecked" )
    private static List<List<String>> gatherAllOptionCombinations( Map<String,String> optionsMap )
    {
        List<List<String>> result = new ArrayList<>();

        Deque<String> stack = new ArrayDeque<>();
        Map.Entry<String,String>[] entries = optionsMap.entrySet().toArray( new Map.Entry[optionsMap.size()] );
        gatherAllOptionCombinations( entries, 0, stack, result );

        return result;
    }

    private static void gatherAllOptionCombinations( Map.Entry<String,String>[] entries, int current,
            Deque<String> stack, List<List<String>> result )
    {
        if ( current == entries.length )
        {
            result.add( new ArrayList<>( stack ) );
        }
        else
        {
            Map.Entry<String,String> entry = entries[current];
            int next = current + 1;

            for ( String arg : possibleArgs( entry.getKey(), entry.getValue() ) )
            {
                stack.push( arg );
                gatherAllOptionCombinations( entries, next, stack, result );
                stack.pop();
            }
        }
    }

    private static List<String> possibleArgs( String key, String value )
    {
        return (value == null) ? asList( "-" + key, "--" + key )
                               : asList(
                                       "-" + key + "=" + value,
                                       "--" + key + "=" + value,
                                       "-" + key + " " + value,
                                       "--" + key + " " + value );
    }

    private static List<List<String>> permutations( List<String> list )
    {
        List<List<String>> result = new ArrayList<>();
        permutations( result, new ArrayList<>(), list );
        return result;
    }

    private static void permutations( List<List<String>> result, List<String> prefix, List<String> list )
    {
        if ( list.isEmpty() )
        {
            result.add( prefix );
        }
        else
        {
            for ( int i = 0; i < list.size(); i++ )
            {
                permutations(
                        result,
                        join( prefix, list.get( i ) ),
                        join( list.subList( 0, i ), list.subList( i + 1, list.size() ) ) );
            }
        }
    }

    private static List<String> join( List<String> list, String element )
    {
        List<String> result = new ArrayList<>( list );
        result.add( element );
        return result;
    }

    private static List<String> join( List<String> list1, List<String> list2 )
    {
        List<String> result = new ArrayList<>( list1.size() + list2.size() );
        result.addAll( list1 );
        result.addAll( list2 );
        return result;
    }
}
