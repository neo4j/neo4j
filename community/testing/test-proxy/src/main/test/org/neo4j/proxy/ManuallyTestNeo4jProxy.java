/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.proxy;

import java.io.IOException;
import java.util.Scanner;

/**
 * To test manually proxy create a server and client socket with nc tool
 * */
public class ManuallyTestNeo4jProxy
{
    public static void main( String[] args ) throws IOException
    {
        Scanner scanner = new Scanner( System.in );
        var proxy = TcpCrusherProxy.builder().build();
        var proxyConfig = proxy.getProxyConfig();
        System.out.println( proxyConfig );
        while ( true )
        {
            var line = scanner.nextLine();
            if ( line.equals( "o" ) )
            {
                System.out.println( "startAcceptingConnections" );
                proxy.startAcceptingConnections();
            }
            else if ( line.equals( "c" ) )
            {
                System.out.println( "stopAcceptingConnections" );
                proxy.stopAcceptingConnections();
            }
            else if ( line.equals( "f" ) )
            {
                System.out.println( "freezeConnection" );
                proxy.freezeConnection();
            }
            else if ( line.equals( "u" ) )
            {
                System.out.println( "unfreezeConnection" );
                proxy.unfreezeConnection();
            }
            else {
                System.out.println("error");
            }
        }
    }
}
