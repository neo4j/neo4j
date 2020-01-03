/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.extension;

import java.io.PrintStream;

import org.neo4j.exceptions.UnsatisfiedDependencyException;

public class ExtensionFailureStrategies
{
    private ExtensionFailureStrategies()
    {
    }

    private static FailedToBuildExtensionException wrap( ExtensionFactory extensionFactory, UnsatisfiedDependencyException e )
    {
        return new FailedToBuildExtensionException(
                "Failed to build kernel extension " + extensionFactory + " due to a missing dependency: " + e.getMessage(), e );
    }

    private static FailedToBuildExtensionException wrap( ExtensionFactory extensionFactory, Throwable e )
    {
        StringBuilder message = new StringBuilder( "Failed to build kernel extension " ).append( extensionFactory );
        if ( e instanceof LinkageError || e instanceof ReflectiveOperationException )
        {
            if ( e instanceof LinkageError )
            {
                message.append( " because it is compiled with a reference to a class, method, or field, that is not in the class path: " );
            }
            else
            {
                message.append( " because it a reflective access to a class, method, or field, that is not in the class path: " );
            }
            message.append( '\'' ).append( e.getMessage() ).append( '\'' );
            message.append( ". The most common cause of this problem, is that Neo4j has been upgraded without also upgrading all" );
            message.append( "installed extensions, such as APOC. " );
            message.append( "Make sure that all of your extensions are build against your specific version of Neo4j." );
        }
        else
        {
            message.append( " because of an unanticipated error: '" ).append( e.getMessage() ).append( "'." );
        }
        return new FailedToBuildExtensionException( message.toString(), e );
    }

    public static ExtensionFailureStrategy fail()
    {
        return new ExtensionFailureStrategy()
        {
            @Override
            public void handle( ExtensionFactory extensionFactory, UnsatisfiedDependencyException e )
            {
                throw wrap( extensionFactory, e );
            }

            @Override
            public void handle( ExtensionFactory extensionFactory, Throwable e )
            {
                throw wrap( extensionFactory, e );
            }
        };
    }

    public static ExtensionFailureStrategy ignore()
    {
        return new ExtensionFailureStrategy()
        {
            @Override
            public void handle( ExtensionFactory extensionFactory, UnsatisfiedDependencyException e )
            {
                // Just ignore.
            }

            @Override
            public void handle( ExtensionFactory extensionFactory, Throwable e )
            {
                // Just ignore.
            }
        };
    }

    // Perhaps not used, but very useful for debugging kernel extension loading problems
    public static ExtensionFailureStrategy print( PrintStream out )
    {
        return new ExtensionFailureStrategy()
        {
            @Override
            public void handle( ExtensionFactory extensionFactory, UnsatisfiedDependencyException e )
            {
                wrap( extensionFactory, e ).printStackTrace( out );
            }

            @Override
            public void handle( ExtensionFactory extensionFactory, Throwable e )
            {
                wrap( extensionFactory, e ).printStackTrace( out );
            }
        };
    }
}
