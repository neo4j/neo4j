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
package org.neo4j.kernel.extension;

import java.io.PrintStream;

import org.neo4j.kernel.impl.util.UnsatisfiedDependencyException;

public class KernelExtensionFailureStrategies
{
    private KernelExtensionFailureStrategies()
    {
    }

    private static FailedToBuildKernelExtensionException wrap( KernelExtensionFactory kernelExtensionFactory, UnsatisfiedDependencyException e )
    {
        return new FailedToBuildKernelExtensionException(
                "Failed to build kernel extension " + kernelExtensionFactory + " due to a missing dependency: " + e.getMessage(), e );
    }

    private static FailedToBuildKernelExtensionException wrap( KernelExtensionFactory kernelExtensionFactory, Throwable e )
    {
        StringBuilder message = new StringBuilder( "Failed to build kernel extension " ).append( kernelExtensionFactory );
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
        return new FailedToBuildKernelExtensionException( message.toString(), e );
    }

    public static KernelExtensionFailureStrategy fail()
    {
        return new KernelExtensionFailureStrategy()
        {
            @Override
            public void handle( KernelExtensionFactory kernelExtensionFactory, UnsatisfiedDependencyException e )
            {
                throw wrap( kernelExtensionFactory, e );
            }

            @Override
            public void handle( KernelExtensionFactory kernelExtensionFactory, Throwable e )
            {
                throw wrap( kernelExtensionFactory, e );
            }
        };
    }

    public static KernelExtensionFailureStrategy ignore()
    {
        return new KernelExtensionFailureStrategy()
        {
            @Override
            public void handle( KernelExtensionFactory kernelExtensionFactory, UnsatisfiedDependencyException e )
            {
                // Just ignore.
            }

            @Override
            public void handle( KernelExtensionFactory kernelExtensionFactory, Throwable e )
            {
                // Just ignore.
            }
        };
    }

    // Perhaps not used, but very useful for debugging kernel extension loading problems
    public static KernelExtensionFailureStrategy print( PrintStream out )
    {
        return new KernelExtensionFailureStrategy()
        {
            @Override
            public void handle( KernelExtensionFactory kernelExtensionFactory, UnsatisfiedDependencyException e )
            {
                wrap( kernelExtensionFactory, e ).printStackTrace( out );
            }

            @Override
            public void handle( KernelExtensionFactory kernelExtensionFactory, Throwable e )
            {
                wrap( kernelExtensionFactory, e ).printStackTrace( out );
            }
        };
    }
}
