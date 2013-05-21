/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.api;

/**
 * Signals that some constraint has been violated in a {@link KernelAPI kernel interaction},
 * for example a name containing invalid characters or length.
 */
public abstract class DataIntegrityKernelException extends KernelException
{
    public DataIntegrityKernelException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public DataIntegrityKernelException( String message )
    {
        super( message );
    }

    public static class AlreadyIndexedException extends DataIntegrityKernelException
    {
        public AlreadyIndexedException( long labelId, long propertyKey )
        {
            super( "Property " + propertyKey + " is already indexed for label " + labelId + "." );
        }
    }

    public static class AlreadyConstrainedException extends DataIntegrityKernelException
    {
        public AlreadyConstrainedException( long labelId, long propertyKey )
        {
            super( "Property " + propertyKey + " is already indexed for label " + labelId + " through a constraint." );
        }
    }

    public static class IllegalTokenNameException extends DataIntegrityKernelException
    {
        public IllegalTokenNameException( String tokenName )
        {
            super( String.format( "%s is not a valid token name. Only non-null, non-empty strings are allowed.",
                    tokenName != null ? "'" + tokenName + "'" : "Null" ) );
        }
    }

    public static class NoSuchIndexException extends DataIntegrityKernelException
    {
        public NoSuchIndexException( long labelId, long propertyKey )
        {
            super( String.format( "There is no index for property %d for label %d.", propertyKey, labelId )
            );
        }
    }

    public static class TooManyLabelsException extends DataIntegrityKernelException
    {
        public TooManyLabelsException( Throwable cause )
        {
            super( "The maximum number of labels available has been reached, cannot create more labels.", cause );
        }
    }
}
