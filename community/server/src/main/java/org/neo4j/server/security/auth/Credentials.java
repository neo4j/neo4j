/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.server.security.auth;

public class Credentials
{
    /*
      Design note: Concurrent access to this class in common, and there are no access locks. Please do not add mutable
      fields to this class.
     */
    public static final Credentials INACCESSIBLE = new Credentials( "", "SHA-256", "" );

    private final String salt;
    private final String digestAlgo;
    private final String hash;

    public Credentials( String salt, String digestAlgo, String hash )
    {
        this.salt = salt;
        this.digestAlgo = digestAlgo;
        this.hash = hash;
    }

    public String salt()
    {
        return salt;
    }

    public String hash()
    {
        return hash;
    }

    public String digestAlgorithm()
    {
        return digestAlgo;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        Credentials that = (Credentials) o;

        if ( digestAlgo != null ? !digestAlgo.equals( that.digestAlgo ) : that.digestAlgo != null )
        {
            return false;
        }
        if ( hash != null ? !hash.equals( that.hash ) : that.hash != null )
        {
            return false;
        }
        if ( salt != null ? !salt.equals( that.salt ) : that.salt != null )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = salt != null ? salt.hashCode() : 0;
        result = 31 * result + (digestAlgo != null ? digestAlgo.hashCode() : 0);
        result = 31 * result + (hash != null ? hash.hashCode() : 0);
        return result;
    }
}
