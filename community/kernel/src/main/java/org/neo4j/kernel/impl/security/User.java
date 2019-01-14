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
package org.neo4j.kernel.impl.security;

import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Controls authorization and authentication for an individual user.
 */
public class User
{
    /*
      Design note: These instances are shared across threads doing disparate things with them, and there are no access
      locks. Correctness depends on write-time assertions and this class remaining immutable. Please do not introduce
      mutable fields here.
     */
    /** User name */
    private final String name;

    /** Authentication credentials used by the built in username/password authentication scheme */
    private final Credential credential;

    /** Set of flags, eg. password_change_required */
    private final SortedSet<String> flags;

    public static final String PASSWORD_CHANGE_REQUIRED = "password_change_required";

    private User( String name, Credential credential, SortedSet<String> flags )
    {
        this.name = name;
        this.credential = credential;
        this.flags = flags;
    }

    public String name()
    {
        return name;
    }

    public Credential credentials()
    {
        return credential;
    }

    public boolean hasFlag( String flag )
    {
        return flags.contains( flag );
    }

    public Iterable<String> getFlags()
    {
        return flags;
    }

    public boolean passwordChangeRequired()
    {
        return flags.contains( PASSWORD_CHANGE_REQUIRED );
    }

    /** Use this user as a base for a new user object */
    public Builder augment()
    {
        return new Builder( this );
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

        User user = (User) o;

        if ( !flags.equals( user.flags ) )
        {
            return false;
        }
        if ( credential != null ? !credential.equals( user.credential ) : user.credential != null )
        {
            return false;
        }
        return name != null ? name.equals( user.name ) : user.name == null;
    }

    @Override
    public int hashCode()
    {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + ( credential != null ? credential.hashCode() : 0);
        result = 31 * result + ( flags.hashCode() );
        return result;
    }

    @Override
    public String toString()
    {
        return "User{" +
                "name='" + name + '\'' +
                ", credentials=" + credential +
                ", flags=" + flags +
                '}';
    }

    public static class Builder
    {
        private String name;
        private Credential credential = Credential.INACCESSIBLE;
        private TreeSet<String> flags = new TreeSet<>();

        public Builder()
        {
        }

        public Builder( String name, Credential credential )
        {
            this.name = name;
            this.credential = credential;
        }

        public Builder( User base )
        {
            name = base.name;
            credential = base.credential;
            flags.addAll( base.flags );
        }

        public Builder withName( String name )
        {
            this.name = name;
            return this;
        }

        public Builder withCredentials( Credential creds )
        {
            this.credential = creds;
            return this;
        }

        public Builder withFlag( String flag )
        {
            this.flags.add( flag );
            return this;
        }

        public Builder withoutFlag( String flag )
        {
            this.flags.remove( flag );
            return this;
        }

        public Builder withRequiredPasswordChange( boolean change )
        {
            if ( change )
            {
                withFlag( PASSWORD_CHANGE_REQUIRED );
            }
            else
            {
                withoutFlag( PASSWORD_CHANGE_REQUIRED );
            }
            return this;
        }

        public User build()
        {
            return new User( name, credential, flags );
        }
    }
}
