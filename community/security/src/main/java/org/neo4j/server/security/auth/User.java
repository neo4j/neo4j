/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

    /** Whether a password change is needed */
    private final boolean passwordChangeRequired;

    public User(String name, Credential credential, boolean passwordChangeRequired)
    {
        this.name = name;
        this.credential = credential;
        this.passwordChangeRequired = passwordChangeRequired;
    }

    public String name()
    {
        return name;
    }

    public Credential credentials()
    {
        return credential;
    }

    public boolean passwordChangeRequired() { return passwordChangeRequired; }

    /** Use this user as a base for a new user object */
    public Builder augment() { return new Builder(this); }

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

        if ( passwordChangeRequired != user.passwordChangeRequired )
        {
            return false;
        }
        if ( credential != null ? !credential.equals( user.credential ) : user.credential != null )
        {
            return false;
        }
        if ( name != null ? !name.equals( user.name ) : user.name != null )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + ( credential != null ? credential.hashCode() : 0);
        result = 31 * result + (passwordChangeRequired ? 1 : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "User{" +
                "name='" + name + '\'' +
                ", credentials=" + credential +
                ", passwordChangeRequired=" + passwordChangeRequired +
                '}';
    }

    public static class Builder
    {
        private String name;
        private Credential credential = Credential.INACCESSIBLE;
        private boolean pwdChangeRequired;

        public Builder() { }

        public Builder( User base )
        {
            name = base.name;
            credential = base.credential;
            pwdChangeRequired = base.passwordChangeRequired;
        }

        public Builder withName( String name ) { this.name = name; return this; }
        public Builder withCredentials( Credential creds ) { this.credential = creds; return this; }
        public Builder withRequiredPasswordChange( boolean change ) { this.pwdChangeRequired = change; return this; }

        public User build()
        {
            return new User(name, credential, pwdChangeRequired );
        }
    }
}
