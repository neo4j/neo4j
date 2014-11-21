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

    public static final String NO_TOKEN = null;

    /** User name */
    private final String name;

    /** Currently valid user authorization token */
    private final String token;

    /** Privileges this user has */
    private final Privileges privileges;

    /** Authentication credentials used by the built in username/password authentication scheme */
    private final Credentials credentials;

    /** Whether a password change is needed */
    private final boolean passwordChangeRequired;

    public User( String name, Privileges privileges )
    {
        this(name, null, privileges, Credentials.INACCESSIBLE, true );
    }

    public User(String name, String token, Privileges privileges, Credentials credentials, boolean passwordChangeRequired)
    {
        this.name = name;
        this.token = token;
        this.privileges = privileges;
        this.credentials = credentials;
        this.passwordChangeRequired = passwordChangeRequired;
    }

    public String name()
    {
        return name;
    }

    public String token()
    {
        return token;
    }

    public boolean hasToken()
    {
        return token != NO_TOKEN; // Instance equality on purpose.
    }

    public Privileges privileges()
    {
        return privileges;
    }

    public Credentials credentials()
    {
        return credentials;
    }

    public boolean passwordChangeRequired() { return passwordChangeRequired; }

    public boolean tokenEquals( String token )
    {
        return (token == NO_TOKEN && this.token == NO_TOKEN) || (token != NO_TOKEN && token.equals( this.token ));
    }

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
        if ( credentials != null ? !credentials.equals( user.credentials ) : user.credentials != null )
        {
            return false;
        }
        if ( name != null ? !name.equals( user.name ) : user.name != null )
        {
            return false;
        }
        if ( privileges != null ? !privileges.equals( user.privileges ) : user.privileges != null )
        {
            return false;
        }
        if ( token != null ? !token.equals( user.token ) : user.token != null )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (token != null ? token.hashCode() : 0);
        result = 31 * result + (privileges != null ? privileges.hashCode() : 0);
        result = 31 * result + (credentials != null ? credentials.hashCode() : 0);
        result = 31 * result + (passwordChangeRequired ? 1 : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "User{" +
                "name='" + name + '\'' +
                ", token="+ (hasToken() ? "'" + token + "'" : "NO_TOKEN" ) +
                ", privileges=" + privileges +
                ", credentials=" + credentials +
                ", passwordChangeRequired=" + passwordChangeRequired +
                '}';
    }

    public static class Builder
    {
        private String name;
        private String token = NO_TOKEN;
        private Privileges privileges = Privileges.NONE;
        private Credentials credentials = Credentials.INACCESSIBLE;
        private boolean pwdChangeRequired;

        public Builder() { }

        public Builder( User base )
        {
            name = base.name;
            token = base.token;
            privileges = base.privileges;
            credentials = base.credentials;
            pwdChangeRequired = base.passwordChangeRequired;
        }

        public Builder withName( String name ) { this.name = name; return this; }
        public Builder withToken( String token ) { this.token = token; return this; }
        public Builder withPrivileges( Privileges privileges ) { this.privileges = privileges; return this; }
        public Builder withCredentials( Credentials creds ) { this.credentials = creds; return this; }
        public Builder withRequiredPasswordChange( boolean change ) { this.pwdChangeRequired = change; return this; }

        public User build()
        {
            return new User(name, token, privileges, credentials, pwdChangeRequired );
        }
    }
}
