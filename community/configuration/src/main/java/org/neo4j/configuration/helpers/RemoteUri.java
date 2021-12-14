package org.neo4j.configuration.helpers;

import java.util.List;
import java.util.Objects;

public class RemoteUri
{
    private final String scheme;
    private final List<SocketAddress> addresses;
    private final String query;

    public RemoteUri( String scheme, List<SocketAddress> addresses, String query )
    {
        this.scheme = scheme;
        this.addresses = addresses;
        this.query = query;
    }

    public String getScheme()
    {
        return scheme;
    }

    public List<SocketAddress> getAddresses()
    {
        return addresses;
    }

    public String getQuery()
    {
        return query;
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
        RemoteUri remoteUri = (RemoteUri) o;
        return Objects.equals( scheme, remoteUri.scheme ) && Objects.equals( addresses, remoteUri.addresses ) && Objects.equals( query, remoteUri.query );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( scheme, addresses, query );
    }

    @Override
    public String toString()
    {
        return "RemoteUri{" + "scheme='" + scheme + '\'' + ", addresses=" + addresses + ", query='" + query + '\'' + '}';
    }
}
