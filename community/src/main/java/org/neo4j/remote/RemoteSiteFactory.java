/*
 * Copyright 2008 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.remote;

import java.net.URI;
import java.util.Arrays;


/**
 * An object that specifies which URIs a specific {@link RemoteSite} can handle,
 * and can create instances of that {@link RemoteSite}. The contract for
 * extending this class is that each instance of the same extending class should
 * behave in the same way, they will be treated as equal by the framework.
 * @author Tobias Ivarsson
 */
public abstract class RemoteSiteFactory
{
	final String[] protocols;

	/**
	 * Create a new {@link RemoteSiteFactory} that supports the protocols
	 * specified by the supplied protocol schema identifiers. A protocol scheme
	 * identifier consists of alphanumeric characters or any of the characters
	 * "-.+". The first character of the scheme identifier is always an
	 * alphabetic character.
	 * @see java.net.URI#getScheme()
	 * @param protocols
	 *            all the protocol scheme identifiers that this remote site
	 *            supports.
	 * @throws IllegalArgumentException
	 *             if no protocols where specified.
	 */
	protected RemoteSiteFactory( String... protocols )
	{
		if ( protocols == null || protocols.length == 0 )
		{
			throw new IllegalArgumentException( "No protocols specified." );
		}
		this.protocols = protocols.clone();
	}

	/**
	 * Create a RemoteSite that connects to a remote Neo resource on the
	 * specified URI. If login is required the supplied user name and password
	 * are used.
	 * @param resourceUri
	 *            the URI of the remote Neo resource.
	 * @return an instance of the specific {@link RemoteSite}, that connects to
	 *         the specified URI.
	 */
	protected abstract RemoteSite create( URI resourceUri );

	/**
	 * Determine if this remote site can handle the specified URI. In it's most
	 * simple implementation this method can just check if the URI starts with a
	 * supported protocol scheme identifier or even simply always return
	 * <code>true</code>. A more advanced implementation might connect to the
	 * resource on the specified URI to determine if it communicates in a way
	 * supported by this remote site. A well behaving implementation returns
	 * <code>false</code> instead of throwing an exception.
	 * @param resourceUri
	 *            the URI of the remote Neo resource.
	 * @return <code>true</code> if this site can handle the specified URI.
	 */
	protected abstract boolean handlesUri( URI resourceUri );

	@Override
	public final boolean equals( Object other )
	{
		return other != null && getClass().equals( other.getClass() );
	}

	@Override
	public final int hashCode()
	{
		return getClass().hashCode();
	}

	@Override
	public final String toString()
	{
		return "RemoteSiteFactory(name=" + getClass().getSimpleName()
		    + ", protocols=" + Arrays.toString( protocols ) + ")";
	}
}
