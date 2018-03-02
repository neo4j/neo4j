/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.protocol;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ProtocolInstallerRepository<O extends ProtocolInstaller.Orientation>
{
    private final Map<Protocol,ProtocolInstaller<O>> installers;

    public ProtocolInstallerRepository( Collection<ProtocolInstaller<O>> installers )
    {
        Map<Protocol,ProtocolInstaller<O>> tempInstallers = new HashMap<>();

        installers.forEach( installer -> addTo( tempInstallers, installer ) );

        this.installers = Collections.unmodifiableMap( tempInstallers );
    }

    private void addTo( Map<Protocol,ProtocolInstaller<O>> tempServerMap, ProtocolInstaller<O> installer )
    {
        Protocol protocol = installer.protocol();
        ProtocolInstaller old = tempServerMap.put( protocol, installer );
        if ( old != null )
        {
            throw new IllegalArgumentException(
                    String.format( "Duplicate protocol installers for protocol %s", protocol )
            );
        }
    }

    public ProtocolInstaller installerFor( Protocol protocol )
    {
        ProtocolInstaller protocolInstaller = installers.get( protocol );
        if ( protocolInstaller == null )
        {
            throw new IllegalStateException( String.format( "Installer for requested protocol %s does not exist", protocol ) );
        }
        else
        {
            return protocolInstaller;
        }
    }
}
