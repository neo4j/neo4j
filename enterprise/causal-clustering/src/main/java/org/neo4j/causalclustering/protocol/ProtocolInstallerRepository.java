/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.protocol;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocol;
import org.neo4j.causalclustering.protocol.Protocol.ModifierProtocol;
import org.neo4j.causalclustering.protocol.handshake.ProtocolStack;

import static java.util.Collections.unmodifiableMap;

public class ProtocolInstallerRepository<O extends ProtocolInstaller.Orientation>
{
    private final Map<ApplicationProtocol,ProtocolInstaller.Factory<O,?>> installers;
    private final Map<ModifierProtocol,ModifierProtocolInstaller<O>> modifiers;

    public ProtocolInstallerRepository( Collection<ProtocolInstaller.Factory<O, ?>> installers, Collection<ModifierProtocolInstaller<O>> modifiers )
    {
        Map<ApplicationProtocol,ProtocolInstaller.Factory<O,?>> tempInstallers = new HashMap<>();
        installers.forEach( installer -> addTo( tempInstallers, installer, installer.applicationProtocol() ) );
        this.installers = unmodifiableMap( tempInstallers );

        Map<ModifierProtocol,ModifierProtocolInstaller<O>> tempModifierInstallers = new HashMap<>();
        modifiers.forEach( installer -> installer.protocols().forEach( protocol -> addTo( tempModifierInstallers, installer, protocol ) ) );
        this.modifiers = unmodifiableMap( tempModifierInstallers );
    }

    private <T, P extends Protocol> void addTo( Map<P,T> tempServerMap, T installer, P protocol )
    {
        T old = tempServerMap.put( protocol, installer );
        if ( old != null )
        {
            throw new IllegalArgumentException(
                    String.format( "Duplicate protocol installers for protocol %s: %s and %s", protocol, installer, old )
            );
        }
    }

    public ProtocolInstaller<O> installerFor( ProtocolStack protocolStack )
    {
        ApplicationProtocol applicationProtocol = protocolStack.applicationProtocol();
        ProtocolInstaller.Factory<O,?> protocolInstaller = installers.get( applicationProtocol );

        ensureKnownProtocol( applicationProtocol, protocolInstaller );

        return protocolInstaller.create( getModifierProtocolInstallers( protocolStack ) );
    }

    private List<ModifierProtocolInstaller<O>> getModifierProtocolInstallers( ProtocolStack protocolStack )
    {
        List<ModifierProtocolInstaller<O>> modifierProtocolInstallers = new ArrayList<>();
        for ( ModifierProtocol modifierProtocol : protocolStack.modifierProtocols() )
        {
            ensureNotDuplicate( modifierProtocolInstallers, modifierProtocol );

            ModifierProtocolInstaller<O> protocolInstaller = modifiers.get( modifierProtocol );

            ensureKnownProtocol( modifierProtocol, protocolInstaller );

            modifierProtocolInstallers.add( protocolInstaller );
        }
        return modifierProtocolInstallers;
    }

    private void ensureNotDuplicate( List<ModifierProtocolInstaller<O>> modifierProtocolInstallers, ModifierProtocol modifierProtocol )
    {
        boolean duplicateIdentifier = modifierProtocolInstallers
                .stream()
                .flatMap( modifier -> modifier.protocols().stream() )
                .anyMatch( protocol -> protocol.category().equals( modifierProtocol.category() ) );
        if ( duplicateIdentifier )
        {
            throw new IllegalArgumentException( "Attempted to install multiple versions of " + modifierProtocol.category() );
        }
    }

    private void ensureKnownProtocol( Protocol protocol, Object protocolInstaller )
    {
        if ( protocolInstaller == null )
        {
            throw new IllegalStateException( String.format( "Installer for requested protocol %s does not exist", protocol ) );
        }
    }
}
