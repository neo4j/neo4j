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
package org.neo4j.kernel.info;

import org.neo4j.logging.Logger;

public interface DiagnosticsProvider
{
    /**
     * Return an identifier for this {@link DiagnosticsProvider}. The result of
     * this method must be stable, i.e. invoking this method multiple times on
     * the same object should return {@link Object#equals(Object) equal}
     * {@link String strings}.
     * 
     * For {@link DiagnosticsProvider}s where there is only one instance of that
     * {@link DiagnosticsProvider}, an implementation like this is would be a
     * sane default, given that the implementing class has a sensible name:
     * 
     * <code><pre>
     * public String getDiagnosticsIdentifier()
     * {
     *     return getClass().getName();
     * }
     * </pre></code>
     * 
     * @return the identifier of this diagnostics provider.
     */
    String getDiagnosticsIdentifier();

    /**
     * Accept a visitor that may or may not be capable of visiting this object.
     * 
     * Typical example:
     * 
     * <code><pre>
     * class OperationalStatistics implements {@link DiagnosticsProvider}
     * {
     *     public void {@link #acceptDiagnosticsVisitor(Object) acceptDiagnosticsVisitor}( {@link Object} visitor )
     *     {
     *         if ( visitor instanceof OperationalStatisticsVisitor )
     *         {
     *              ((OperationalStatisticsVisitor)visitor).visitOperationalStatistics( this );
     *         }
     *     }
     * }
     * 
     * interface OperationalStatisticsVisitor
     * {
     *     void visitOperationalStatistics( OperationalStatistics statistics );
     * }
     * </pre></code>
     * 
     * @param visitor the visitor visiting this {@link DiagnosticsProvider}.
     */
    void acceptDiagnosticsVisitor( Object visitor );

    /**
     * Dump the diagnostic information of this {@link DiagnosticsProvider} for
     * the specified {@link DiagnosticsPhase phase} to the provided
     * {@link Logger logger}.
     * 
     * @param phase the {@link DiagnosticsPhase phase} to dump information for.
     * @param logger the {@link Logger logger} to dump information to.
     */
    void dump( DiagnosticsPhase phase, Logger logger );
}
