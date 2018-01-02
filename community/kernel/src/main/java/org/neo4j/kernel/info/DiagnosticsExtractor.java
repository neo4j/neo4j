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

/**
 * An object that can extract diagnostics information from a source of a
 * specific type.
 * 
 * @author Tobias Lindaaker <tobias.lindaaker@neotechnology.com>
 * 
 * @param <T> the type of the source to extract diagnostics information from.
 */
public interface DiagnosticsExtractor<T>
{
    /**
     * A {@link DiagnosticsExtractor} capable of
     * {@link DiagnosticsProvider#acceptDiagnosticsVisitor(Object) accepting
     * visitors}.
     * 
     * @author Tobias Lindaaker <tobias.lindaaker@neotechnology.com>
     * 
     * @param <T> the type of the source to extract diagnostics information
     *            from.
     */
    interface VisitableDiagnostics<T> extends DiagnosticsExtractor<T>
    {
        /**
         * Accept a visitor that may or may not be capable of visiting this
         * object.
         * 
         * @see DiagnosticsProvider#acceptDiagnosticsVisitor(Object)
         * @param source the source to get diagnostics information from.
         * @param visitor the visitor visiting the diagnostics information.
         */
        void dispatchDiagnosticsVisitor( T source, Object visitor );
    }

    /**
     * Dump the diagnostic information of the specified source for the specified
     * {@link DiagnosticsPhase phase} to the provided {@link Logger logger}.
     * 
     * @see DiagnosticsProvider#dump(DiagnosticsPhase, Logger)
     * @param source the source to get diagnostics information from.
     * @param phase the {@link DiagnosticsPhase phase} to dump information for.
     * @param logger the {@link Logger logger} to dump information to.
     */
    void dumpDiagnostics( T source, DiagnosticsPhase phase, Logger logger );
}
