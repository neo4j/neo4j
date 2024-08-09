/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.procedure.builtin;

import static java.util.Collections.singletonList;
import static org.neo4j.internal.helpers.collection.Iterators.asRawIterator;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTString;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.storable.Values.utf8Value;

import org.neo4j.collection.ResourceRawIterator;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.ResourceMonitor;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.VirtualValues;

/**
 * This procedure lists "components" and their version.
 * While components are currently hard-coded, it is intended
 * that this implementation will be replaced once a clean
 * system for component assembly exists where we could dynamically
 * get a list of which components are loaded and what versions of them.
 *
 * This way, it works as a general mechanism into which capabilities
 * a given Neo4j system has, and which version of those components
 * are in use.
 *
 * This would include things like Kernel, Storage Engine, Query Engines,
 * Bolt protocol versions et cetera.
 */
public class ListComponentsProcedure extends CallableProcedure.BasicProcedure {
    private static final TextValue NEO4J_KERNEL = utf8Value("Neo4j Kernel");
    private final TextValue neo4jVersion;
    private final TextValue neo4jEdition;

    public ListComponentsProcedure(QualifiedName name, String neo4jVersion, String neo4jEdition) {
        super(procedureSignature(name)
                .out("name", NTString)
                // Since Bolt, Cypher and other components support multiple versions
                // at the same time, list of versions rather than single version.
                .out("versions", NTList(NTString))
                .out("edition", NTString)
                .mode(Mode.DBMS)
                .description("List DBMS components and their versions.")
                .systemProcedure()
                .build());
        this.neo4jVersion = stringValue(neo4jVersion);
        this.neo4jEdition = stringValue(neo4jEdition);
    }

    @Override
    public ResourceRawIterator<AnyValue[], ProcedureException> apply(
            Context ctx, AnyValue[] input, ResourceMonitor resourceMonitor) throws ProcedureException {
        return asRawIterator(
                singletonList(new AnyValue[] {NEO4J_KERNEL, VirtualValues.list(neo4jVersion), neo4jEdition})
                        .iterator());
    }
}
