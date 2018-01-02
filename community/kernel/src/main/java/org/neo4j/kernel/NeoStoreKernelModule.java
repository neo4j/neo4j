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
package org.neo4j.kernel;

import org.neo4j.kernel.api.InwardKernel;
import org.neo4j.kernel.impl.api.Kernel;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.transaction.state.NeoStoreFileListing;
import org.neo4j.kernel.impl.util.Dependencies;

class NeoStoreKernelModule
{
    private final TransactionCommitProcess transactionCommitProcess;
    private final Kernel kernel;
    private final KernelTransactions kernelTransactions;
    private final NeoStoreFileListing fileListing;

    NeoStoreKernelModule( TransactionCommitProcess transactionCommitProcess, Kernel kernel,
            KernelTransactions kernelTransactions, NeoStoreFileListing fileListing )
    {
        this.transactionCommitProcess = transactionCommitProcess;
        this.kernel = kernel;
        this.kernelTransactions = kernelTransactions;
        this.fileListing = fileListing;
    }

    public InwardKernel kernelAPI()
    {
        return kernel;
    }

    KernelTransactions kernelTransactions()
    {
        return kernelTransactions;
    }

    NeoStoreFileListing fileListing()
    {
        return fileListing;
    }

    public void satisfyDependencies( Dependencies dependencies )
    {
        dependencies.satisfyDependencies( transactionCommitProcess, kernel, kernelTransactions, fileListing );
    }
}
