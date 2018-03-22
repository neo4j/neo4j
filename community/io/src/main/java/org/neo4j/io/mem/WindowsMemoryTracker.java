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
package org.neo4j.io.mem;

import com.sun.jna.Native;
import com.sun.jna.Structure;
import com.sun.jna.platform.win32.Kernel32;
import com.sun.jna.platform.win32.WinDef.DWORD;
import com.sun.jna.platform.win32.WinNT;
import org.apache.commons.lang3.SystemUtils;

import java.util.Arrays;

import static com.sun.jna.platform.win32.BaseTSD.SIZE_T;

public class WindowsMemoryTracker
{
    static
    {
        try
        {
            if ( SystemUtils.IS_OS_WINDOWS )
            {
                Native.register( "psapi" );
            }
        }
        catch ( Throwable t )
        {
            t.printStackTrace();
        }
    }

    public static native boolean GetProcessMemoryInfo( WinNT.HANDLE hProcess, PROCESS_MEMORY_COUNTERS_EX process_memory_counters_ex,
            long processMemoryCountersSize );

    public static PROCESS_MEMORY_COUNTERS_EX getMemoryCounters()
    {
        PROCESS_MEMORY_COUNTERS_EX memoryCountersEx = new PROCESS_MEMORY_COUNTERS_EX();
        if ( SystemUtils.IS_OS_WINDOWS && GetProcessMemoryInfo( Kernel32.INSTANCE.GetCurrentProcess(), memoryCountersEx, memoryCountersEx.size() ) )
        {
            return memoryCountersEx;
        }
        return null;
    }

    public static class PROCESS_MEMORY_COUNTERS_EX extends Structure
    {
        public DWORD cb;
        public DWORD PageFaultCount;
        public SIZE_T PeakWorkingSetSize;
        public SIZE_T WorkingSetSize;
        public SIZE_T QuotaPeakPagedPoolUsage;
        public SIZE_T QuotaPagedPoolUsage;
        public SIZE_T QuotaPeakNonPagedPoolUsage;
        public SIZE_T QuotaNonPagedPoolUsage;
        public SIZE_T PagefileUsage;
        public SIZE_T PeakPagefileUsage;
        public SIZE_T PrivateUsage;

        public PROCESS_MEMORY_COUNTERS_EX()
        {
        }

        @Override
        protected java.util.List getFieldOrder()
        {
            return Arrays.asList( new String[]{"cb", "PageFaultCount", "PeakWorkingSetSize", "WorkingSetSize", "QuotaPeakPagedPoolUsage", "QuotaPagedPoolUsage",
                    "QuotaPeakNonPagedPoolUsage", "QuotaNonPagedPoolUsage", "PagefileUsage", "PeakPagefileUsage", "PrivateUsage"} );
        }

        @Override
        public String toString()
        {
            return "ProcessMemory:{pageFaults:" + PageFaultCount.longValue() + "," +
                    "PeakWorkingSet:" + PeakWorkingSetSize.longValue() + "b," +
                    "CurrentWorkingSet:" + WorkingSetSize.longValue() + "b," +
                    "PeakPagePoolUsage:" + QuotaPeakPagedPoolUsage.longValue() + "b," +
                    "CurrentPagePoolUsage:" + QuotaPagedPoolUsage.longValue() + "b," +
                    "PeakNonPagePoolUsage:" + QuotaPeakNonPagedPoolUsage.longValue() + "b," +
                    "CurrentNonPagePoolUsage:" + QuotaNonPagedPoolUsage.longValue() + "b," +
                    "CommitCharge:" + PagefileUsage.longValue() + "b," +
                    "PeakCommitCharge:" + PeakPagefileUsage.longValue() + "b," +
                    "PrivateUsage:" + PrivateUsage.longValue() + "b " +
                    "}";
        }
    }
}
