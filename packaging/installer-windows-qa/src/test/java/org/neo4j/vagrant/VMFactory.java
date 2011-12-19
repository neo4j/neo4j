/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.vagrant;

import static org.neo4j.vagrant.FileUtilMethodsThatAreDuplicatedAThousandTimes.copyFolder;

import java.io.File;

public class VMFactory {

    private static String workingDir = System.getProperty("jagrant.workingdir", System.getProperty("user.home") + "/jagrant/");
    private static String templateDir = System.getProperty("jagrant.templatedir", System.getProperty("user.dir") + "/target/test-classes/vagrant/");
    

    public static VirtualMachine vm(Box box, String name, String templateProject)
    {
        VirtualMachine v;
        File projectFolder = new File(workingDir + name);
        File templateFolder = new File(templateDir + templateProject);
        
        System.out.println(projectFolder.getAbsolutePath());
        System.out.println(templateFolder.getAbsolutePath());
        
        if (!projectFolder.exists())
        {
            projectFolder.mkdirs();
            v = vm(box, projectFolder);
            if(!templateFolder.exists()) {
                System.out.println("No vagrant project for " + templateProject + " ("+templateFolder.getAbsolutePath()+"), using vagrant init.");
                v.init(box);
            } else {
                copyFolder(templateFolder, projectFolder);
            }
        } else
        {
            v = vm(box, projectFolder);
        }
        return v;
    }

    public static VirtualMachine vm(Box box, File projectFolder)
    {
        VirtualMachine v = new VirtualMachine(projectFolder, true);
        v.ensureBoxExists(box);
        return v;
    }

    protected static VirtualMachine vm(Box baseBox, String name)
    {
        return vm(baseBox, name, baseBox.getName() + "-vanilla");
    }
    
}
