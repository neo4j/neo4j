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

import static org.neo4j.vagrant.UtilMethodsThatAreDuplicatedAThousandTimes.copyFolder;

import java.io.File;

public class VMFactory {

    private static String workingDir = System.getProperty("jagrant.workingdir", System.getProperty("user.home") + "/jagrant/");
    private static String templateDir = System.getProperty("jagrant.templatedir", System.getProperty("user.dir") + "/target/test-classes/vagrant/");
    private static String templateShareDir = System.getProperty("jagrant.templatesharedir", System.getProperty("user.dir") + "/target/test-classes/vagrant/all/");

    public static VirtualMachine vm(VMDefinition config)
    {
        VirtualMachine v;
        File projectFolder = new File(workingDir + config.vmName());
        File templateFolder = new File(templateDir + config.vmName());
        File templateSharedFolder = new File(templateShareDir);
        
        if (!projectFolder.exists())
        {
            projectFolder.mkdirs();
            v = vm(projectFolder, config);
            if(!templateFolder.exists()) {
                System.out.println("No vagrant project for " + config.vmName() + " ("+templateFolder.getAbsolutePath()+"), using vagrant init.");
                v.init(config.box());
            }
        } else
        {
            v = vm(projectFolder, config);
        }
        
        if(templateFolder.exists()) {
            copyFolder(templateFolder, projectFolder);
        }
        copyFolder(templateSharedFolder, projectFolder);
        
        return v;
    }

    private static VirtualMachine vm(File projectFolder, VMDefinition config)
    {
        VirtualMachine v = new VirtualMachine(projectFolder, config);
        v.ensureBoxExists(config.box());
        v.setTransactional(true);
        return v;
    }
    
}
