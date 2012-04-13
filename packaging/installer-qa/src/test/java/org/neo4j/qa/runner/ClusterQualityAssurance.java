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
package org.neo4j.qa.runner;

import static org.neo4j.vagrant.VMFactory.vm;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Ignore;
import org.junit.internal.AssumptionViolatedException;
import org.junit.internal.runners.model.EachTestNotifier;
import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.Suite;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;
import org.junit.runners.model.TestClass;
import org.neo4j.qa.Neo4jVM;
import org.neo4j.qa.Platforms;
import org.neo4j.qa.SharedConstants;
import org.neo4j.qa.clusterstate.DefaultMachineClusterModel;
import org.neo4j.qa.clusterstate.MachineClusterModel;
import org.neo4j.qa.clusterstate.modifier.DownloadClusterLogs;
import org.neo4j.qa.clusterstate.modifier.RecreateAllMachines;
import org.neo4j.qa.clusterstate.modifier.RollbackAllMachines;
import org.neo4j.qa.clusterstate.verifier.HAClusterDoesNotWorkException;
import org.neo4j.qa.driver.UbuntuDebEnterpriseDriver;
import org.neo4j.qa.driver.UbuntuTarGzEnterpriseDriver;
import org.neo4j.qa.driver.WindowsEnterpriseDriver;
import org.neo4j.qa.machinestate.DefaultMachineModelImpl;
import org.neo4j.vagrant.VirtualMachine;

public class ClusterQualityAssurance extends Suite {

    VagrantIssueIdentifier vagrantIssue = new VagrantIssueIdentifier();

    private class QualityAssuranceTestClassRunner extends
            BlockJUnit4ClassRunner {

        private static final int MAX_HA_FAILS = 3;
        private ClusterTestPermutation testPermutation;

        QualityAssuranceTestClassRunner(Class<?> type,
                ClusterTestPermutation testPermutation)
                throws InitializationError
        {
            super(type);
            this.testPermutation = testPermutation;
        }

        @Override
        public Object createTest() throws Exception
        {
            return getTestClass().getOnlyConstructor().newInstance(
                    testPermutation.getConstructorArgs());
        }

        @Override
        protected void runChild(final FrameworkMethod method,
                RunNotifier notifier)
        {
            Description description = describeChild(method);
            if (method.getAnnotation(Ignore.class) != null)
            {
                notifier.fireTestIgnored(description);
            } else
            {
                Statement statement = methodBlock(method);

                EachTestNotifier eachNotifier = new EachTestNotifier(notifier,
                        description);
                eachNotifier.fireTestStarted();
                try
                {
                    try
                    {
                        runTestCase(statement, testPermutation.getMachineClusterModel(), 0);
                    } catch (AssumptionViolatedException e)
                    {
                        throw e;
                    } catch (Throwable e)
                    {
                        // Destroy vms and retry
                        testPermutation.getMachineClusterModel()
                                .forceApply(new RecreateAllMachines());
                        runTestCase(statement, testPermutation.getMachineClusterModel(), 0);
                    }

                } catch (AssumptionViolatedException e)
                {
                    eachNotifier.addFailedAssumption(e);
                } catch (Throwable e)
                {
                    eachNotifier.addFailure(e);
                } finally
                {

                    try
                    {
                        testPermutation.getMachineClusterModel().forceApply(
                                new DownloadClusterLogs(
                                        SharedConstants.TEST_LOGS_DIR
                                                + testName(method)));
                    } catch (Throwable e)
                    {

                    }

                    eachNotifier.fireTestFinished();
                }

            }
        }
        
        private void runTestCase(Statement statement, MachineClusterModel machineClusterModel, int retryNumber) throws Throwable {
            try
            {
                machineClusterModel.forceApply(
                        new RollbackAllMachines());
                statement.evaluate();
            } catch(HAClusterDoesNotWorkException e) {
                if(retryNumber < MAX_HA_FAILS) {
                    runTestCase(statement, machineClusterModel, ++retryNumber);
                } else {
                    throw e;
                }
            }
        }

        @Override
        protected String getName()
        {
            return String.format("[%s]", testPermutation.getName());
        }

        @Override
        protected String testName(final FrameworkMethod method)
        {
            return String.format("%s[%s]", method.getName(),
                    testPermutation.getName());
        }

        @Override
        protected void validateConstructor(List<Throwable> errors)
        {
            validateOnlyOneConstructor(errors);
        }

        @Override
        protected Statement classBlock(RunNotifier notifier)
        {
            return childrenInvoker(notifier);
        }

        @Override
        protected Annotation[] getRunnerAnnotations()
        {
            return new Annotation[0];
        }
    }

    private class ClusterTestPermutation {

        private String name;
        private MachineClusterModel model;

        public ClusterTestPermutation(String name, MachineClusterModel model)
        {
            this.name = name;
            this.model = model;
        }

        public Object[] getConstructorArgs()
        {
            return new Object[] { model };
        }

        public MachineClusterModel getMachineClusterModel()
        {
            return model;
        }

        public String getName()
        {
            return name;
        }

    }

    private final ArrayList<Runner> runners = new ArrayList<Runner>();

    public ClusterQualityAssurance(Class<?> klass) throws Throwable
    {
        super(klass, Collections.<Runner> emptyList());

        for (ClusterTestPermutation pm : getTestPermutations(getTestClass()))
        {
            runners.add(new QualityAssuranceTestClassRunner(getTestClass()
                    .getJavaClass(), pm));
        }
    }

    @Override
    protected List<Runner> getChildren()
    {
        return runners;
    }

    private List<ClusterTestPermutation> getTestPermutations(TestClass klass)
            throws Throwable
    {
        Map<String, ClusterTestPermutation> platforms = new HashMap<String, ClusterTestPermutation>();

        VirtualMachine win1 = vm(Neo4jVM.WIN_1);
        VirtualMachine win2 = vm(Neo4jVM.WIN_2);
        VirtualMachine win3 = vm(Neo4jVM.WIN_3);

        VirtualMachine ubuntu1 = vm(Neo4jVM.UBUNTU_1);
        VirtualMachine ubuntu2 = vm(Neo4jVM.UBUNTU_2);
        VirtualMachine ubuntu3 = vm(Neo4jVM.UBUNTU_3);

        // Windows
        platforms.put(Platforms.WINDOWS, new ClusterTestPermutation(
                WindowsEnterpriseDriver.class.getSimpleName(),
                new DefaultMachineClusterModel(new DefaultMachineModelImpl(
                        new WindowsEnterpriseDriver(win1)),
                        new DefaultMachineModelImpl(
                                new WindowsEnterpriseDriver(win2)),
                        new DefaultMachineModelImpl(
                                new WindowsEnterpriseDriver(win3)))));

        // Ubuntu, with debian installer
        platforms.put(Platforms.UBUNTU_DEB, new ClusterTestPermutation(
                UbuntuDebEnterpriseDriver.class.getSimpleName(),
                new DefaultMachineClusterModel(new DefaultMachineModelImpl(
                        new UbuntuDebEnterpriseDriver(ubuntu1)),
                        new DefaultMachineModelImpl(
                                new UbuntuDebEnterpriseDriver(ubuntu2)),
                        new DefaultMachineModelImpl(
                                new UbuntuDebEnterpriseDriver(ubuntu3)))));

        // Ubuntu, with tarball packages
        platforms.put(Platforms.UBUNTU_TAR_GZ, new ClusterTestPermutation(
                UbuntuTarGzEnterpriseDriver.class.getSimpleName(),
                new DefaultMachineClusterModel(new DefaultMachineModelImpl(
                        new UbuntuTarGzEnterpriseDriver(ubuntu1)),
                        new DefaultMachineModelImpl(
                                new UbuntuTarGzEnterpriseDriver(ubuntu2)),
                        new DefaultMachineModelImpl(
                                new UbuntuTarGzEnterpriseDriver(ubuntu3)))));

        List<ClusterTestPermutation> permutations = new ArrayList<ClusterTestPermutation>();
        for (String platformKey : Platforms.selectedPlatforms())
        {
            if (platforms.containsKey(platformKey))
            {
                permutations.add(platforms.get(platformKey));
            }
        }

        return permutations;
    }

}
