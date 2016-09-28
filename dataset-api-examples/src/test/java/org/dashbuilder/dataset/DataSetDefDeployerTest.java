package org.dashbuilder.dataset;

import java.io.File;
import java.io.FileOutputStream;

import org.dashbuilder.DataSetCore;
import org.dashbuilder.dataset.def.DataSetDef;
import org.dashbuilder.dataset.def.DataSetDefRegistry;
import org.dashbuilder.dataset.def.DataSetDefRegistryListener;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class DataSetDefDeployerTest {

    DataSetDefRegistry dataSetDefRegistry;
    DataSetDefDeployer dataSetDefDeployer;
    String dataSetsDir = Thread.currentThread().getContextClassLoader().getResource("datasets").getFile();

    @Mock
    DataSetDefRegistryListener registryListener;

    @Before
    public void setUp() {
        dataSetDefDeployer = DataSetCore.get().getDataSetDefDeployer();
        dataSetDefRegistry = DataSetCore.get().getDataSetDefRegistry();
        dataSetDefRegistry.addListener(registryListener);
        dataSetDefDeployer.setScanIntervalInMillis(1000);
        assertNotNull(dataSetDefDeployer);
        assertNotNull(dataSetDefRegistry);
    }

    @Test
    public void testDoDeploy() throws Exception {
        assertNull(dataSetDefRegistry.getDataSetDef("expenseReports"));
        dataSetDefDeployer.deploy(dataSetsDir);

        FileOutputStream doDeploy = new FileOutputStream(new File(dataSetsDir, "expenseReports.dset.deploy"));
        doDeploy.write("".getBytes());
        doDeploy.flush();
        doDeploy.close();

        Thread.sleep(2000);
        DataSetDef def = dataSetDefRegistry.getDataSetDef("expenseReports");
        assertNotNull(def);
        verify(registryListener).onDataSetDefRegistered(def);
    }
}
