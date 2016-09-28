/*
 * Copyright 2016 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package org.dashbuilder.dataset;

import java.net.URL;

import org.dashbuilder.DataSetCore;
import org.dashbuilder.dataprovider.DataSetProviderRegistry;
import org.dashbuilder.dataprovider.csv.CSVDataSetProvider;
import org.dashbuilder.dataset.def.DataSetDef;
import org.dashbuilder.dataset.def.DataSetDefFactory;
import org.dashbuilder.dataset.def.DataSetDefRegistry;
import org.dashbuilder.dataset.def.DataSetDefRegistryListener;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.dashbuilder.dataset.filter.FilterFactory.*;
import static org.dashbuilder.dataset.group.AggregateFunctionType.*;
import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class CSVDataSetTest {

    public static final String WORLD_POPULATION = "worldPopulation";

    DataSetDefRegistry dataSetDefRegistry;
    DataSetManager dataSetManager;
    DataSetProviderRegistry providerRegistry;

    URL fileURL = Thread.currentThread().getContextClassLoader().getResource("datasets/worldPopulation.csv");
    DataSetDef dataSetDef = DataSetDefFactory.newCSVDataSetDef()
            .uuid(WORLD_POPULATION)
            .fileURL(fileURL.toString())
            .numberPattern("#,###.##")
            .separatorChar(',')
            .quoteChar('\"')
            .escapeChar('\\')
            .buildDef();

    @Before
    public void setUp() {
        dataSetDefRegistry = DataSetCore.get().getDataSetDefRegistry();
        dataSetManager = DataSetCore.get().getDataSetManager();
        assertNotNull(dataSetDefRegistry);
        assertNotNull(dataSetManager);

        // CSV provider must be explicitly registered
        providerRegistry = DataSetCore.get().getDataSetProviderRegistry();
        providerRegistry.registerDataProvider(CSVDataSetProvider.get());
    }

    @Test
    public void testDataSet() {
        dataSetDefRegistry.registerDataSetDef(dataSetDef);

        DataSet dataSet = dataSetManager.getDataSet(WORLD_POPULATION);
        DataSetMetadata dataSetMetadata = dataSetManager.getDataSetMetadata(WORLD_POPULATION);

        assertEquals(dataSet.getRowCount(), 125);
        assertEquals(dataSet.getColumns().size(), 5);

        assertEquals(dataSetMetadata.getUUID(), WORLD_POPULATION);
        assertEquals(dataSetMetadata.getDefinition(), dataSetDef);
        assertEquals(dataSetMetadata.getNumberOfColumns(), 5);
        assertEquals(dataSetMetadata.getNumberOfRows(), 125);
    }

    @Test
    public void testDataSetLookup() {
        dataSetDefRegistry.registerDataSetDef(dataSetDef);

        DataSet dataSet = dataSetManager.lookupDataSet(
                DataSetLookupFactory.newDataSetLookupBuilder()
                .dataset(WORLD_POPULATION)
                .filter("Country", equalsTo("India"))
                .column(COUNT, "result")
                .buildLookup());

        assertEquals(dataSet.getRowCount(), 1);
        assertEquals(dataSet.getColumns().size(), 1);
        assertEquals(dataSet.getValueAt(0, 0), 6d);
    }
}
