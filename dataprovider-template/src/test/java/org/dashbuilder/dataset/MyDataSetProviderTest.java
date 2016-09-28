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

import org.dashbuilder.DataSetCore;
import org.dashbuilder.dataprovider.DataSetProviderRegistry;
import org.dashbuilder.dataprovider.DataSetProviderType;
import org.dashbuilder.dataprovider.MyDataSetProvider;
import org.dashbuilder.dataset.def.DataSetDef;
import org.dashbuilder.dataset.def.DataSetDefRegistry;
import org.dashbuilder.dataset.json.DataSetDefJSONMarshaller;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class MyDataSetProviderTest {

    MyDataSetProvider customProvider = spy(new MyDataSetProvider());
    DataSetProviderRegistry providerRegistry = DataSetCore.get().getDataSetProviderRegistry();
    DataSetDefRegistry dataSetDefRegistry = DataSetCore.get().getDataSetDefRegistry();
    DataSetManager dataSetManager = DataSetCore.get().getDataSetManager();
    DataSetDefJSONMarshaller jsonMarshaller = DataSetCore.get().getDataSetDefJSONMarshaller();
    DataSetDef customDef = new DataSetDef();

    @Before
    public void setUp() {
        providerRegistry.registerDataProvider(customProvider);

        customDef.setProvider(customProvider.getType());
        customDef.setProperty("prop1", "hello");
        customDef.setProperty("prop2", "world");
        customDef.setUUID("test");
        dataSetDefRegistry.registerDataSetDef(customDef);
    }

    @Test
    public void testRegistry() throws Exception {
        DataSetProviderType type = providerRegistry.getProviderTypeByName("MY");
        assertEquals(customProvider.getType().getName(), "MY");
        assertEquals(type, customProvider.getType());
    }

    @Test
    public void testJson() throws Exception {
        String json = jsonMarshaller.toJsonString(customDef);
        assertTrue(json.contains("\"prop1\": \"hello\""));
        assertTrue(json.contains("\"prop2\": \"world\""));
    }

    @Test
    public void testMetadata() throws Exception {
        DataSetMetadata medatata = dataSetManager.getDataSetMetadata("test");

        verify(customProvider).getDataSetMetadata(customDef);
        assertEquals(medatata.getNumberOfColumns(), 2);
        assertEquals(medatata.getColumnId(0), "prop1");
        assertEquals(medatata.getColumnId(1), "prop2");
    }

    @Test
    public void testLookup() throws Exception {
        DataSetLookup lookup = DataSetLookupFactory
                .newDataSetLookupBuilder().dataset("test")
                .buildLookup();

        DataSet dataSet = dataSetManager.lookupDataSet(lookup);

        verify(customProvider).lookupDataSet(customDef, lookup);
        assertEquals(dataSet.getRowCount(), 1);
        assertEquals(dataSet.getValueAt(0, 0), "hello");
        assertEquals(dataSet.getValueAt(0, 1), "world");
   }
}