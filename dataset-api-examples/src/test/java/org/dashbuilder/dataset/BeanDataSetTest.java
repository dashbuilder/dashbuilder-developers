package org.dashbuilder.dataset;

import org.dashbuilder.DataSetCore;
import org.dashbuilder.dataset.def.DataSetDef;
import org.dashbuilder.dataset.def.DataSetDefFactory;
import org.dashbuilder.dataset.def.DataSetDefRegistry;
import org.dashbuilder.dataset.def.DataSetDefRegistryListener;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.dashbuilder.dataset.group.AggregateFunctionType.*;
import static org.dashbuilder.dataset.filter.FilterFactory.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class BeanDataSetTest {

    public static final String SEQUENCE = "sequence";

    DataSetDefRegistry dataSetDefRegistry;
    DataSetManager dataSetManager;

    DataSetDef dataSetDef = DataSetDefFactory.newBeanDataSetDef()
            .uuid(SEQUENCE)
            .generatorClass("org.dashbuilder.dataset.SampleBeanDataSetGenerator")
            .generatorParam("from", "1")
            .generatorParam("to", "100")
            .buildDef();

    @Mock
    DataSetDefRegistryListener registryListener;

    @Before
    public void setUp() {
        dataSetDefRegistry = DataSetCore.get().getDataSetDefRegistry();
        dataSetManager = DataSetCore.get().getDataSetManager();

        assertNotNull(dataSetDefRegistry);
        assertNotNull(dataSetManager);
        dataSetDefRegistry.addListener(registryListener);
    }

    @Test
    public void testRegisterLifecycle() {
        dataSetDefRegistry.registerDataSetDef(dataSetDef);
        verify(registryListener).onDataSetDefRegistered(dataSetDef);

        DataSetDef modifiedDef = DataSetDefFactory
                .newBeanDataSetDef()
                .uuid(SEQUENCE)
                .buildDef();

        dataSetDefRegistry.registerDataSetDef(modifiedDef);
        verify(registryListener).onDataSetDefStale(dataSetDef);
        verify(registryListener).onDataSetDefModified(dataSetDef, modifiedDef);

        dataSetDefRegistry.removeDataSetDef(SEQUENCE);
        verify(registryListener).onDataSetDefRemoved(modifiedDef);
    }

    @Test
    public void testDataSet() {
        dataSetDefRegistry.registerDataSetDef(dataSetDef);

        DataSet dataSet = dataSetManager.getDataSet(SEQUENCE);
        DataSetMetadata dataSetMetadata = dataSetManager.getDataSetMetadata(SEQUENCE);

        assertEquals(dataSet.getRowCount(), 99);
        assertEquals(dataSet.getColumns().size(), 1);
        assertEquals(dataSet.getRowCount(), 99);

        assertEquals(dataSetMetadata.getUUID(), SEQUENCE);
        assertEquals(dataSetMetadata.getDefinition(), dataSetDef);
        assertEquals(dataSetMetadata.getNumberOfColumns(), 1);
        assertEquals(dataSetMetadata.getNumberOfRows(), 99);
        assertEquals(dataSetMetadata.getColumnId(0), "index");
        assertEquals(dataSetMetadata.getColumnType(0), ColumnType.NUMBER);
    }

    @Test
    public void testDataSetLookup() {
        dataSetDefRegistry.registerDataSetDef(dataSetDef);

        DataSet dataSet = dataSetManager.lookupDataSet(
                DataSetLookupFactory.newDataSetLookupBuilder()
                .dataset(SEQUENCE)
                .filter("index", greaterThan(90))
                .column(COUNT, "result")
                .buildLookup());

        assertEquals(dataSet.getRowCount(), 1);
        assertEquals(dataSet.getColumns().size(), 1);
        assertEquals(dataSet.getValueAt(0, 0), 9d);
    }
}
