/*
 * Copyright 2016 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dashbuilder.dataprovider;

import org.dashbuilder.dataset.DataSet;
import org.dashbuilder.dataset.DataSetFactory;
import org.dashbuilder.dataset.DataSetLookup;
import org.dashbuilder.dataset.DataSetMetadata;
import org.dashbuilder.dataset.def.DataSetDef;
import org.dashbuilder.dataset.def.DataSetDefRegistryListener;

/**
 * My data set provider implementation
 */
public class MyDataSetProvider implements DataSetProvider, DataSetDefRegistryListener {

    DataSetProviderType TYPE = new DefaultProviderType("MY");

    public DataSetProviderType getType() {
        return TYPE;
    }

    /**
     * This method is used to get the metadata related with a given the data set definition.
     */
    public DataSetMetadata getDataSetMetadata(DataSetDef def) throws Exception {

        // Add your own logic...

        // As a dummy implementation we are retrieving the entire dataset and getting its metadata. This is not the
        // recommended approach tough, specially in cases where the dataset is big. The recommended approach is to
        // ask the external storage with the minimum required calls in order to figure out all the required metadata.

        DataSet dataSet = lookupDataSet(def, null);
        return dataSet == null ? null : dataSet.getMetadata();
    }

    /**
     * This method contains the required implementation to get a {@link DataSet} from a given {@link DataSetLookup}
     * call. For example, if your provider reads data from an SQL storage, then an SQL query must be generated from the
     * {@link DataSetLookup} instance. Once the query is executed, the results must be transformed back into a
     * {@link DataSet} instance.
     *
     * @param def The data set definition lookup request
     * @param lookup The lookup request over the data set. If null then return the data set as is.
     * @return A {@link DataSet} containing the results
     */
    public DataSet lookupDataSet(DataSetDef def, DataSetLookup lookup) throws Exception {

        // Add your own logic...

        // As a dummy implementation a 2-column dataset is generated.

        String prop1 = def.getProperty("prop1");
        String prop2 = def.getProperty("prop2");

        return DataSetFactory.newDataSetBuilder()
                .uuid(def.getUUID())
                .label("prop1").label("prop2")
                .row(prop1, prop2)
                .buildDataSet();
    }

    /**
     * This method is very useful in case the provider is storing temporal data for every data set definition.
     * For instance, some core providers like SQL o CSV has an internal cache that can be activated. When that happens,
     * calls to this method are executed by the dashbuilder core the determine if the data set requires an update.
     * The update frequency is determined by the {@link DataSetDef} settings {@link DataSetDef#isRefreshAlways()} and
     * {@link DataSetDef#getRefreshTime()}.
     *
     * @param def The data set definition to check
     * @return true only when temporal data hold by this provider that has become stale.
     */
    public boolean isDataSetOutdated(DataSetDef def) {
        // By default, a provider does not hold any temporal data, so return false by default.
        return false;
    }

    // Listen to changes on the data set definition registry, like for instance, when a dataset definition
    // is registered, removed, or updated.

    public void onDataSetDefStale(DataSetDef def) {
        if (this.getType().equals(def.getProvider())) {
            // Add your own logic in case you are interested in this type of events ...
        }
    }

    public void onDataSetDefModified(DataSetDef olDef, DataSetDef newDef) {
        if (this.getType().equals(olDef.getProvider())) {
            // Add your own logic in case you are interested in this type of events ...
        }
    }

    public void onDataSetDefRemoved(DataSetDef oldDef) {
        if (this.getType().equals(oldDef.getProvider())) {
            // Add your own logic in case you are interested in this type of events ...
        }
    }

    public void onDataSetDefRegistered(DataSetDef newDef) {
        // Add your own logic in case you are interested in this type of events ...
    }
}
