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

import java.util.Map;

public class SampleBeanDataSetGenerator implements DataSetGenerator {

    public DataSet buildDataSet(Map<String, String> params) {

        DataSetBuilder builder = DataSetFactory.newDataSetBuilder();
        builder.number("index");

        String fromStr = params.get("from");
        String toStr = params.get("to");
        int from = fromStr != null ? Integer.valueOf(fromStr) : 0;
        int to = fromStr != null ? Integer.valueOf(toStr) : 10;
        for (int i = from; i < to; i++) {
             builder.row(i);
        }
        return builder.buildDataSet();
    }
}
