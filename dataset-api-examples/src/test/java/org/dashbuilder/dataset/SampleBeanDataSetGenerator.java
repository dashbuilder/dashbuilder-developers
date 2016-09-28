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
