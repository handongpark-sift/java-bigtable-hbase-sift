/*
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.beam.sequencefiles;

import com.google.bigtable.repackaged.com.google.api.core.InternalApi;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DoFn} function that converts a {@link Result} in the pipeline input to a {@link
 * Mutation} for output.
 */
@InternalApi
public class HBaseResultToDeleteMutationFn extends DoFn<KV<ImmutableBytesWritable, Result>, Mutation> {
    private static Logger logger = LoggerFactory.getLogger(HBaseResultToMutationFn.class);

    private static final long serialVersionUID = 1L;

    private static final int MAX_CELLS = 100_000 - 1;

    private static final DataCellPredicateFactory DATA_CELL_PREDICATE_FACTORY =
        new DataCellPredicateFactory();

    private transient boolean isEmptyRowWarned;

    @VisibleForTesting
    static void setLogger(Logger log) {
        logger = log;
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
        KV<ImmutableBytesWritable, Result> kv = context.element();
        List<Cell> cells = checkEmptyRow(kv);
        if (cells.isEmpty()) {
            return;
        }

        // Split the row into multiple puts if it exceeds the maximum mutation limit
        Iterator<Cell> cellIt = cells.iterator();

        while (cellIt.hasNext()) {
            Delete delete = new Delete(kv.getKey().get());

            for (int i = 0; i < MAX_CELLS && cellIt.hasNext(); i++) {
                delete.add(cellIt.next());
            }

            context.output(delete);
        }
    }

    // Warns about empty row on first occurrence only and replaces a null array with 0-length one.
    private List<Cell> checkEmptyRow(KV<ImmutableBytesWritable, Result> kv) {
        List<Cell> cells = kv.getValue().listCells();
        if (cells == null) {
            cells = Collections.emptyList();
        }
        if (!isEmptyRowWarned && cells.isEmpty()) {
            logger.warn("Encountered empty row. Was input file serialized by HBase 0.94?");
            isEmptyRowWarned = true;
        }
        return cells;
    }
}