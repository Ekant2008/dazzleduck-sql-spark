package io.dazzleduck.sql.spark.extension;

import io.dazzleduck.sql.spark.ArrowRPCScan;
import org.apache.spark.sql.catalyst.rules.Rule;
import org.apache.spark.sql.execution.ProjectExec;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.aggregate.HashAggregateExec;
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec;

/**
 * Optimizer rule that removes redundant HashAggregate operations when the aggregation
 * has already been pushed down to the ArrowRPCScan datasource.
 */
public class RemoveHashAggregate extends Rule<SparkPlan> {

    @Override
    public SparkPlan apply(SparkPlan plan) {
        if (!(plan instanceof HashAggregateExec hashAggregateExec)) {
            return plan;
        }
        if (!(hashAggregateExec.child() instanceof ProjectExec projectExec)) {
            return plan;
        }
        if (!(projectExec.child() instanceof BatchScanExec batchScanExec)) {
            return plan;
        }
        if (!(batchScanExec.scan() instanceof ArrowRPCScan arrowScan) || !arrowScan.hasPushedAggregation()) {
            return plan;
        }
        // Aggregation already pushed to datasource, remove redundant HashAggregate
        return projectExec;
    }
}


