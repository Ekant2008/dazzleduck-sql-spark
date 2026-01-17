package io.dazzleduck.sql.spark.extension;

import org.apache.spark.sql.SparkSessionExtensions;
import org.apache.spark.sql.SparkSessionExtensionsProvider;
import scala.runtime.BoxedUnit;

public class ArrowRPCExtensions implements SparkSessionExtensionsProvider {
    @Override
    public BoxedUnit apply(SparkSessionExtensions extensions) {
        extensions.injectQueryStageOptimizerRule(session -> new RemoveHashAggregate());
        return BoxedUnit.UNIT;
    }
}
