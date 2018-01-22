/*
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
package com.facebook.presto.tests.statistics;

import java.util.Optional;

import static com.facebook.presto.tests.statistics.MetricComparison.Result.DIFFER;
import static com.facebook.presto.tests.statistics.MetricComparison.Result.MATCH;
import static com.facebook.presto.tests.statistics.MetricComparison.Result.NO_BASELINE;
import static com.facebook.presto.tests.statistics.MetricComparison.Result.NO_ESTIMATE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class MetricComparison<T>
{
    private final Metric<T> metric;
    private final Optional<T> estimatedValue;
    private final Optional<T> actualValue;

    public MetricComparison(Metric<T> metric, Optional<T> estimatedValue, Optional<T> actualValue)
    {
        this.metric = metric;
        this.estimatedValue = estimatedValue;
        this.actualValue = actualValue;
    }

    public Metric<T> getMetric()
    {
        return metric;
    }

    @Override
    public String toString()
    {
        return format("Metric [%s] - estimated: [%s], real: [%s]",
                metric, print(estimatedValue), print(actualValue));
    }

    public Result result(MetricComparisonStrategy<T> metricComparisonStrategy)
    {
        requireNonNull(metricComparisonStrategy, "metricComparisonStrategy is null");

        if (!estimatedValue.isPresent() && !actualValue.isPresent()) {
            return MATCH;
        }
        if (!estimatedValue.isPresent()) {
            return NO_ESTIMATE;
        }
        if (!actualValue.isPresent()) {
            return NO_BASELINE;
        }
        return metricComparisonStrategy.matches(actualValue.get(), estimatedValue.get()) ? MATCH : DIFFER;
    }

    private String print(Optional<T> cost)
    {
        return cost.map(Object::toString).orElse("UNKNOWN");
    }

    public enum Result
    {
        NO_ESTIMATE,
        NO_BASELINE,
        DIFFER,
        MATCH
    }
}
