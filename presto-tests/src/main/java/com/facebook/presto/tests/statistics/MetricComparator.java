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

import com.facebook.presto.Session;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.planner.iterative.Lookup.noLookup;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public final class MetricComparator
{
    private MetricComparator() {}

    public static Set<MetricComparison<?>> getMetricComparisons(String query, QueryRunner runner, Set<Metric<?>> metrics)
    {
        return getMetricComparisons(query, runner, ImmutableList.copyOf(metrics));
    }

    private static Set<MetricComparison<?>> getMetricComparisons(String query, QueryRunner runner, List<Metric<?>> metrics)
    {
        List<Optional<?>> estimatedValues = getEstimatedValues(metrics, query, runner);
        List<Optional<?>> actualValues = getActualValues(metrics, query, runner);

        ImmutableSet.Builder<MetricComparison<?>> metricComparisons = ImmutableSet.builder();
        for (int i = 0; i < metrics.size(); ++i) {
            //noinspection unchecked
            metricComparisons.add(new MetricComparison(
                    metrics.get(i),
                    estimatedValues.get(i),
                    actualValues.get(i)));
        }
        return metricComparisons.build();
    }

    private static List<Optional<?>> getEstimatedValues(List<Metric<?>> metrics, String query, QueryRunner runner)
    {
        return transaction(runner.getTransactionManager(), runner.getAccessControl())
                .singleStatement()
                .execute(runner.getDefaultSession(), session -> {
                    if (runner instanceof DistributedQueryRunner) {
                        return getEstimatedValuesDistributed(metrics, query, (DistributedQueryRunner) runner, session);
                    }
                    else if (runner instanceof LocalQueryRunner) {
                        return getEstimatedValuesLocal(metrics, query, (LocalQueryRunner) runner, session);
                    }
                    else {
                        throw new IllegalArgumentException("only local and distributed runner supported");
                    }
                });
    }

    private static List<Optional<?>> getEstimatedValuesDistributed(List<Metric<?>> metrics, String query, DistributedQueryRunner runner, Session session)
    {
        String queryId = runner.executeWithQueryId(session, query).getQueryId();
        Plan queryPlan = runner.getQueryPlan(new QueryId(queryId));
        return getEstimatedValues(metrics, runner, session, queryPlan);
    }

    private static List<Optional<?>> getEstimatedValuesLocal(List<Metric<?>> metrics, String query, LocalQueryRunner runner, Session session)
    {
        Plan queryPlan = runner.createPlan(session, query);
        return getEstimatedValues(metrics, runner, session, queryPlan);
    }

    private static List<Optional<?>> getEstimatedValues(List<Metric<?>> metrics, QueryRunner runner, Session session, Plan queryPlan)
    {
        OutputNode outputNode = (OutputNode) queryPlan.getRoot();
        PlanNodeStatsEstimate outputNodeStats = calculateStats(outputNode, runner.getStatsCalculator(), session, queryPlan.getTypes());
        StatsContext statsContext = buildStatsContext(queryPlan, outputNode);
        List<Optional<?>> estimatedValues = getEstimatedValues(metrics, outputNodeStats, statsContext);
        return estimatedValues;
    }

    private static PlanNodeStatsEstimate calculateStats(PlanNode node, StatsCalculator statsCalculator, Session session, Map<Symbol, Type> types)
    {
        // We calculate stats one-off, so caching is not necessary
        return statsCalculator.calculateStats(
                node,
                source -> calculateStats(source, statsCalculator, session, types),
                noLookup(),
                session,
                types);
    }

    private static StatsContext buildStatsContext(Plan queryPlan, OutputNode outputNode)
    {
        ImmutableMap.Builder<String, Symbol> columnSymbols = ImmutableMap.builder();
        for (int columnId = 0; columnId < outputNode.getColumnNames().size(); ++columnId) {
            columnSymbols.put(outputNode.getColumnNames().get(columnId), outputNode.getOutputSymbols().get(columnId));
        }
        return new StatsContext(columnSymbols.build(), queryPlan.getTypes());
    }

    private static List<Optional<?>> getActualValues(List<Metric<?>> metrics, String query, QueryRunner runner)
    {
        String statsQuery = "SELECT "
                + metrics.stream().map(Metric::getComputingAggregationSql).collect(joining(","))
                + " FROM (" + query + ")";
        try {
            MaterializedRow actualValuesRow = getOnlyElement(runner.execute(statsQuery).getMaterializedRows());

            ImmutableList.Builder<Optional<?>> actualValues = ImmutableList.builder();
            for (int i = 0; i < metrics.size(); ++i) {
                actualValues.add(metrics.get(i).getValueFromAggregationQueryResult(actualValuesRow.getField(i)));
            }
            return actualValues.build();
        }
        catch (Exception e) {
            throw new RuntimeException(format("Failed to execute query to compute actual values: %s", statsQuery), e);
        }
    }

    private static List<Optional<?>> getEstimatedValues(List<Metric<?>> metrics, PlanNodeStatsEstimate outputNodeStatisticsEstimates, StatsContext statsContext)
    {
        return metrics.stream()
                .map(metric -> metric.getValueFromPlanNodeEstimate(outputNodeStatisticsEstimates, statsContext))
                .collect(toList());
    }
}
