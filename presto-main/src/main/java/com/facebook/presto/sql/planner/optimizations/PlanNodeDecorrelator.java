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

package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolsExtractor;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.tree.ComparisonExpressionType.EQUAL;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class PlanNodeDecorrelator
{
    private final PlanNodeIdAllocator idAllocator;
    private final Lookup lookup;

    public PlanNodeDecorrelator(PlanNodeIdAllocator idAllocator, Lookup lookup)
    {
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.lookup = requireNonNull(lookup, "lookup is null");
    }

    public Optional<DecorrelatedNode> decorrelateFilters(PlanNode node, List<Symbol> correlation)
    {
        Optional<DecorrelationResult> decorrelationResultOptional = lookup.resolve(node).accept(new DecorrelatingVisitor(correlation), null);
        return decorrelationResultOptional.flatMap(decorrelationResult -> decorrelatedNode(
                decorrelationResult.correlatedPredicates,
                decorrelationResult.node,
                correlation));
    }

    private class DecorrelatingVisitor
            extends PlanVisitor<Optional<DecorrelationResult>, Void>
    {
        final List<Symbol> correlation;

        DecorrelatingVisitor(List<Symbol> correlation)
        {
            this.correlation = correlation;
        }

        @Override
        protected Optional<DecorrelationResult> visitPlan(PlanNode node, Void context)
        {
            return Optional.of(new DecorrelationResult(
                    node,
                    ImmutableSet.of(),
                    ImmutableList.of(),
                    ImmutableMap.of(),
                    false));
        }

        @Override
        public Optional<DecorrelationResult> visitFilter(FilterNode node, Void context)
        {
            Expression predicate = node.getPredicate();
            if (!SymbolsExtractor.extractUnique(predicate).containsAll(correlation)) {
                return Optional.empty();
            }

            Map<Boolean, List<Expression>> predicates = ExpressionUtils.extractConjuncts(predicate).stream()
                    .collect(Collectors.partitioningBy(isUsingPredicate(correlation)));
            List<Expression> correlatedPredicates = ImmutableList.copyOf(predicates.get(true));
            List<Expression> uncorrelatedPredicates = ImmutableList.copyOf(predicates.get(false));

            FilterNode newFilterNode = new FilterNode(
                    idAllocator.getNextId(),
                    node.getSource(),
                    ExpressionUtils.combineConjuncts(uncorrelatedPredicates));

            return Optional.of(new DecorrelationResult(
                    newFilterNode,
                    Sets.difference(SymbolsExtractor.extractUnique(correlatedPredicates), ImmutableSet.copyOf(correlation)),
                    correlatedPredicates,
                    extractCorrelatedSymbolsMapping(correlatedPredicates),
                    false));
        }

        @Override
        public Optional<DecorrelationResult> visitLimit(LimitNode node, Void context)
        {
            Optional<DecorrelationResult> childDecorrelationResultOptional = lookup.resolve(node.getSource()).accept(this, null);
            if (!childDecorrelationResultOptional.isPresent() || node.getCount() == 0) {
                return Optional.empty();
            }

            DecorrelationResult childDecorrelationResult = childDecorrelationResultOptional.get();
            if (childDecorrelationResult.atMostSingleRow) {
                return childDecorrelationResultOptional;
            }

            if (correlation.isEmpty() || !childDecorrelationResult.getConstantSymbols().containsAll(node.getOutputSymbols())) {
                return Optional.empty();
            }

            // Rewrite to aggregation on symbols which we know are constants and are also part of output
            childDecorrelationResultOptional = new AggregationNode(
                    idAllocator.getNextId(),
                    node.getSource(),
                    ImmutableMap.of(),
                    ImmutableList.of(ImmutableList.<Symbol>builder()
                            .addAll(correlation)
                            .addAll(node.getOutputSymbols())
                            .build()),
                    AggregationNode.Step.SINGLE,
                    Optional.empty(),
                    Optional.empty()).accept(this, null);
            if (!childDecorrelationResultOptional.isPresent() || !childDecorrelationResultOptional.get().atMostSingleRow) {
                return Optional.empty();
            }

            return childDecorrelationResultOptional;
        }

        @Override
        public Optional<DecorrelationResult> visitAggregation(AggregationNode node, Void context)
        {
            if (node.hasEmptyGroupingSet()) {
                return Optional.empty();
            }

            Optional<DecorrelationResult> childDecorrelationResultOptional = lookup.resolve(node.getSource()).accept(this, null);
            if (!childDecorrelationResultOptional.isPresent()) {
                return Optional.empty();
            }

            DecorrelationResult childDecorrelationResult = childDecorrelationResultOptional.get();
            Set<Symbol> constantSymbols = childDecorrelationResult.getConstantSymbols();

            AggregationNode decorrelatedAggregation = new SymbolMapper(childDecorrelationResult.correlatedSymbolsMapping)
                    .map(node, childDecorrelationResult.node);

            ImmutableList.Builder<List<Symbol>> newGroupingSets = ImmutableList.builder();
            for (List<Symbol> groupingSet : decorrelatedAggregation.getGroupingSets()) {
                List<Symbol> symbolsToAdd = childDecorrelationResult.symbolsToPropagate.stream()
                        .filter(symbol -> !groupingSet.contains(symbol))
                        .collect(toImmutableList());

                if (!constantSymbols.containsAll(symbolsToAdd)) {
                    return Optional.empty();
                }

                newGroupingSets.add(ImmutableList.<Symbol>builder()
                        .addAll(groupingSet)
                        .addAll(symbolsToAdd).build());
            }

            AggregationNode newAggregation = new AggregationNode(
                    decorrelatedAggregation.getId(),
                    decorrelatedAggregation.getSource(),
                    decorrelatedAggregation.getAggregations(),
                    newGroupingSets.build(),
                    decorrelatedAggregation.getStep(),
                    decorrelatedAggregation.getHashSymbol(),
                    decorrelatedAggregation.getGroupIdSymbol());

            boolean atMostSingleRow = newAggregation.getGroupingSets().size() == 1
                    && constantSymbols.containsAll(newAggregation.getGroupingKeys());

            return Optional.of(new DecorrelationResult(
                    newAggregation,
                    childDecorrelationResult.symbolsToPropagate,
                    childDecorrelationResult.correlatedPredicates,
                    childDecorrelationResult.correlatedSymbolsMapping,
                    atMostSingleRow));
        }

        @Override
        public Optional<DecorrelationResult> visitProject(ProjectNode node, Void context)
        {
            Optional<DecorrelationResult> childDecorrelationResultOptional = lookup.resolve(node.getSource()).accept(this, null);
            if (!childDecorrelationResultOptional.isPresent()) {
                return Optional.empty();
            }

            DecorrelationResult childDecorrelationResult = childDecorrelationResultOptional.get();
            List<Symbol> symbolsToAdd = childDecorrelationResult.symbolsToPropagate.stream()
                    .filter(symbol -> !node.getOutputSymbols().contains(symbol))
                    .collect(toImmutableList());

            Assignments assignments = Assignments.builder()
                    .putAll(node.getAssignments())
                    .putIdentities(symbolsToAdd)
                    .build();

            return Optional.of(new DecorrelationResult(
                    new ProjectNode(idAllocator.getNextId(), childDecorrelationResult.node, assignments),
                    childDecorrelationResult.symbolsToPropagate,
                    childDecorrelationResult.correlatedPredicates,
                    childDecorrelationResult.correlatedSymbolsMapping,
                    childDecorrelationResult.atMostSingleRow));
        }

        private Map<Symbol, Symbol> extractCorrelatedSymbolsMapping(List<Expression> conjuncts)
        {
            Map<Symbol, Symbol> mapping = new HashMap<>();
            for (Expression conjunct : conjuncts) {
                if (!(conjunct instanceof ComparisonExpression)) {
                    continue;
                }

                ComparisonExpression comparison = (ComparisonExpression) conjunct;
                if (!(comparison.getLeft() instanceof SymbolReference
                        && comparison.getRight() instanceof SymbolReference
                        && comparison.getType().equals(EQUAL))) {
                    continue;
                }

                Symbol left = Symbol.from(comparison.getLeft());
                Symbol right = Symbol.from(comparison.getRight());

                if (correlation.contains(left) && !correlation.contains(right)) {
                    mapping.put(left, right);
                }

                if (correlation.contains(right) && !correlation.contains(left)) {
                    mapping.put(right, left);
                }
            }

            return ImmutableMap.copyOf(mapping);
        }

        private Predicate<Expression> isUsingPredicate(List<Symbol> symbols)
        {
            return expression -> symbols.stream().anyMatch(SymbolsExtractor.extractUnique(expression)::contains);
        }
    }

    private static class DecorrelationResult
    {
        final PlanNode node;
        final Set<Symbol> symbolsToPropagate;
        final List<Expression> correlatedPredicates;

        final Map<Symbol, Symbol> correlatedSymbolsMapping;
        // If a subquery has at most single row for any correlation values?
        final boolean atMostSingleRow;

        DecorrelationResult(PlanNode node, Set<Symbol> symbolsToPropagate, List<Expression> correlatedPredicates, Map<Symbol, Symbol> correlatedSymbolsMapping, boolean atMostSingleRow)
        {
            this.node = node;
            this.symbolsToPropagate = symbolsToPropagate;
            this.correlatedPredicates = correlatedPredicates;
            this.atMostSingleRow = atMostSingleRow;
            this.correlatedSymbolsMapping = correlatedSymbolsMapping;
        }

        Set<Symbol> getConstantSymbols()
        {
            // Returns constants symbols from a perspective of a subquery
            return ImmutableSet.copyOf(correlatedSymbolsMapping.values());
        }
    }

    private Optional<DecorrelatedNode> decorrelatedNode(
            List<Expression> correlatedPredicates,
            PlanNode node,
            List<Symbol> correlation)
    {
        if (SymbolsExtractor.extractUnique(node, lookup).stream().anyMatch(correlation::contains)) {
            // node is still correlated ; /
            return Optional.empty();
        }
        return Optional.of(new DecorrelatedNode(correlatedPredicates, node));
    }

    public static class DecorrelatedNode
    {
        private final List<Expression> correlatedPredicates;
        private final PlanNode node;

        public DecorrelatedNode(List<Expression> correlatedPredicates, PlanNode node)
        {
            requireNonNull(correlatedPredicates, "correlatedPredicates is null");
            this.correlatedPredicates = ImmutableList.copyOf(correlatedPredicates);
            this.node = requireNonNull(node, "node is null");
        }

        public Optional<Expression> getCorrelatedPredicates()
        {
            if (correlatedPredicates.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(ExpressionUtils.and(correlatedPredicates));
        }

        public PlanNode getNode()
        {
            return node;
        }
    }
}
