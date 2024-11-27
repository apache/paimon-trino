/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.trino;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import io.trino.testing.AbstractDistributedEngineOnlyQueries;
import io.trino.testing.QueryRunner;

/** The test of TrinoDistributedQuery. */
public class TrinoDistributedQueryTest extends AbstractDistributedEngineOnlyQueries {

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        return TrinoQueryRunner.createPrestoQueryRunner(ImmutableMap.of());
    }

    @Override
    public void testCreateTableAsTable() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testDuplicatedRowCreateTable() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testExplain() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testExplainAnalyze() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testExplainAnalyzeDynamicFilterInfo() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testExplainAnalyzeVerbose() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testImplicitCastToRowWithFieldsRequiringDelimitation() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testInsertTableIntoTable() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testInsertWithCoercion() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testQueryTransitionsToRunningState() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testTooManyStages() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testAggregationInPatternMatching() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testAliasedInInlineView() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxPercentile() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxSetBigint() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxSetBigintGroupBy() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxSetDouble() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxSetDoubleGroupBy() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxSetGroupByWithNulls() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxSetGroupByWithOnlyNullsInOneGroup() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxSetOnlyNulls() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxSetVarcharGroupBy() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxSetWithNulls() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testArrayAgg() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testArrayShuffle() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testArrays() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testAssignUniqueId() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testAverageAll() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testCaseInsensitiveAliasedRelation() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxSetVarchar() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testCaseInsensitiveAttribute() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testCaseNoElse() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testCaseNoElseInconsistentResultType() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testCast() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testChainedPatternMatch() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testChainedUnionsWithOrder() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testColumnAliases() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testCorrelatedExistsSubqueries() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testCorrelatedExistsSubqueriesWithEqualityPredicatesInWhere() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testCorrelatedExistsSubqueriesWithPrunedCorrelationSymbols() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testCorrelatedInPredicateSubqueries() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testCorrelatedJoin() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testCorrelatedNonAggregationScalarSubqueries() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testCorrelatedScalarSubqueries() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testCorrelatedScalarSubqueriesWithScalarAggregation() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testCorrelatedScalarSubqueriesWithScalarAggregationAndEqualityPredicatesInWhere() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testColumnNames() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testExplainAnalyzeTopLevelTimes() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testExplainDistributed() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testCorrelationSymbolMapping() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testCustomAdd() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testCustomRank() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testCustomSum() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testDefaultExplainGraphvizFormat() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testDefaultExplainTextFormat() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testDereferenceInComparison() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testDereferenceInFunctionCall() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testDereferenceInSubquery() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testDescribeInput() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testDescribeInputNoParameters() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testDescribeInputWithAggregation() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testDescribeOutput() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testDescribeOutputNamedAndUnnamed() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testDescribeOutputNonSelect() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testDescribeOutputOnAliasedColumnsAndExpressions() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testDistinctWithOrderByNotInSelect() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testDistributedExplain() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testDistributedExplainGraphvizFormat() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testDistributedExplainTextFormat() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testDuplicateFields() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testExcept() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testExceptWithAggregation() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testExchangeWithProjectionPushDown() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testExecuteUsingWithSubquery() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testExistsSubquery() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testExistsSubqueryWithGroupBy() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testExplainExecute() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testExplainExecuteWithUsing() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testExplainJoinDistribution() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testExplainOfExplain() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testExplainOfExplainAnalyze() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testGroupByKeyPredicatePushdown() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testGroupByOrderByLimit() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testGroupingInTableSubquery() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testHaving() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testIfExpression() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testInlineView() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testInlineViewWithProjections() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testIntersect() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testIntersectWithAggregation() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testInvalidCastInMultilineQuery() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testInvalidColumn() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testIoExplain() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testJoinedPatternMatch() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testJsonArrayFunction() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testJsonExistsFunction() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testJsonObjectFunction() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testJsonQueryAsInput() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testJsonQueryFunction() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testJsonValueDefaultNull() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testJsonValueDefaults() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testJsonValueFunctionReturnType() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testLargePivot() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testLimitAll() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testLimitZero() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testLocallyUnrepresentableTimeLiterals() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testLogicalExplain() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testLogicalExplainGraphvizFormat() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testLogicalExplainTextFormat() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testLongPatternMatch() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testMaps() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testMatchRecognize() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testMaxBy() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testMaxByN() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testMaxMinStringWithNulls() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testMergeEmptyApproxSet() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testMergeEmptyNonEmptyApproxSet() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testMergeHyperLogLog() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testMergeHyperLogLogGroupBy() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testMergeHyperLogLogGroupByWithNulls() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testMergeHyperLogLogOnlyNulls() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testMergeHyperLogLogWithNulls() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testMinBy() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testMinByN() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testMultiColumnUnionAll() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testMultipleOccurrencesOfCorrelatedSymbol() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testNonDeterministic() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testNonDeterministicAggregationPredicatePushdown() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testNonDeterministicProjection() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testNonDeterministicTableScanPredicatePushdown() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testNullInput() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testOffsetEmptyResult() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testP4ApproxSetBigint() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testP4ApproxSetBigintGroupBy() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testP4ApproxSetDouble() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testP4ApproxSetDoubleGroupBy() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testP4ApproxSetGroupByWithNulls() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testP4ApproxSetGroupByWithOnlyNullsInOneGroup() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testP4ApproxSetOnlyNulls() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testP4ApproxSetVarchar() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testP4ApproxSetVarcharGroupBy() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testP4ApproxSetWithNulls() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testPartialLimitWithPresortedConstantInputs() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testPassingClause() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testPruningCountAggregationOverScalar() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testPushAggregationWithMaskThroughOuterJoin() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testQuantifiedComparison() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testQuotedIdentifiers() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testReferenceToWithQueryInFromClause() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testRepeatedOutputs() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testResetSession() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testRollupOverUnion() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testRowFieldAccessorInJoin() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testRowNumberLimit() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testRowNumberNoOptimization() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testRowNumberPartitionedFilter() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testRowNumberPropertyDerivation() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testRowNumberUnpartitionedFilter() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testRowNumberUnpartitionedFilterLimit() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testRowSubscript() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testScalarSubquery() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testScalarSubqueryWithGroupBy() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testSelectAllFromOuterScopeTable() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testSelectAllFromRow() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testSelectAllFromTable() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testSelectCaseInsensitive() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testSelectColumnOfNulls() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testSetSession() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testShowSession() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testShowTablesFrom() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testShowTablesLikeWithEscape() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testStdDev() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testStdDevPop() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testSubqueriesWithDisjunction() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testSubqueryBody() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testSubqueryBodyDoubleOrderby() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testSubqueryBodyOrderLimit() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testSubqueryBodyProjectedOrderby() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testSubqueryInJsonFunctions() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testSubqueryUnion() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testTableAsSubquery() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testTableQuery() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testTableQueryInUnion() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testTableQueryOrderLimit() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testTopNPartitionedWindow() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testTopNPartitionedWindowWithEqualityFilter() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testTopNRank() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testTopNUnpartitionedLargeWindow() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testTopNUnpartitionedWindow() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testTopNUnpartitionedWindowWithCompositeFilter() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testTopNUnpartitionedWindowWithEqualityFilter() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testTry() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testTwoCorrelatedExistsSubqueries() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testUnaliasedSubqueries() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testUnaliasedSubqueries1() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testUnion() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testUnionAll() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testUnionAllPredicateMoveAroundWithOverlappingProjections() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testUnionDistinct() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testUnionRequiringCoercion() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testUnionWithAggregation() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testUnionWithAggregationAndJoin() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testUnionWithAggregationAndTableScan() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testUnionWithFilterNotInSelect() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testUnionWithJoin() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testUnionWithJoinOnNonTranslateableSymbols() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testUnionWithProjectionPushDown() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testUnionWithTopN() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testUnionWithUnionAndAggregation() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testUnnest() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testUnsuccessfulPatternMatch() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testVariance() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testVariancePop() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testWhereNull() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testWildcardFromSubquery() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testWith() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testWithAliased() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testWithChaining() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testWithColumnAliasing() {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    public void testWithNestedSubqueries() {
        throw new RuntimeException("TODO: test not implemented yet");
    }
}
