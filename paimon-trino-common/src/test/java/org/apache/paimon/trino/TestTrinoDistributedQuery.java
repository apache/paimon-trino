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
import org.testng.SkipException;

/** The test of TrinoDistributedQuery. */
public class TestTrinoDistributedQuery extends AbstractDistributedEngineOnlyQueries {

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        return TrinoQueryRunner.createPrestoQueryRunner(ImmutableMap.of());
    }

    @Override
    public void testCreateTableAsTable() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDuplicatedRowCreateTable() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testExplain() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testExplainAnalyze() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testExplainAnalyzeDynamicFilterInfo() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testExplainAnalyzeVerbose() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testImplicitCastToRowWithFieldsRequiringDelimitation() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testInsertTableIntoTable() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testInsertWithCoercion() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testQueryTransitionsToRunningState() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testTooManyStages() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testAggregationInPatternMatching() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testAliasedInInlineView() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxPercentile() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxSetBigint() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxSetBigintGroupBy() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxSetDouble() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxSetDoubleGroupBy() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxSetGroupByWithNulls() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxSetGroupByWithOnlyNullsInOneGroup() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxSetOnlyNulls() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxSetVarcharGroupBy() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxSetWithNulls() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testArrayAgg() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testArrayShuffle() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testArrays() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testAssignUniqueId() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testAverageAll() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCaseInsensitiveAliasedRelation() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testApproxSetVarchar() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCaseInsensitiveAttribute() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCaseNoElse() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCaseNoElseInconsistentResultType() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCast() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testChainedPatternMatch() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testChainedUnionsWithOrder() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testColumnAliases() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCorrelatedExistsSubqueries() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCorrelatedExistsSubqueriesWithEqualityPredicatesInWhere() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCorrelatedExistsSubqueriesWithPrunedCorrelationSymbols() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCorrelatedInPredicateSubqueries() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCorrelatedJoin() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCorrelatedNonAggregationScalarSubqueries() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCorrelatedScalarSubqueries() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCorrelatedScalarSubqueriesWithScalarAggregation() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCorrelatedScalarSubqueriesWithScalarAggregationAndEqualityPredicatesInWhere() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCorrelationSymbolMapping() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCustomAdd() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCustomRank() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testCustomSum() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDefaultExplainGraphvizFormat() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDefaultExplainTextFormat() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDereferenceInComparison() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDereferenceInFunctionCall() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDereferenceInSubquery() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDescribeInput() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDescribeInputNoParameters() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDescribeInputWithAggregation() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDescribeOutput() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDescribeOutputNamedAndUnnamed() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDescribeOutputNonSelect() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDescribeOutputOnAliasedColumnsAndExpressions() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDistinctWithOrderByNotInSelect() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDistributedExplain() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDistributedExplainGraphvizFormat() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDistributedExplainTextFormat() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDuplicateFields() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testExcept() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testExceptWithAggregation() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testExchangeWithProjectionPushDown() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testExecuteUsingWithSubquery() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testExistsSubquery() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testExistsSubqueryWithGroupBy() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testExplainExecute() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testExplainExecuteWithUsing() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testExplainJoinDistribution() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testExplainOfExplain() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testExplainOfExplainAnalyze() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testGroupByKeyPredicatePushdown() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testGroupByOrderByLimit() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testGroupingInTableSubquery() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testHaving() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testIfExpression() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testInlineView() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testInlineViewWithProjections() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testIntersect() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testIntersectWithAggregation() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testInvalidCastInMultilineQuery() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testInvalidColumn() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testIoExplain() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testJoinedPatternMatch() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testJsonArrayFunction() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testJsonExistsFunction() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testJsonObjectFunction() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testJsonQueryAsInput() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testJsonQueryFunction() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testJsonValueDefaultNull() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testJsonValueDefaults() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testJsonValueFunctionReturnType() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testLargePivot() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testLateMaterializationOuterJoin() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testLimitAll() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testLimitZero() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testLocallyUnrepresentableTimeLiterals() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testLogicalExplain() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testLogicalExplainGraphvizFormat() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testLogicalExplainTextFormat() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testLongPatternMatch() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testMaps() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testMatchRecognize() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testMaxBy() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testMaxByN() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testMaxMinStringWithNulls() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testMergeEmptyApproxSet() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testMergeEmptyNonEmptyApproxSet() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testMergeHyperLogLog() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testMergeHyperLogLogGroupBy() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testMergeHyperLogLogGroupByWithNulls() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testMergeHyperLogLogOnlyNulls() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testMergeHyperLogLogWithNulls() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testMinBy() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testMinByN() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testMultiColumnUnionAll() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testMultipleOccurrencesOfCorrelatedSymbol() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testNonDeterministic() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testNonDeterministicAggregationPredicatePushdown() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testNonDeterministicProjection() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testNonDeterministicTableScanPredicatePushdown() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testNullInput() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testOffsetEmptyResult() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testP4ApproxSetBigint() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testP4ApproxSetBigintGroupBy() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testP4ApproxSetDouble() {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testP4ApproxSetDoubleGroupBy() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testP4ApproxSetGroupByWithNulls() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testP4ApproxSetGroupByWithOnlyNullsInOneGroup() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testP4ApproxSetOnlyNulls() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testP4ApproxSetVarchar() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testP4ApproxSetVarcharGroupBy() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testP4ApproxSetWithNulls() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testPartialLimitWithPresortedConstantInputs() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testPassingClause() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testPruningCountAggregationOverScalar() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testPushAggregationWithMaskThroughOuterJoin() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testQuantifiedComparison() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testQuotedIdentifiers() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testReferenceToWithQueryInFromClause() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testRepeatedOutputs() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testResetSession() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testRollupOverUnion() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testRowFieldAccessorInJoin() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testRowNumberLimit() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testRowNumberNoOptimization() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testRowNumberPartitionedFilter() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testRowNumberPropertyDerivation() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testRowNumberUnpartitionedFilter() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testRowNumberUnpartitionedFilterLimit() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testRowSubscript() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testScalarSubquery() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testScalarSubqueryWithGroupBy() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testSelectAllFromOuterScopeTable() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testSelectAllFromRow() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testSelectAllFromTable() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testSelectCaseInsensitive() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testSelectColumnOfNulls() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testSetSession() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testShowSession() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testShowTablesFrom() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testShowTablesLikeWithEscape() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testStdDev() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testStdDevPop() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testSubqueriesWithDisjunction() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testSubqueryBody() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testSubqueryBodyDoubleOrderby() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testSubqueryBodyOrderLimit() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testSubqueryBodyProjectedOrderby() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testSubqueryInJsonFunctions() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testSubqueryUnion() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testTableAsSubquery() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testTableQuery() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testTableQueryInUnion() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testTableQueryOrderLimit() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testTopNPartitionedWindow() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testTopNPartitionedWindowWithEqualityFilter() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testTopNRank() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testTopNUnpartitionedLargeWindow() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testTopNUnpartitionedWindow() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testTopNUnpartitionedWindowWithCompositeFilter() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testTopNUnpartitionedWindowWithEqualityFilter() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testTry() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testTwoCorrelatedExistsSubqueries() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testUnaliasedSubqueries() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testUnaliasedSubqueries1() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testUnion() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testUnionAll() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testUnionAllPredicateMoveAroundWithOverlappingProjections() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testUnionDistinct() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testUnionRequiringCoercion() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testUnionWithAggregation() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testUnionWithAggregationAndJoin() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testUnionWithAggregationAndTableScan() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testUnionWithFilterNotInSelect() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testUnionWithJoin() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testUnionWithJoinOnNonTranslateableSymbols() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testUnionWithProjectionPushDown() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testUnionWithTopN() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testUnionWithUnionAndAggregation() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testUnnest() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testUnsuccessfulPatternMatch() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testVariance() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testVariancePop() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testWhereNull() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testWildcardFromSubquery() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testWith() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testWithAliased() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testWithChaining() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testWithColumnAliasing() {
        throw new SkipException("TODO: test not implemented yet");
    }

    public void testWithNestedSubqueries() {
        throw new SkipException("TODO: test not implemented yet");
    }
}
