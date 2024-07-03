// ============================================================================
// Copyright BRAINTRIBE TECHNOLOGY GMBH, Austria, 2002-2022
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// ============================================================================
package com.braintribe.execution.graph.impl;

import static com.braintribe.testing.junit.assertions.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;

import com.braintribe.execution.graph.api.ParallelGraphExecution;
import com.braintribe.execution.graph.api.ParallelGraphExecution.PgeResult;

/**
 * @author peter.gazdik
 */
public class PgeExecution_Healthy_Tests extends _PgeTestBase {

	@Test
	public void standardDag_children() throws Exception {
		standardDagSetup();

		PgeResult<TestNode, Boolean> result = ParallelGraphExecution.foreach("Test", NODES_ROOT) //
				.itemsToProcessFirst(n -> n.getChildren()) //
				.withThreadPool(2) //
				.run(this::markExecutionTime);

		assertCorrectOrder(result);
	}

	@Test
	public void standardDag_parent() throws Exception {
		standardDagSetup();

		PgeResult<TestNode, Boolean> result = ParallelGraphExecution.foreach("Test", NODES_LEAVES) //
				.itemsToProcessAfter(n -> n.getParents()) //
				.withThreadPool(2) //
				.run(this::markExecutionTime);

		assertCorrectOrder(result);
	}

	@Test
	public void standardDag_children_multigraph() throws Exception {
		standardDagSetup();

		PgeResult<TestNode, Boolean> result = ParallelGraphExecution.foreach("Test", NODES_ROOT) //
				.itemsToProcessFirst(n -> concatNTimes(n.getChildren(), 5)) //
				.withThreadPool(2) //
				.run(this::s_checkCalledJustOnce);

		assertCorrectOrder(result);
	}

	@Test
	public void standardDag_parent_multigraph() throws Exception {
		standardDagSetup();

		PgeResult<TestNode, Boolean> result = ParallelGraphExecution.foreach("Test", NODES_LEAVES) //
				.itemsToProcessAfter(n -> concatNTimes(n.getParents(), 5)) //
				.withThreadPool(2) //
				.run(this::s_checkCalledJustOnce);

		assertThat(result.hasError()).isFalse();
	}

	private List<TestNode> concatNTimes(List<TestNode> nodes, int n) {
		return IntStream.range(0, n) //
				.mapToObj(i -> nodes) //
				.flatMap(List::stream) //
				.collect(Collectors.toList());
	}

	// helpers
	int assertionIndex = 0;

	private void assertCorrectOrder(PgeResult<TestNode, Boolean> result) {
		assertNoErrorAndSortByTimeOfExecution(result, NODES_ALL);

		assertNextNodes(4, "leaf");
		assertNextNodes(2, "inner");
		assertNextNodes(1, "root");
	}

	private void assertNextNodes(int n, String expectedNamePrefix) {
		for (int i = 0; i < n; i++) {
			TestNode node = NODES_ALL.get(assertionIndex++);
			assertThat(node.name).as("Wrong node at position: " + assertionIndex).startsWith(expectedNamePrefix);
		}
	}

}
