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

import static com.braintribe.utils.lcd.CollectionTools2.newList;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.Test;

import com.braintribe.execution.graph.api.ParallelGraphExecution;
import com.braintribe.execution.graph.api.ParallelGraphExecution.PgeResult;

/**
 * We make a graph where each node is a child of all previous nodes;
 * <p>
 * The original cycle-detection was super inefficient with dense graphs.
 * 
 * @author peter.gazdik
 */
public class PgeExecution_Healthy_BigGraph_Tests extends _PgeTestBase {

	private static final int NODES = 50;

	List<TestNode> nodes = newList();

	@Test
	public void bigGraphTest() throws Exception {
		for (int i = 1; i <= NODES; i++)
			addNode(i);

		PgeResult<TestNode, Boolean> result = ParallelGraphExecution.foreach("Test", nodes) //
				.itemsToProcessFirst(n -> n.getParents()) //
				.withThreadPool(2) //
				.run(this::s_checkCalledJustOnce);

		assertCorrectOrder(result);
	}

	private void addNode(int i) {
		TestNode newNode = new TestNode("n-" + i);

		for (TestNode otherNode : nodes)
			markChildParent(newNode, otherNode);

		nodes.add(newNode);
	}

	int assertionIndex = 0;

	protected void assertCorrectOrder(PgeResult<TestNode, Boolean> result) {
		assertNoErrorAndSortByTimeOfExecution(result, nodes);

		for (TestNode extectedNode : nodes) {
			TestNode actualNode = nodes.get(assertionIndex++);
			assertThat(actualNode).as("Wrong node at position: " + assertionIndex).isSameAs(extectedNode);
		}
	}

}
