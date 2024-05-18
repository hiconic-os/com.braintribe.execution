// ============================================================================
// Copyright BRAINTRIBE TECHNOLOGY GMBH, Austria, 2002-2022
// 
// This library is free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either version 3 of the License, or (at your option) any later version.
// 
// This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License along with this library; See http://www.gnu.org/licenses/.
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
