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

import static com.braintribe.testing.junit.assertions.assertj.core.api.Assertions.assertThat;
import static com.braintribe.utils.lcd.CollectionTools2.asList;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import com.braintribe.execution.graph.api.ParallelGraphExecution;
import com.braintribe.execution.graph.api.ParallelGraphExecution.PgeResult;

/**
 * @author peter.gazdik
 */
public class PgeExecution_ErrorHandling_Tests extends _PgeTestBase {

	protected final TestNode subLeafLLL = new TestNode("subLeafLLL");
	protected final TestNode subLeafLLR = new TestNode("subLeafLLR");
	protected final TestNode subLeafLRL = new TestNode("subLeafLRL");
	protected final TestNode subLeafLRR = new TestNode("subLeafLRR");

	protected final TestNode subLeafRLL = new TestNode("subLeafRLL");
	protected final TestNode subLeafRLR = new TestNode("subLeafRLR");
	protected final TestNode subLeafRRL = new TestNode("subLeafRRL");
	protected final TestNode subLeafRRR = new TestNode("subLeafRRR");

	@Before
	public void setup() {
		NODES_ALL.addAll(asList( //
				subLeafLLL, subLeafLLR, subLeafLRL, subLeafLRR, //
				subLeafRLL, subLeafRLR, subLeafRRL, subLeafRRR //
		));
	}

	@Test
	public void errorIsHandledInternally() throws Exception {
		extendedTreeSetup();

		PgeResult<TestNode, Boolean> result = ParallelGraphExecution.foreach("Test", NODES_ROOT) //
				.itemsToProcessFirst(n -> n.getChildren()) //
				.withThreadPool(2) //
				.run(this::failOnLeaf);

		assertSomeSubLeafsExecuted(result);
	}

	protected void assertSomeSubLeafsExecuted(PgeResult<TestNode, Boolean> result) {
		assertThat(result.hasError()).isTrue();

		List<TestNode> nodes = NODES_ALL.stream() //
				.filter(n -> n.timeOfExecution != null) //
				.collect(Collectors.toList());

		assertThat(nodes).isNotEmpty();
		for (TestNode node : nodes)
			assertThat(node.name).startsWith("subLeaf");
	}

	protected void failOnLeaf(TestNode n) {
		if (n.name.startsWith("leaf"))
			throw new RuntimeException("No: " + n.name);

		n.timeOfExecution = System.nanoTime();
	}

	private void extendedTreeSetup() {
		standardTreeSetup();

		markChildParent(subLeafLLL, leafLL);
		markChildParent(subLeafLLR, leafLL);
		markChildParent(subLeafLRL, leafLR);
		markChildParent(subLeafLRR, leafLR);

		markChildParent(subLeafRLL, leafRL);
		markChildParent(subLeafRLR, leafRL);
		markChildParent(subLeafRRL, leafRR);
		markChildParent(subLeafRRR, leafRR);
	}

}
