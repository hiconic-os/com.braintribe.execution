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
