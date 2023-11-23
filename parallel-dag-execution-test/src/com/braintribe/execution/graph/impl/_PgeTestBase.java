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

import static com.braintribe.utils.lcd.CollectionTools2.asList;

import java.util.List;
import java.util.function.Function;

/**
 * @author peter.gazdik
 */
public abstract class _PgeTestBase {

	protected static Function<List<TestNode>, PgeGraph<TestNode>> childFactory = nodes -> PgeGraph.forChildResolver(nodes, TestNode::getChildren);
	protected static Function<List<TestNode>, PgeGraph<TestNode>> parentFactory = nodes -> PgeGraph.forParentResolver(nodes, TestNode::getParents);

	protected final TestNode root = new TestNode("root");
	protected final TestNode innerL = new TestNode("innerL");
	protected final TestNode innerR = new TestNode("innerR");

	protected final TestNode leafLL = new TestNode("leafLL");
	protected final TestNode leafLR = new TestNode("leafLR");

	protected final TestNode leafRL = new TestNode("leafRL");
	protected final TestNode leafRR = new TestNode("leafRL");

	protected final List<TestNode> NODES_ROOT = asList(root);
	protected final List<TestNode> NODES_LEAVES = asList(leafLL, leafLR, leafRL, leafRR);
	protected final List<TestNode> NODES_ALL = asList(root, innerL, innerR, leafLL, leafLR, leafRL, leafRR);

	protected void standardTreeSetup() {
		markChildParent(leafLL, innerL);
		markChildParent(leafLR, innerL);

		markChildParent(leafRL, innerR);
		markChildParent(leafRR, innerR);

		markChildParent(innerL, root);
		markChildParent(innerR, root);
	}

	protected void standardDagSetup() {
		standardTreeSetup();
		standardDagAdditionToStandardTree();
	}

	protected void standardDagAdditionToStandardTree() {
		markChildParent(leafLL, innerR);
		markChildParent(leafLR, innerR);

		markChildParent(leafRL, innerL);
		markChildParent(leafRR, innerL);
	}

	protected static void markChildParent(TestNode child, TestNode parent) {
		child.parents.add(parent);
		parent.children.add(child);
	}

}
