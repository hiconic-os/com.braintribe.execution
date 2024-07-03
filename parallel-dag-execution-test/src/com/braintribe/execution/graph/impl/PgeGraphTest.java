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

import static com.braintribe.utils.lcd.CollectionTools2.asList;
import static com.braintribe.utils.lcd.CollectionTools2.first;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Test;

/**
 * Tests for {@link PgeGraph} construction, using either {@link PgeGraph#forChildResolver(Iterable, Function)} or {@link PgeGraph#forParentResolver(Iterable, Function)}.
 * <p>
 * The tests verify that a correct graph is created based on the {@link TestNode}s provided. Each test is named based on the configured graph topology and which of the "neighbor" functions  
 * 
 * @author peter.gazdik
 */
public class PgeGraphTest extends _PgeTestBase {

	// #################################################
	// ## . . . . . . . . Single Node . . . . . . . . ##
	// #################################################

	@Test
	public void singleNode_Child() throws Exception {
		singleNode(childFactory);
	}

	@Test
	public void singleNode_Parent() throws Exception {
		singleNode(parentFactory);
	}

	private void singleNode(Function<List<TestNode>, PgeGraph<TestNode>> graphFactory) {
		PgeGraph<TestNode> graph = graphFactory.apply(asList(root));

		assertThat(graph).isNotNull();
		assertThat(graph.leafs).hasSize(1);
		assertThat(first(graph.leafs).item).isSameAs(root);
	}

	// #################################################
	// ## . . . . . . Direct Chain Node . . . . . . . ##
	// #################################################

	// a -> parent-of-A -> great-parent-of-A

	@Test
	public void directChain_Child() throws Exception {
		directChain(childFactory);
	}

	@Test
	public void directChain_Parent() throws Exception {
		directChain(parentFactory);
	}

	private void directChain(Function<List<TestNode>, PgeGraph<TestNode>> graphFactory) {
		directChainSetup();

		PgeGraph<TestNode> graph = graphFactory.apply(asList(root, innerL, leafLL));

		assertThat(graph).isNotNull();

		assertThat(graph.leafs).hasSize(1);

		PgeNode<TestNode> node = first(graph.leafs);
		assertThat(node.item).isSameAs(leafLL);

		node = first(node.parents);
		assertThat(node.item).isSameAs(innerL);

		node = first(node.parents);
		assertThat(node.item).isSameAs(root);
	}

	private void directChainSetup() {
		markChildParent(innerL, root);
		markChildParent(leafLL, innerL);
	}

	// #################################################
	// ## . . . . . . . Standard Tree . . . . . . . . ##
	// #################################################

	@Test
	public void standardTree_Child() throws Exception {
		standardTree(childFactory, NODES_ALL);
		standardTree(childFactory, asList(root));
	}

	@Test
	public void standardTree_Parent() throws Exception {
		standardTree(parentFactory, NODES_ALL);
		standardTree(parentFactory, asList(leafLL, leafLR, leafRL, leafRR));
	}

	private void standardTree(Function<List<TestNode>, PgeGraph<TestNode>> graphFactory, List<TestNode> inputNodes) {
		standardTreeSetup();

		PgeGraph<TestNode> graph = graphFactory.apply(inputNodes);

		assertThat(graph).isNotNull();

		Set<PgeNode<TestNode>> nodes = graph.leafs;
		assertThat(nodes).hasSize(4);
		assertThat(items(nodes)).containsExactlyInAnyOrder(leafLL, leafLR, leafRL, leafRR);

		nodes = parents(nodes);
		assertThat(nodes).hasSize(2);
		assertThat(items(nodes)).containsExactlyInAnyOrder(innerL, innerR);

		nodes = parents(nodes);
		assertThat(nodes).hasSize(1);
		assertThat(items(nodes)).containsExactly(root);
	}

	// #################################################
	// ## . . . . . . . Standard DAG . . . . . . . . .##
	// #################################################

	// Extension of standard Tree with more edges, still a DAG

	@Test
	public void standarDag_Child() throws Exception {
		standardDag(childFactory);
	}

	@Test
	public void standardDag_Parent() throws Exception {
		standardDag(parentFactory);
	}

	private void standardDag(Function<List<TestNode>, PgeGraph<TestNode>> graphFactory) {
		standardDagAdditionToStandardTree();

		standardTree(graphFactory, NODES_ALL);
	}

	// #################################################
	// ## . . . . . . . Cyclic Graph . . . . . . . . .##
	// #################################################

	@Test(expected = IllegalArgumentException.class)
	public void cyclicGraph_Child() throws Exception {
		cyclicGraph(childFactory);
	}

	@Test(expected = IllegalArgumentException.class)
	public void cyclicGraph_Parent() throws Exception {
		cyclicGraph(parentFactory);
	}

	private void cyclicGraph(Function<List<TestNode>, PgeGraph<TestNode>> graphFactory) {
		cyclicGraphSetup();

		graphFactory.apply(asList(root, innerL, leafLL));
	}

	private void cyclicGraphSetup() {
		markChildParent(leafLL, innerL);
		markChildParent(innerL, root);
		markChildParent(root, leafLL);
	}

	// #################################################
	// ## . . . . . . . . Helpers . . . . . . . . . . ##
	// #################################################

	private Set<TestNode> items(Collection<PgeNode<TestNode>> pgeNodes) {
		return pgeNodes.stream() //
				.map(n -> n.item) //
				.collect(Collectors.toSet());
	}

	private Set<PgeNode<TestNode>> parents(Set<PgeNode<TestNode>> nodes) {
		return nodes.stream() //
				.flatMap(n -> n.parents.stream()) //
				.collect(Collectors.toSet());
	}

}
