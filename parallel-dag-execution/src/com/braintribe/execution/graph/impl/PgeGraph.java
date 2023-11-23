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

import static com.braintribe.utils.lcd.CollectionTools2.newLinkedSet;
import static com.braintribe.utils.lcd.CollectionTools2.newMap;
import static com.braintribe.utils.lcd.CollectionTools2.newSet;
import static com.braintribe.utils.lcd.CollectionTools2.nullSafe;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.braintribe.execution.graph.api.ParallelGraphExecution;

/**
 * Represents a directed acyclic graph for the purpose of {@link ParallelGraphExecution}.
 * <p>
 * The graph consists of {@link PgeNode}s, and each node has {@link PgeNode#parents} which represents the nodes that can be processed (by the
 * executor) only after given node is processed.
 * <p>
 * {@link PgeGraph#leafs} are nodes that are not parents of other nodes.
 * 
 * @author peter.gazdik
 */
class PgeGraph<N> {

	public Set<PgeNode<N>> leafs;

	/* package */ PgeGraph(Set<PgeNode<N>> leafs) {
		this.leafs = leafs;
	}

	public PgeGraph<N> copy() {
		Map<PgeNode<N>, PgeNode<N>> originToCopy = newMap();

		Set<PgeNode<N>> leafsCopy = leafs.stream() //
				.map(origin -> origin.copyWith(originToCopy)) //
				.collect(Collectors.toSet());

		return new PgeGraph<>(leafsCopy);
	}
	public static <N> PgeGraph<N> forChildResolver(Iterable<? extends N> items, Function<N, Iterable<? extends N>> childResolver) {
		return new ChildResolvingGraphBuilder<N>(items, childResolver).build();
	}

	public static <N> PgeGraph<N> forParentResolver(Iterable<? extends N> items, Function<N, Iterable<? extends N>> parentResolver) {
		return new ParentResolvingGraphBuilder<N>(items, parentResolver).build();
	}

	static class AbstractGraphBuilder<N> {

		final Iterable<? extends N> items;
		final Map<N, PgeNode<N>> itemToNode = newMap();
		final Set<PgeNode<N>> leafs = newLinkedSet();

		public AbstractGraphBuilder(Iterable<? extends N> items) {
			this.items = items;
		}

		public PgeGraph<N> buildGraph() {
			verifyNoCycles();

			return new PgeGraph<>(leafs);
		}

		private void verifyNoCycles() {
			Set<PgeNode<N>> visited = newLinkedSet();

			visitAll(itemToNode.values(), visited);
		}

		private void visitAll(Collection<PgeNode<N>> nodes, Set<PgeNode<N>> visited) {
			for (PgeNode<N> node : nodes) {
				if (!visited.add(node))
					throwCycleDetected(node, visited);

				visitAll(node.parents, visited);

				visited.remove(node);
			}
		}

		private void throwCycleDetected(PgeNode<N> cycleNode, Set<PgeNode<N>> visited) {
			Iterator<PgeNode<N>> it = visited.iterator();
			while (it.next() != cycleNode) {
				/* do nothing */
			}

			StringJoiner sj = new StringJoiner(" -> ");

			sj.add(cycleNode.item.toString());
			while (it.hasNext())
				sj.add(it.next().item.toString());
			sj.add(cycleNode.item.toString());

			throw new IllegalArgumentException("Cannot create execution graph. Cycle detected: " + sj.toString());
		}
	}

	static class ChildResolvingGraphBuilder<N> extends AbstractGraphBuilder<N> {

		private final Function<N, Iterable<? extends N>> childResolver;

		ChildResolvingGraphBuilder(Iterable<? extends N> items, Function<N, Iterable<? extends N>> childResolver) {
			super(items);
			this.childResolver = childResolver;
		}

		public PgeGraph<N> build() {
			Set<N> visited = newSet();

			for (N item : items)
				processNode(item, visited);

			return buildGraph();
		}

		private void processNode(N item, Set<N> visited) {
			if (!visited.add(item))
				return;

			PgeNode<N> node = acquireNode(item);
			Iterable<? extends N> children = childResolver.apply(item);

			boolean hasChildren = false;
			for (N child : nullSafe(children)) {
				acquireNode(child).parents.add(node);
				hasChildren = true;
				processNode(child, visited);
			}

			if (!hasChildren)
				leafs.add(node);
		}

		private PgeNode<N> acquireNode(N item) {
			return itemToNode.computeIfAbsent(item, PgeNode::new);
		}

	}

	static class ParentResolvingGraphBuilder<N> extends AbstractGraphBuilder<N> {

		private final Function<N, Iterable<? extends N>> parentResolver;

		ParentResolvingGraphBuilder(Iterable<? extends N> items, Function<N, Iterable<? extends N>> parentResolver) {
			super(items);
			this.parentResolver = parentResolver;
		}

		public PgeGraph<N> build() {
			Set<N> visited = newSet();

			for (N item : items)
				processNode(item, visited);

			return buildGraph();
		}

		private void processNode(N item, Set<N> visited) {
			if (!visited.add(item))
				return;

			PgeNode<N> node = acquireNode(item);
			Iterable<? extends N> parents = parentResolver.apply(item);

			for (N parent : nullSafe(parents)) {
				PgeNode<N> parentNode = acquireNode(parent);
				node.parents.add(parentNode);
				leafs.remove(parentNode);
				processNode(parent, visited);
			}
		}

		private PgeNode<N> acquireNode(N item) {
			return itemToNode.computeIfAbsent(item, i -> {
				PgeNode<N> node = new PgeNode<>(i);
				leafs.add(node);
				return node;
			});
		}

	}

}
