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

import static com.braintribe.utils.lcd.CollectionTools2.newLinkedSet;

import java.util.Map;
import java.util.Set;

/**
 * Node in the {@link PgeGraph}.
 * <p>
 * Every node processed before it's parents can be processed.
 * <p>
 * In higher levels the children are called dependencies and the parents dependers.
 * 
 * @see PgeGraph
 * 
 * @author peter.gazdik
 */
class PgeNode<N> {

	public final N item;

	// parent = depender --> processing is child-first
	public Set<PgeNode<N>> parents = newLinkedSet();

	public PgeNode(N item) {
		this.item = item;
	}

	public PgeNode<N> copyWith(Map<PgeNode<N>, PgeNode<N>> originToCopy) {
		PgeNode<N> result = originToCopy.get(this);

		if (result != null)
			return result;

		result = new PgeNode<>(item);
		originToCopy.put(this, result);

		for (PgeNode<N> parent : parents)
			result.parents.add(parent.copyWith(originToCopy));

		return result;
	}

}
