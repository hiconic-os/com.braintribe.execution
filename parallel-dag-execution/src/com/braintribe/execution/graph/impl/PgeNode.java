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

import java.util.Map;
import java.util.Set;

/**
 * Node in the {@link PgeGraph}.
 * <p>
 * Every node processed before it's parents can be processed. In higher levels the children are called dependencies and the parents dependers.
 * 
 * @see PgeGraph
 * 
 * @author peter.gazdik
 */
class PgeNode<N> {

	public final N item;

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
