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
package com.braintribe.execution.priority;

public class Prioritized implements HasPriority {

	protected double priority = 0d;
	private int insertionIndex;

	public Prioritized(double priority, int insertionIndex) {
		this.priority = priority;
		this.insertionIndex = insertionIndex;
	}
	
	public double getPriority() {
		return priority;
	}
	public int getInsertionIndex() {
		return insertionIndex;
	}
	
	public int compareTo(Prioritized o) {
		if (o.priority > this.priority) {
			return 1;
		} else if (o.priority < this.priority) {
			return -1;
		}
		if (o.insertionIndex < this.insertionIndex) {
			return 1;
		} else {
			return -1;
		}
	}
}
