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
package com.braintribe.execution.queue;

import java.util.concurrent.atomic.AtomicInteger;

public class FifoEntry<E extends Comparable<? super E>> implements Comparable<FifoEntry<E>> {
	static final AtomicInteger seq = new AtomicInteger(0);
	final int seqNum;
	final E entry;
	
	public FifoEntry(E entry) {
		seqNum = seq.getAndIncrement();
		this.entry = entry;
	}
	
	public E getEntry() { 
		return entry; 
	}
	
	@Override
	public int compareTo(FifoEntry<E> other) {
		int res = entry.compareTo(other.entry);
		if (res != 0) {
			return res;
		}
		if (other.seqNum > 0 && this.seqNum < 0) {
			return 1;
		}
		if (other.seqNum < 0 && this.seqNum > 0) {
			return -1;				
		}
		if (other.seqNum < this.seqNum) {
			return 1;
		} else if (other.seqNum > this.seqNum) {
			return -1;
		}
		return 0;
	}
	
	@Override
	public String toString() {
		return ""+seqNum+": "+entry;
	}
}
