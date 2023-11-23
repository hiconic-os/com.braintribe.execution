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
package com.braintribe.execution.context;

import com.braintribe.common.attribute.AttributeContext;
import com.braintribe.utils.collection.impl.AttributeContexts;

public class AttributeContextTransferRunnable implements Runnable {
	private AttributeContext attributeContext = AttributeContexts.peek();
	private Runnable runnable;
	
	public AttributeContextTransferRunnable(Runnable runnable) {
		super();
		this.runnable = runnable;
	}

	@Override
	public void run() {
		if (attributeContext != AttributeContexts.empty()) {
			AttributeContexts.with(attributeContext).run(runnable);
		}
		else {
			runnable.run();
		}
	}
}