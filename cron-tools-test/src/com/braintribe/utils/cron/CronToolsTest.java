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
package com.braintribe.utils.cron;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;
import org.quartz.CronExpression;

public class CronToolsTest {

	@Test
	public void testCreateCronExpressionFromTimeSpanQuartz() throws Exception {

		//@formatter:off
		String[] pairs = new String[] {
				"30s", "*/30 * * * * ? *",
				"1min", "0 * * * * ? *",
				"2min", "0 */2 * * * ? *",
				"1d", "0 0 0 * * ? *",
				"1m", "0 0 0 1 * ? *",
				"2m", "0 0 0 1 */2 ? *",
				"1y", "0 0 0 1 1 ? *",
				"2y", "0 0 0 1 1 ? */2"
		};
		//@formatter:on

		for (int i = 0; i < pairs.length; i = i + 2) {
			String spec = pairs[0];
			String expected = pairs[1];

			String actual = CronTools.createCronExpressionFromTimeSpan(spec);

			assertThat(actual).isEqualTo(expected);
			assertThat(CronExpression.isValidExpression(actual)).isTrue();
		}

	}

	@Test
	public void testCreateCronExpressionFromTimeSpanCron4j() throws Exception {

		//@formatter:off
		String[] pairs = new String[] {
				"30s", "* * * * *",
				"1min", "* * * * *",
				"2min", "*/2 * * * *",
				"1d", "0 0 * * *",
				"1m", "0 0 1 * *",
				"2m", "0 0 1 */2 *",
				"1y", "0 0 1 * *",
				"2y", "* * * * *"
		};
		//@formatter:on

		for (int i = 0; i < pairs.length; i = i + 2) {
			String spec = pairs[0];
			String expected = pairs[1];

			String actual = CronTools.createCronExpressionFromTimeSpan(spec, CronType.CRON4J);

			assertThat(actual).isEqualTo(expected);
		}

	}
}
