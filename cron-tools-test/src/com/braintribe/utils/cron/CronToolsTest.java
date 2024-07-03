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
