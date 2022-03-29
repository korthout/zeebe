/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.camunda.zeebe.protocol;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClass.Predicates;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition;
import com.tngtech.archunit.lang.syntax.elements.GivenClassesConjunction;
import io.camunda.zeebe.protocol.record.ImmutableProtocol;
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.value.ProcessInstanceRelated;
import org.immutables.value.Value;

/**
 * This test ensures that we always generate immutable variants of every protocol type, namely:
 *
 * <ul>
 *   <li>{@link Record}
 *   <li>all types extending {@link io.camunda.zeebe.protocol.record.RecordValue}, except certain
 *       meta types, such as {@link ProcessInstanceRelated} or {@link
 *       io.camunda.zeebe.protocol.record.RecordValueWithVariables}
 *   <li>all other types referenced in the above values, such as {@link
 *       io.camunda.zeebe.protocol.record.value.deployment.DeploymentResource}
 * </ul>
 */
@AnalyzeClasses(packages = "io.camunda.zeebe.protocol.record..")
final class ImmutableProtocolTest {
  @ArchTest
  void shouldAnnotateImmutableProtocol(final JavaClasses importedClasses) {
    // given
    final DescribedPredicate<JavaClass> excludedClasses = getExcludedClasses();
    final ArchRule rule =
        getBaseRule(excludedClasses).should().beAnnotatedWith(Value.Immutable.class);

    // then
    rule.check(importedClasses);
  }

  @ArchTest
  void shouldAnnotateImmutableProtocolValues(final JavaClasses importedClasses) {
    // given
    final DescribedPredicate<JavaClass> excludedClasses = getExcludedClasses();
    final ArchRule rule = getBaseRule(excludedClasses).should(beAnnotatedWithImmutableProtocol());

    // then
    rule.check(importedClasses);
  }

  private GivenClassesConjunction getBaseRule(final DescribedPredicate<JavaClass> excludedClasses) {
    return ArchRuleDefinition.classes()
        .that()
        // lookup only interface types, ignore enums or concrete classes
        .areInterfaces()
        .and()
        // check only all the types under the record.value and subpackages
        .resideInAnyPackage("io.camunda.zeebe.protocol.record.value..")
        // also check the Record interface itself
        .or(Predicates.equivalentTo(Record.class))
        // exclude certain interfaces
        .and(DescribedPredicate.not(excludedClasses));
  }

  // exclude certain interfaces for which we won't be generating any immutable variants
  private DescribedPredicate<JavaClass> getExcludedClasses() {
    return Predicates.equivalentTo(ProcessInstanceRelated.class);
  }

  private ArchCondition<JavaClass> beAnnotatedWithImmutableProtocol() {
    return new ArchCondition<JavaClass>("ensure ImmutableProtocol types are properly configured") {
      @Override
      public void check(final JavaClass item, final ConditionEvents events) {
        final ImmutableProtocol annotation = item.getAnnotationOfType(ImmutableProtocol.class);

        events.add(
            new SimpleConditionEvent(
                item,
                item.isAnnotatedWith(ImmutableProtocol.class),
                String.format("%s is not annotated with @ImmutableProtocol", item)));
        events.add(
            new SimpleConditionEvent(
                item,
                item.isAssignableFrom(annotation.immutable()),
                String.format(
                    "%s @ImmutableProtocol.immutable()=[%s] is not an implementation of %s",
                    item, annotation.immutable(), item)));
        events.add(
            new SimpleConditionEvent(
                item,
                annotation.immutable().isAnnotationPresent(ImmutableProtocol.Type.class),
                String.format(
                    "%s @ImmutableProtocol.immutable()=[%s] is not an immutable protocol type",
                    item, annotation.immutable())));
        events.add(
            new SimpleConditionEvent(
                item,
                annotation.builder().getDeclaringClass().equals(annotation.immutable()),
                String.format(
                    "%s @ImmutableProtocol.builder()=[%s] is not the inner builder of @ImmutableProtocol.immutable()=[%s]",
                    item, annotation.builder(), annotation.immutable())));
      }
    };
  }
}
