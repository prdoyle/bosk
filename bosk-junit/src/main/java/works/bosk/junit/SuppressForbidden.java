package works.bosk.junit;

/*
 * PD - copied from here:
 *
 *   https://raw.githubusercontent.com/policeman-tools/forbidden-apis/c16a1f2dc73a859cb2f4b0c2d68728b0e330d8c5/src/main/java/de/thetaphi/forbiddenapis/SuppressForbidden.java
 *
 * Original copyright notice:
 *
 * (C) Copyright Uwe Schindler (Generics Policeman) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Default annotation to suppress forbidden-apis errors inside a whole class, a method, or a field.
 * You can define your own annotation and pass a list of suppressing annotation types to the checker,
 * this allows to use the feature without compile-time dependencies to forbidden-apis.
 */
@Retention(RetentionPolicy.CLASS)
@Target({ ElementType.CONSTRUCTOR, ElementType.FIELD, ElementType.METHOD, ElementType.TYPE })
public @interface SuppressForbidden {
	String value();
}
