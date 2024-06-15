package io.vena.bosk.util;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Executable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import static java.lang.reflect.Modifier.isPrivate;
import static java.util.Objects.requireNonNull;
import static org.objectweb.asm.ClassReader.SKIP_CODE;
import static org.objectweb.asm.ClassReader.SKIP_DEBUG;
import static org.objectweb.asm.ClassReader.SKIP_FRAMES;
import static org.objectweb.asm.Opcodes.ASM9;
import static org.objectweb.asm.Type.ARRAY;
import static org.objectweb.asm.Type.OBJECT;

public final class ReflectionHelpers {

	public static Field setAccessible(Field field) {
		makeAccessible(field, field.getModifiers());
		return field;
	}

	public static <T extends Executable> T setAccessible(T method) {
		makeAccessible(method, method.getModifiers());
		return method;
	}

	private static void makeAccessible(AccessibleObject object, int modifiers) {
		// Let's honour "private" modifiers so people can know that private
		// methods and fields aren't being called by us. That allows them to
		// refactor them freely without concern for breaking some Bosk magic.
		//
		if (isPrivate(modifiers)) {
			throw new IllegalArgumentException("Access to private " + object.getClass().getSimpleName() + " is forbidden: " + object);
		}

		//... but otherwise, it's open season.
		object.setAccessible(true);
	}

	/**
	 * @param type must be defined in a classfile accessible by passing {@link Class#getResourceAsStream(String)}
	 *             the class's own name followed by <code>.class</code>.
	 *             In particular, this can't be a dynamically generated class.
	 * @return like {@link Class#getDeclaredMethods()} except in bytecode order.
	 */
	public static List<Method> getDeclaredMethodsInOrder(Class<?> type) {
		// Won't someone please tell me why getDeclaredMethods doesn't already work this way?
		List<Method> result = new ArrayList<>();
		ClassLoader loader = type.getClassLoader();
		ClassReader cr;
		try {
			String typeName = type.getName();
			String fileName = typeName.substring(typeName.lastIndexOf('.') + 1) + ".class";
			InputStream resource = type.getResourceAsStream(fileName);
			if (resource == null) {
				throw new IOException("No resource called \"" + fileName + "\"");
			}
			cr = new ClassReader(resource);
		} catch (IOException e) {
			throw new IllegalStateException("Unable to open the classfile corresponding to " + type, e);
		}
		cr.accept(new ClassVisitor(ASM9) {
			@Override
			public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
				if (name.equals("<init>") || name.equals("<clinit>")) {
					// getDeclaredMethods explicitly excludes these
					return null;
				} else if ((access & Opcodes.ACC_SYNTHETIC) != 0) {
					return null;
				}
				Type[] argumentTypes = Type.getArgumentTypes(descriptor);
				Class<?>[] argumentClasses = new Class<?>[argumentTypes.length];
				for (int i = 0; i < argumentTypes.length; i++) {
					argumentClasses[i] = findClass(argumentTypes[i], loader);
				}
				try {
					result.add(type.getDeclaredMethod(name, argumentClasses));
				} catch (NoSuchMethodException e) {
					throw new IllegalStateException(e);
				}
				return null;
			}
		}, SKIP_CODE | SKIP_DEBUG | SKIP_FRAMES);
		return result;
	}

	private static Class<?> findClass(Type argumentType, ClassLoader loader) {
		try {
			return switch (argumentType.getSort()) {
				case OBJECT ->
					requireNonNull(Class.forName(argumentType.getClassName(), false, loader));
				case ARRAY ->
					findClass(argumentType.getElementType(), loader).arrayType();
				default ->
					requireNonNull(classForPrimitiveName(argumentType.getClassName()));
			};
		} catch (ClassNotFoundException e) {
			throw new IllegalStateException(e);
		}
	}

	/**
	 * Stolen shamelessly from Java 22
	 */
	private static Class<?> classForPrimitiveName(String primitiveName) {
		return switch(primitiveName) {
			// Integral types
			case "int"     -> int.class;
			case "long"    -> long.class;
			case "short"   -> short.class;
			case "char"    -> char.class;
			case "byte"    -> byte.class;

			// Floating-point types
			case "float"   -> float.class;
			case "double"  -> double.class;

			// Other types
			case "boolean" -> boolean.class;
			case "void"    -> void.class;

			default        -> null;
		};
	}
}
